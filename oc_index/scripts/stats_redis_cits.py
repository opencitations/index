#!/usr/bin/env python3
"""
redis_citation_stats.py

Reads a Redis DB where:
  - each key is a "cited" entity id, e.g. "br/062601246144"
  - each key is a SET whose members are "citing" entities, formatted as
    "<collection>:<citing_id>", e.g. "coci:br/062401151688"

Produces:
  - global stats (total keys, total citation edges, per-collection counts)
  - unique citing entities per collection
  - unique citing entities overall, ignoring the collection prefix
  - per-key citation-count distribution (min/max/avg/median, top-N busiest keys)
  - optional CSV/JSON dump of the full per-key breakdown

Usage:
    python redis_citation_stats.py --host 127.0.0.1 --port 6379 --db 8 \
        --pattern "br/*" --out-json stats.json --out-csv per_key.csv
"""

import argparse
import csv
import json
import statistics
from collections import Counter, defaultdict

import redis


def parse_args():
    p = argparse.ArgumentParser(description="Compute citation stats from a Redis SET-based DB.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=8)
    p.add_argument("--pattern", default="br/*", help="SCAN match pattern for cited-entity keys")
    p.add_argument("--scan-count", type=int, default=1000, help="COUNT hint for SCAN")
    p.add_argument("--batch-size", type=int, default=500, help="Keys per pipeline batch for SMEMBERS")
    p.add_argument("--top-n", type=int, default=10, help="Show top-N busiest cited keys")
    p.add_argument("--out-json", default=None, help="Optional path to dump full stats as JSON")
    p.add_argument("--out-csv", default=None, help="Optional path to dump per-key breakdown as CSV")
    return p.parse_args()


def scan_keys(r, pattern, count):
    """Yield all keys matching pattern using SCAN (non-blocking, safe for large DBs)."""
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=pattern, count=count)
        for k in keys:
            yield k
        if cursor == 0:
            break


def batched(iterable, n):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= n:
            yield batch
            batch = []
    if batch:
        yield batch


def main():
    args = parse_args()
    r = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=True)

    # ---- accumulators ----
    per_key = {}                              # cited_key -> {collection: [citing_ids]}
    collection_edge_counts = Counter()        # collection -> number of citation edges
    collection_unique_citing = defaultdict(set)  # collection -> set of unique citing_ids
    unique_citing_overall = set()             # citing_id (collection-agnostic)
    unique_cited_overall = set()              # cited keys themselves
    citations_per_key = {}                    # cited_key -> total citation count (all collections)

    total_keys = 0
    total_edges = 0

    for key_batch in batched(scan_keys(r, args.pattern, args.scan_count), args.batch_size):
        pipe = r.pipeline(transaction=False)
        for key in key_batch:
            pipe.smembers(key)
        results = pipe.execute()

        for key, members in zip(key_batch, results):
            total_keys += 1
            unique_cited_overall.add(key)
            per_key[key] = defaultdict(list)

            for member in members:
                if ":" not in member:
                    # malformed member, skip or log
                    continue
                collection, citing_id = member.split(":", 1)

                per_key[key][collection].append(citing_id)
                collection_edge_counts[collection] += 1
                collection_unique_citing[collection].add(citing_id)
                unique_citing_overall.add(citing_id)   # collection-agnostic uniqueness
                total_edges += 1

            citations_per_key[key] = sum(len(v) for v in per_key[key].values())

    # ---- summary stats ----
    counts_list = list(citations_per_key.values())
    print("=" * 60)
    print("GLOBAL STATS")
    print("=" * 60)
    print(f"Total cited keys scanned:        {total_keys}")
    print(f"Total citation edges:            {total_edges}")
    print(f"Unique citing entities (overall, collection-agnostic): {len(unique_citing_overall)}")
    print()

    print("Per-collection edge counts:")
    for coll, cnt in collection_edge_counts.most_common():
        print(f"  {coll:15s} edges={cnt:8d}   unique_citing={len(collection_unique_citing[coll])}")
    print()

    if counts_list:
        print("Citations-per-key distribution:")
        print(f"  min:    {min(counts_list)}")
        print(f"  max:    {max(counts_list)}")
        print(f"  mean:   {statistics.mean(counts_list):.2f}")
        print(f"  median: {statistics.median(counts_list)}")
        print()

    top_keys = sorted(citations_per_key.items(), key=lambda x: x[1], reverse=True)[: args.top_n]
    print(f"Top {args.top_n} busiest cited keys:")
    for key, cnt in top_keys:
        colls = ", ".join(f"{c}:{len(v)}" for c, v in per_key[key].items())
        print(f"  {key:25s} total={cnt:5d}   ({colls})")
    print()

    # ---- optional exports ----
    if args.out_json:
        out = {
            "total_keys": total_keys,
            "total_edges": total_edges,
            "unique_citing_overall": len(unique_citing_overall),
            "collection_edge_counts": dict(collection_edge_counts),
            "collection_unique_citing_counts": {c: len(s) for c, s in collection_unique_citing.items()},
            "citations_per_key": citations_per_key,
        }
        with open(args.out_json, "w") as f:
            json.dump(out, f, indent=2)
        print(f"Wrote JSON summary to {args.out_json}")

    if args.out_csv:
        with open(args.out_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["cited_key", "collection", "citing_id"])
            for key, coll_dict in per_key.items():
                for coll, citing_ids in coll_dict.items():
                    for cid in citing_ids:
                        writer.writerow([key, coll, cid])
        print(f"Wrote per-key breakdown CSV to {args.out_csv}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

"""
Run overlap queries for OpenCitations collections.

Produces:
    - collection_combinations.csv
      Counts for every combination of sizes 2,3,4,5,7.

    - only_collections.csv
      Counts of citations occurring exclusively in one collection.

Examples

Run everything:
    python combinations.py

Only overlap combinations:
    python combinations.py --mode combinations

Only exclusive counts:
    python combinations.py --mode exclusive

Custom endpoint:
    python combinations.py \
        --endpoint http://localhost:7021 \
        --mode both
"""

import argparse
import csv
import itertools
import sys

import requests


COLLECTIONS = [
    "coci",
    "doci",
    "poci",
    "oroci",
    "joci",
    "outoci",
    "moci",
]

BASE = "https://w3id.org/oc/index/"


def build_query(combination):
    """
    Query counting citations present in ALL collections of a combination.
    """

    triples = "\n".join(
        f"?citation <http://www.w3.org/ns/prov#atLocation> <{BASE}{c}/> ."
        for c in combination
    )

    return f"""
SELECT (COUNT(?citation) AS ?count)
WHERE {{
{triples}
}}
"""


def build_only_query(collection):
    """
    Query counting citations present ONLY in one collection.
    Uses MINUS instead of FILTER NOT EXISTS.
    """

    others = "\n".join(
        f"<{BASE}{c}/>"
        for c in COLLECTIONS
        if c != collection
    )

    return f"""
SELECT (COUNT(?citation) AS ?count)
WHERE {{
    ?citation <http://www.w3.org/ns/prov#atLocation> <{BASE}{collection}/> .

    MINUS {{
        ?citation <http://www.w3.org/ns/prov#atLocation> ?other .
        VALUES ?other {{
            {others}
        }}
    }}
}}
"""


def run_query(endpoint, query):

    headers = {
        "Accept": "application/sparql-results+json"
    }

    response = requests.post(
        endpoint,
        data={"query": query},
        headers=headers,
        timeout=600,
    )

    response.raise_for_status()

    data = response.json()

    return int(data["results"]["bindings"][0]["count"]["value"])


def main():

    parser = argparse.ArgumentParser(
        description="Run overlap/exclusive queries over OpenCitations collections."
    )

    parser.add_argument(
        "--endpoint",
        default="http://localhost:7021",
        help="SPARQL endpoint",
    )

    parser.add_argument(
        "--sizes",
        nargs="+",
        type=int,
        default=[2, 3, 4, 5, 7],
        help="Combination sizes to compute",
    )

    parser.add_argument(
        "--mode",
        choices=["combinations", "exclusive", "both"],
        default="both",
        help="What to compute.",
    )

    parser.add_argument(
        "--output",
        default="collection_combinations.csv",
        help="Output CSV for combinations",
    )

    parser.add_argument(
        "--only-output",
        default="only_collections.csv",
        help="Output CSV for exclusive counts",
    )

    args = parser.parse_args()

    ###########################################################
    # COMBINATIONS
    ###########################################################

    if args.mode in ("combinations", "both"):

        combination_rows = []

        total = sum(
            len(list(itertools.combinations(COLLECTIONS, s)))
            for s in args.sizes
            if 2 <= s <= len(COLLECTIONS)
        )

        current = 1

        print(f"Running {total} combination queries...\n")

        for size in args.sizes:

            if size < 2 or size > len(COLLECTIONS):
                continue

            for comb in itertools.combinations(COLLECTIONS, size):

                name = "-".join(comb)

                print(f"[{current}/{total}] {name}")

                try:
                    count = run_query(
                        args.endpoint,
                        build_query(comb),
                    )

                except Exception as e:
                    print(e, file=sys.stderr)
                    count = None

                combination_rows.append(
                    {
                        "size": size,
                        "combination": name,
                        "count": count,
                    }
                )

                current += 1

        with open(args.output, "w", newline="") as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "size",
                    "combination",
                    "count",
                ],
            )

            writer.writeheader()
            writer.writerows(combination_rows)

        print(f"\nSaved {len(combination_rows)} rows to {args.output}")

    ###########################################################
    # EXCLUSIVE
    ###########################################################

    if args.mode in ("exclusive", "both"):

        exclusive_rows = []

        print("\nRunning exclusive queries...\n")

        for collection in COLLECTIONS:

            print(f"only-{collection}")

            try:

                count = run_query(
                    args.endpoint,
                    build_only_query(collection),
                )

            except Exception as e:

                print(e, file=sys.stderr)
                count = None

            exclusive_rows.append(
                {
                    "collection": collection,
                    "count": count,
                }
            )

        with open(args.only_output, "w", newline="") as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "collection",
                    "count",
                ],
            )

            writer.writeheader()
            writer.writerows(exclusive_rows)

        print(f"\nSaved {len(exclusive_rows)} rows to {args.only_output}")

    print("\nDone.")


if __name__ == "__main__":
    main()

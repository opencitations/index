#!/usr/bin/env python3

"""
Run overlap queries for OpenCitations collections.

Produces:
    - collection_combinations.csv
      Counts for every combination of sizes 2,3,4,5,7.

    - only_collections.csv
      Counts of citations occurring exclusively in one collection.

Example:

python combinations.py

python combinations.py \
    --endpoint http://localhost:7021 \
    --output combinations.csv \
    --only-output exclusive.csv

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
    """Build query for citations appearing in all collections of a combination."""

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
    """Build query for citations appearing ONLY in one collection."""

    query = f"""
SELECT (COUNT(?citation) AS ?count)
WHERE {{
?citation <http://www.w3.org/ns/prov#atLocation> <{BASE}{collection}/> .
"""

    for other in COLLECTIONS:
        if other == collection:
            continue

        query += f"""
FILTER NOT EXISTS {{
    ?citation <http://www.w3.org/ns/prov#atLocation> <{BASE}{other}/> .
}}
"""

    query += "\n}"

    return query


def run_query(endpoint, query):

    headers = {
        "Accept": "application/sparql-results+json",
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

    parser = argparse.ArgumentParser()

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
        "--output",
        default="collection_combinations.csv",
        help="CSV for combination counts",
    )

    parser.add_argument(
        "--only-output",
        default="only_collections.csv",
        help="CSV for exclusive collection counts",
    )

    args = parser.parse_args()

    ####################################################################
    # Combination queries
    ####################################################################

    combination_rows = []

    total = sum(
        len(list(itertools.combinations(COLLECTIONS, s)))
        for s in args.sizes
        if 2 <= s <= len(COLLECTIONS)
    )

    i = 1

    for size in args.sizes:

        if size < 2 or size > len(COLLECTIONS):
            continue

        for comb in itertools.combinations(COLLECTIONS, size):

            name = "-".join(comb)

            print(f"[{i}/{total}] {name}")

            query = build_query(comb)

            try:
                count = run_query(args.endpoint, query)
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

            i += 1

    with open(args.output, "w", newline="") as f:

        writer = csv.DictWriter(
            f,
            fieldnames=["size", "combination", "count"],
        )

        writer.writeheader()
        writer.writerows(combination_rows)

    ####################################################################
    # Exclusive queries
    ####################################################################

    exclusive_rows = []

    print("\nRunning exclusive collection queries...\n")

    for collection in COLLECTIONS:

        print(f"only-{collection}")

        query = build_only_query(collection)

        try:
            count = run_query(args.endpoint, query)
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
            fieldnames=["collection", "count"],
        )

        writer.writeheader()
        writer.writerows(exclusive_rows)

    print("\nFinished.")
    print(f"Combination counts : {args.output}")
    print(f"Exclusive counts   : {args.only_output}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import argparse
import csv
import itertools
import requests
import sys

COLLECTIONS = [
    "coci",
    "doci",
    "poci",
    "oroci",
    "joci",
    "outoci",
    "moci",
]

PREFIX = "https://w3id.org/oc/index/"


def build_query(combination):
    patterns = "\n".join(
        f"?citation <http://www.w3.org/ns/prov#atLocation> <{PREFIX}{c}/> ."
        for c in combination
    )

    return f"""PREFIX cito:<http://purl.org/spar/cito/>

SELECT (COUNT(?citation) AS ?count)
WHERE {{
{patterns}
}}
"""


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
    parser = argparse.ArgumentParser(
        description="Count citation overlaps between OpenCitations collections."
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
        "--output",
        default="collection_combinations.csv",
        help="Output CSV file",
    )

    args = parser.parse_args()

    rows = []

    for size in args.sizes:

        if size < 2 or size > len(COLLECTIONS):
            print(f"Skipping invalid size {size}")
            continue

        for comb in itertools.combinations(COLLECTIONS, size):

            name = "-".join(comb)

            print(f"Running {name}...")

            query = build_query(comb)

            try:
                count = run_query(args.endpoint, query)

            except Exception as e:
                print(f"ERROR: {e}", file=sys.stderr)
                count = None

            rows.append({
                "size": size,
                "combination": name,
                "count": count,
            })

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["size", "combination", "count"],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nSaved {len(rows)} results to {args.output}")


if __name__ == "__main__":
    main()

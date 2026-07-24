#!/usr/bin/env python3

"""
Run overlap queries for OpenCitations collections.

Produces:
    - collection_combinations.csv
      Counts for:
        size 1 -> individual collections
        size 2 -> pairs
        size 3 -> triples
        size 4 -> quadruples
        size 5 -> five-way combinations
        size 7 -> all collections together

    - only_collections.csv
      Counts of citations occurring exclusively in one collection.

Examples:

Run everything:
    python combinations.py

Only combinations:
    python combinations.py --mode combinations

Only exclusive:
    python combinations.py --mode exclusive

Custom endpoint:
    python combinations.py --endpoint http://localhost:7021
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
    Count citations present in all collections of the combination.
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
    Count citations appearing ONLY in one collection.
    Uses MINUS.
    """

    others = "\n".join(
        f"<{BASE}{c}/>"
        for c in COLLECTIONS
        if c != collection
    )

    return f"""
SELECT (COUNT(?citation) AS ?count)
WHERE {{
    ?citation <http://www.w3.org/ns/prov#atLocation>
        <{BASE}{collection}/> .

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

    return int(
        data["results"]["bindings"][0]["count"]["value"]
    )


def main():

    parser = argparse.ArgumentParser(
        description="Run OpenCitations collection overlap analysis."
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
        default=[1, 2, 3, 4, 5, 7],
        help="Combination sizes",
    )

    parser.add_argument(
        "--mode",
        choices=[
            "combinations",
            "exclusive",
            "both",
        ],
        default="both",
        help="Run combinations, exclusive, or both",
    )

    parser.add_argument(
        "--output",
        default="collection_combinations.csv",
        help="Combination output CSV",
    )

    parser.add_argument(
        "--only-output",
        default="only_collections.csv",
        help="Exclusive output CSV",
    )

    args = parser.parse_args()


    ############################################################
    # ALL COMBINATIONS INCLUDING SINGLE COLLECTIONS
    ############################################################

    if args.mode in ("combinations", "both"):

        rows = []

        total = sum(
            len(list(itertools.combinations(COLLECTIONS, size)))
            for size in args.sizes
            if 1 <= size <= len(COLLECTIONS)
        )

        counter = 1

        print(
            f"Running {total} combination queries...\n"
        )

        for size in args.sizes:

            if size < 1 or size > len(COLLECTIONS):
                continue

            for combination in itertools.combinations(
                COLLECTIONS,
                size
            ):

                name = "-".join(combination)

                print(
                    f"[{counter}/{total}] {name}"
                )

                query = build_query(combination)

                try:

                    count = run_query(
                        args.endpoint,
                        query
                    )

                except Exception as e:

                    print(
                        e,
                        file=sys.stderr
                    )

                    count = None


                rows.append(
                    {
                        "size": size,
                        "combination": name,
                        "count": count,
                    }
                )

                counter += 1


        with open(
            args.output,
            "w",
            newline=""
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "size",
                    "combination",
                    "count",
                ],
            )

            writer.writeheader()
            writer.writerows(rows)


        print(
            f"\nSaved {len(rows)} rows to {args.output}"
        )


    ############################################################
    # EXCLUSIVE COLLECTIONS
    ############################################################

    if args.mode in ("exclusive", "both"):

        rows = []

        print(
            "\nRunning exclusive collection queries...\n"
        )


        for collection in COLLECTIONS:

            print(
                f"only-{collection}"
            )

            query = build_only_query(
                collection
            )

            try:

                count = run_query(
                    args.endpoint,
                    query
                )

            except Exception as e:

                print(
                    e,
                    file=sys.stderr
                )

                count = None


            rows.append(
                {
                    "collection": collection,
                    "count": count,
                }
            )


        with open(
            args.only_output,
            "w",
            newline=""
        ) as f:

            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "collection",
                    "count",
                ],
            )

            writer.writeheader()
            writer.writerows(rows)


        print(
            f"\nSaved {len(rows)} rows to {args.only_output}"
        )


    print("\nFinished.")


if __name__ == "__main__":
    main()

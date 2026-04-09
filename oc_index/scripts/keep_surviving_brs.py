#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import os
import re
from pathlib import Path


# -----------------------------
# Load CSV mappings
# -----------------------------
def load_mappings(csv_path):
    """
    Returns:
        dict: {merged_id_digits -> surviving_id_digits}
    """
    mapping = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            surviving = row["surviving_entity"].strip()
            merged_list = row["merged_entities"].split(";")

            surviving_id = extract_digits(surviving)

            for merged in merged_list:
                merged = merged.strip()
                if not merged:
                    continue

                merged_id = extract_digits(merged)
                mapping[merged_id] = surviving_id

    return mapping


# -----------------------------
# Extract numeric identifier
# -----------------------------
def extract_digits(uri):
    """
    Extract numeric id from:
    https://w3id.org/oc/meta/br/062401300477
    """
    return uri.rstrip("/").split("/")[-1]


# -----------------------------
# Replace IDs inside a line
# -----------------------------
META_PATTERN = re.compile(
    r"(https://w3id\.org/oc/meta/br/)(\d+)"
)

CI_PATTERN = re.compile(
    r"(https://w3id\.org/oc/index/ci/)(\d+)-(\d+)"
)


def replace_ids(line, mapping):
    """
    Replace identifiers in:
      - meta/br/<ID>
      - index/ci/<ID>-<ID>
    """

    # Replace meta/br IDs
    def meta_repl(match):
        prefix, digits = match.groups()
        new_digits = mapping.get(digits, digits)
        return prefix + new_digits

    line = META_PATTERN.sub(meta_repl, line)

    # Replace ci IDs (both parts independently)
    def ci_repl(match):
        prefix, id1, id2 = match.groups()

        id1_new = mapping.get(id1, id1)
        id2_new = mapping.get(id2, id2)

        return f"{prefix}{id1_new}-{id2_new}"

    line = CI_PATTERN.sub(ci_repl, line)

    return line


# -----------------------------
# Process one TTL file
# -----------------------------
def process_file(in_path, out_path, mapping):

    with open(in_path, "r", encoding="utf-8") as fin, \
         open(out_path, "w", encoding="utf-8") as fout:

        for line in fin:
            new_line = replace_ids(line, mapping)
            fout.write(new_line)


# -----------------------------
# Walk directories
# -----------------------------
def process_directory(input_dir, mapping, suffix):

    input_dir = Path(input_dir)

    ttl_files = list(input_dir.rglob("*.ttl"))

    print(f"Found {len(ttl_files)} TTL files")

    for ttl in ttl_files:
        out_name = suffix + ttl.name
        out_path = ttl.parent / out_name

        process_file(ttl, out_path, mapping)

        print(f"✔ {ttl} -> {out_path}")


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Rewrite OC TTL identifiers using merge CSV"
    )

    parser.add_argument(
        "--input-dir",
        required=True,
        help="Root directory containing TTL files"
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="CSV mapping file"
    )

    parser.add_argument(
        "--prefix",
        default="__edit__",
        help="Prefix for edited TTL files (default: __edit__)"
    )

    args = parser.parse_args()

    print("Loading mappings...")
    mapping = load_mappings(args.csv)
    print(f"Loaded {len(mapping)} merged identifiers")

    process_directory(args.input_dir, mapping, args.prefix)

    print("Done.")


if __name__ == "__main__":
    main()

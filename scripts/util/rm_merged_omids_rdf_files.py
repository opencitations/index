import argparse
import csv
import os
import re
from pathlib import Path


ID_REGEX = re.compile(r'/(\d+)$')


def extract_merged_ids(csv_path):
    merged_ids = set()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            merged_col = row.get("merged_entities")
            if not merged_col:
                continue

            entities = merged_col.split(";")
            for entity in entities:
                entity = entity.strip()
                match = ID_REGEX.search(entity)
                if match:
                    merged_ids.add(match.group(1))

    return merged_ids


def line_contains_merged_id(line, merged_ids):
    # Fast check: extract all numeric sequences from line
    for found_id in re.findall(r'\d+', line):
        if found_id in merged_ids:
            return True
    return False


def process_file(filepath, merged_ids, dry_run=False):
    removed_count = 0
    kept_lines = []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if line_contains_merged_id(line, merged_ids):
                removed_count += 1
            else:
                kept_lines.append(line)

    if removed_count > 0:
        print(f"[MODIFY] {filepath} → removing {removed_count} lines")
        if not dry_run:
            with open(filepath, "w", encoding="utf-8") as f:
                f.writelines(kept_lines)

    return removed_count


def main():
    parser = argparse.ArgumentParser(
        description="Remove TTL triples containing merged IDs."
    )

    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing TTL files (recursive)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be modified without rewriting files",
    )

    args = parser.parse_args()

    merged_ids = extract_merged_ids(args.csv)
    print(f"Collected {len(merged_ids)} merged IDs")

    ttl_files = list(Path(args.input_dir).rglob("*.ttl"))
    print(f"Found {len(ttl_files)} TTL files")

    total_removed = 0

    for ttl_file in ttl_files:
        total_removed += process_file(ttl_file, merged_ids, args.dry_run)

    print(f"Done. Total lines removed: {total_removed}")


if __name__ == "__main__":
    main()

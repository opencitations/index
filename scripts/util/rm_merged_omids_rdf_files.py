import argparse
import csv
import re
from pathlib import Path


# Strict pattern: meta/br/<digits>
BR_PATTERN = re.compile(r'meta/br/(\d+)')


def extract_mapping(csv_path):
    """
    Build mapping:
    merged_id -> surviving_id
    Only from meta/br/<ID> patterns.
    """
    mapping = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            surviving = row.get("surviving_entity")
            merged_col = row.get("merged_entities")

            if not surviving or not merged_col:
                continue

            # Extract surviving ID
            surviving_match = BR_PATTERN.search(surviving)
            if not surviving_match:
                continue

            surviving_id = surviving_match.group(1)

            # Extract merged IDs
            for match in BR_PATTERN.finditer(merged_col):
                merged_id = match.group(1)
                mapping[merged_id] = surviving_id

    return mapping


def replace_ids_in_line(line, mapping):
    """
    Replace only IDs inside meta/br/<ID>.
    """

    def replacer(match):
        found_id = match.group(1)
        if found_id in mapping:
            return f"meta/br/{mapping[found_id]}"
        return match.group(0)

    return BR_PATTERN.sub(replacer, line)


def process_file(filepath, mapping, dry_run=False):
    modified_count = 0
    changed = False
    new_lines = []

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            new_line = replace_ids_in_line(line, mapping)
            if new_line != line:
                modified_count += 1
                changed = True
            new_lines.append(new_line)

    if changed:
        print(f"[MODIFY] {filepath} → modified {modified_count} lines")
        if not dry_run:
            with open(filepath, "w", encoding="utf-8", buffering=16 * 1024 * 1024) as f:
                f.writelines(new_lines)

    return modified_count


def main():
    parser = argparse.ArgumentParser(
        description="Replace merged OMIDs in TTL files using CSV mapping (meta/br only)."
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

    mapping = extract_mapping(args.csv)
    print(f"Collected {len(mapping)} merged → surviving mappings")

    ttl_files = list(Path(args.input_dir).rglob("*.ttl"))
    print(f"Found {len(ttl_files)} TTL files")

    total_modified = 0

    for ttl_file in ttl_files:
        total_modified += process_file(ttl_file, mapping, args.dry_run)

    print(f"Done. Total lines modified: {total_modified}")


if __name__ == "__main__":
    main()

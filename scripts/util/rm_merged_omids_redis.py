import csv
import re
import argparse
import redis


def extract_merged_ids(csv_file):
    merged_ids = set()

    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            merged_column = row.get("merged_entities")
            if not merged_column:
                continue

            entities = merged_column.split(";")
            for entity in entities:
                entity = entity.strip()
                match = re.search(r'/(\d+)$', entity)
                if match:
                    merged_ids.add(match.group(1))

    return merged_ids


def main():
    parser = argparse.ArgumentParser(
        description="Remove merged OMIDs from Redis"
    )

    parser.add_argument("--host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--db", type=int, required=True, help="Redis database number")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not modify Redis, only print what would happen",
    )

    args = parser.parse_args()

    # Connect to Redis
    r = redis.Redis(
        host=args.host,
        port=args.port,
        db=args.db,
        decode_responses=True
    )

    # Extract IDs
    merged_ids = extract_merged_ids(args.csv)
    print(f"Collected {len(merged_ids)} merged IDs.")

    # 1️⃣ Delete keys if they exist
    for mid in merged_ids:
        if r.exists(mid):
            print(f"[KEY DELETE] {mid}")
            if not args.dry_run:
                r.delete(mid)

    # 2️⃣ Remove from sets
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor)
        for key in keys:
            if r.type(key) == "set":
                for mid in merged_ids:
                    if r.sismember(key, mid):
                        print(f"[SET REMOVE] {mid} from {key}")
                        if not args.dry_run:
                            r.srem(key, mid)

        if cursor == 0:
            break

    print("Done.")


if __name__ == "__main__":
    main()

#!python
# Copyright (c) 2023 Ivan Heibi.
# Copyright (c) 2026 Arcangelo Massari.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import argparse
import csv
import io
import json
import os
import re
import sys
import tarfile
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from zipfile import ZipFile

from redis import Redis
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn

from oc.index.utils.config import get_config

console = Console()

_config = get_config()
if _config is None:
    raise RuntimeError("Configuration not loaded")
csv.field_size_limit(sys.maxsize)


class RedisDB:

    def __init__(self, redishost, redisport, _db):
        self.rconn = Redis(host=redishost, port=redisport, db=_db, decode_responses=True)

    def flush_index(self, data):
        pipe = self.rconn.pipeline()
        for _k, _v in data.items():
            pipe.sadd(_k, *_v)
        pipe.execute()

    def flush_metadata(self, data):
        pipe = self.rconn.pipeline()
        for _k, _v in data.items():
            pipe.set(_k, _v)
        pipe.execute()

    def key_count(self):
        return self.rconn.dbsize()

    def export_to_csv(self, filepath):
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            for key in self.rconn.scan_iter():
                members: set[str] = self.rconn.smembers(key)  # type: ignore[assignment]
                writer.writerow([key, "; ".join(members)])

def get_key_ids(text):
    return text.split(" ")

def get_att_ids(text):
    bracket_contents = re.findall(r'\[(.*?)\]', text)
    return [part.split() for part in bracket_contents]

def get_id_val(l_ids, l_id_type=()):
    res = []
    for _id in l_ids:
        for _id_type in l_id_type:
            if _id.startswith(_id_type):
                res.append(_id)
    return res

def _p_csvfile(a_csv_file, rconn_db_br, rconn_db_ra, rconn_db_metadata):
    br_data = defaultdict(set)
    ra_data = defaultdict(set)
    metadata = {}

    for o_row in csv.DictReader(io.TextIOWrapper(a_csv_file)):
        br_ids = get_key_ids(o_row["id"])
        br_ids_omid = get_id_val(br_ids, ["omid"])
        br_ids_other = [x for x in br_ids if x not in br_ids_omid]
        for __oid in br_ids_other:
            br_data[__oid].update(br_ids_omid)

        l_ra_ids = get_att_ids(o_row["author"])
        for _ra in l_ra_ids:
            ra_ids_omid = get_id_val(_ra, ["omid"])
            ra_ids_other = [x for x in _ra if x not in ra_ids_omid]
            for __oid in ra_ids_other:
                ra_data[__oid].update(ra_ids_omid)

        orcids = []
        for _ra in l_ra_ids:
            orcids += get_id_val(_ra, ["orcid"])

        issns = []
        l_venue_ids = get_att_ids(o_row["venue"])
        for _venue in l_venue_ids:
            issns += get_id_val(_venue, ["issn"])

        for _omid in br_ids_omid:
            metadata[_omid] = json.dumps({
                "date": str(o_row["pub_date"]),
                "valid": True,
                "orcid": [a.replace("orcid:", "") for a in orcids],
                "issn": [a.replace("issn:", "") for a in issns]
            })

    rconn_db_br.flush_index(br_data)
    rconn_db_ra.flush_index(ra_data)
    rconn_db_metadata.flush_metadata(metadata)


def _process_file_worker(args: tuple[str, str, str, str, str, str, str, str]) -> str:
    file_type, path, name, redishost, redisport, db_br, db_ra, db_metadata = args

    rconn_db_br = RedisDB(redishost, redisport, db_br)
    rconn_db_ra = RedisDB(redishost, redisport, db_ra)
    rconn_db_metadata = RedisDB(redishost, redisport, db_metadata)

    if file_type == 'zip':
        with ZipFile(path) as archive:
            with archive.open(name) as csv_file:
                _p_csvfile(csv_file, rconn_db_br, rconn_db_ra, rconn_db_metadata)
    elif file_type == 'tar':
        with tarfile.open(path, 'r:gz') as archive:
            csv_file = archive.extractfile(name)
            if csv_file:
                _p_csvfile(csv_file, rconn_db_br, rconn_db_ra, rconn_db_metadata)
    elif file_type == 'file':
        with open(path, 'rb') as csv_file:
            _p_csvfile(csv_file, rconn_db_br, rconn_db_ra, rconn_db_metadata)

    rconn_db_br.rconn.close()
    rconn_db_ra.rconn.close()
    rconn_db_metadata.rconn.close()

    return name


def _get_csv_files(dump_path):
    """Return list of (csv_name, open_func) tuples for all CSV files to process."""
    csv_files = []

    if os.path.isfile(dump_path):
        if dump_path.endswith(".zip"):
            with ZipFile(dump_path) as archive:
                for name in archive.namelist():
                    if name.endswith('.csv'):
                        csv_files.append(('zip', dump_path, name))

        elif dump_path.endswith(".tar.gz") or dump_path.endswith(".tgz"):
            with tarfile.open(dump_path, 'r:gz') as archive:
                for name in archive.getnames():
                    if name.endswith('.csv'):
                        csv_files.append(('tar', dump_path, name))

        elif dump_path.endswith(".csv"):
            csv_files.append(('file', dump_path, os.path.basename(dump_path)))

    elif os.path.isdir(dump_path):
        for filename in os.listdir(dump_path):
            filepath = os.path.join(dump_path, filename)
            if os.path.isfile(filepath) and filename.endswith(".csv"):
                csv_files.append(('file', filepath, filename))

    return csv_files


def upload2redis(dump_path="", redishost="localhost", redisport="6379", db_br="10", db_ra="11", db_metadata="12", redis_only=False, workers=None):
    rconn_db_br = RedisDB(redishost, redisport, db_br)
    rconn_db_ra = RedisDB(redishost, redisport, db_ra)
    rconn_db_metadata = RedisDB(redishost, redisport, db_metadata)

    csv_files = _get_csv_files(dump_path)
    if not csv_files:
        console.print(f"[red]No CSV files found in: {dump_path}[/red]")
        return ("0", "0")

    num_workers = workers or cpu_count()
    console.print(f"Found {len(csv_files)} CSV files to process with {num_workers} workers")

    worker_args = [
        (file_type, path, name, redishost, redisport, db_br, db_ra, db_metadata)
        for file_type, path, name in csv_files
    ]

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Processing CSV files", total=len(csv_files))

        with Pool(processes=num_workers) as pool:
            for name in pool.imap_unordered(_process_file_worker, worker_args):
                progress.update(task, description=f"Completed {name}")
                progress.advance(task)

    if not redis_only:
        console.print("Saving indexes to CSV...")
        rconn_db_br.export_to_csv('meta_br.csv')
        rconn_db_ra.export_to_csv('meta_ra.csv')

    result = (str(rconn_db_br.key_count()), str(rconn_db_ra.key_count()))

    rconn_db_br.rconn.close()
    rconn_db_ra.rconn.close()
    rconn_db_metadata.rconn.close()

    return result


def main():
    if _config is None:
        raise RuntimeError("Configuration not loaded")

    parser = argparse.ArgumentParser(description='Store the metadata of OpenCitations Meta in Redis')
    parser.add_argument('--dump', type=str, required=True, help='The directory of CSVs or file (in ZIP or TAR.GZ) representing OpenCitations Meta dump')
    parser.add_argument('--redis-only', action='store_true', help='Only upload to Redis, do not save CSV files')
    parser.add_argument('--workers', type=int, default=None, help='Number of parallel workers (default: CPU count)')
    args = parser.parse_args()

    console.print("Start uploading data to Redis.")

    res = upload2redis(
        dump_path=args.dump,
        redishost=_config.get("redis", "host"),
        redisport=_config.get("redis", "port"),
        db_br=_config.get("cnc", "db_br"),
        db_ra=_config.get("cnc", "db_ra"),
        db_metadata=_config.get("INDEX", "db"),
        redis_only=args.redis_only,
        workers=args.workers
    )

    console.print(f"[green]Done![/green] Found {res[0]} unique BR OMIDs and {res[1]} unique RA OMIDs.")

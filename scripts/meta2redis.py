#!python
# Copyright (c) 2023 Ivan Heibi.
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

import csv
import json
from zipfile import ZipFile
import tarfile
import os
import io
import argparse
from redis import Redis
import re
import sys

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn
from collections import defaultdict
from oc.index.utils.config import get_config

console = Console()

_config = get_config()
if _config is None:
    raise RuntimeError("Configuration not loaded")
csv.field_size_limit(sys.maxsize)

br_index = defaultdict(set)
ra_index = defaultdict(set)

class RedisDB(object):

    def __init__(self, redishost, redisport, redisbatchsize, _db):
        self.redisbatchsize = int(redisbatchsize)
        self.rconn = Redis(host=redishost, port=redisport, db=_db)

    def set_data(self, data, force=False, type=None):
        if len(data) >= self.redisbatchsize or force:
            for item in data:
                _k = item[0]
                _v = item[1]
                _v_val = _v

                if type == "br":
                    if _k in br_index:
                        br_index[_k].update( set(_v) )
                    else:
                        br_index[_k] = set(_v)
                    _v_val = "; ".join(br_index[_k])

                elif type == "ra":
                    if _k in ra_index:
                        ra_index[_k].update( set(_v) )
                    else:
                        ra_index[_k] = set(_v)
                    _v_val = "; ".join(ra_index[_k])

                self.rconn.set(_k, _v_val)

            return len(data)
        return 0

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
    # Set buffers
    db_br_buffer = []
    db_ra_buffer = []
    db_metadata_buffer = []

    l_brs = list(csv.DictReader(io.TextIOWrapper(a_csv_file)))

    for o_row in l_brs:
        # List of BR ids
        # Update the <db_br_buffer> to be added in Redis (<rconn_db_br>)
        br_ids = get_key_ids(o_row["id"])
        br_ids_omid = get_id_val(br_ids, ["omid"])
        br_ids_other = [x for x in br_ids if x not in br_ids_omid]
        # Add it to the list of BRs
        for __oid in br_ids_other:
            db_br_buffer.append((__oid, br_ids_omid))

        # List of RA ids
        # Update the <db_ra_buffer> to be added in Redis (<rconn_db_ra>)
        l_ra_ids = get_att_ids(o_row["author"])  # [["omid:123","orcid:1111-2222"], ...]
        for _ra in l_ra_ids:  # ["omid:123","orcid:1111-2222"]
            ra_ids_omid = get_id_val(_ra, ["omid"])  # ["omid:123"]
            ra_ids_other = [x for x in _ra if x not in ra_ids_omid]  # ["orcid:1111-2222"]
            # Add it to the list of RAs
            for __oid in ra_ids_other:
                db_ra_buffer.append((__oid, ra_ids_omid))

        # Metadata of each BR
        # Update the <db_metadata_buffer> to be added in Redis (<rconn_db_metadata>)
        for _omid in br_ids_omid:
            # Take only ORCIDs
            orcids = []
            for _ra in l_ra_ids:
                orcids += get_id_val(_ra, ["orcid"])  # ["orcid:1111-2222"]

            # Take only ISSNs
            issns = []
            l_venue_ids = get_att_ids(o_row["venue"])
            for _venue in l_venue_ids:
                issns += get_id_val(_venue, ["issn"])  # ["issn:1111-2222"]

            db_metadata_buffer.append((
                _omid,
                json.dumps({
                    "date": str(o_row["pub_date"]),
                    "valid": True,
                    "orcid": [a.replace("orcid:", "") for a in orcids],
                    "issn": [a.replace("issn:", "") for a in issns]
                })
            ))

        # Flush buffers when batch size is reached
        if rconn_db_br.set_data(db_br_buffer, type="br") > 0:
            db_br_buffer.clear()
        if rconn_db_ra.set_data(db_ra_buffer, type="ra") > 0:
            db_ra_buffer.clear()
        if rconn_db_metadata.set_data(db_metadata_buffer) > 0:
            db_metadata_buffer.clear()

    # Flush remaining data in Redis
    rconn_db_metadata.set_data(db_metadata_buffer, force=True)
    rconn_db_br.set_data(db_br_buffer, force=True, type="br")
    rconn_db_ra.set_data(db_ra_buffer, force=True, type="ra")


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


def upload2redis(dump_path="", redishost="localhost", redisport="6379", redisbatchsize="10000", db_br="10", db_ra="11", db_metadata="12", redis_only=False):
    rconn_db_br = RedisDB(redishost, redisport, redisbatchsize, db_br)
    rconn_db_ra = RedisDB(redishost, redisport, redisbatchsize, db_ra)
    rconn_db_metadata = RedisDB(redishost, redisport, redisbatchsize, db_metadata)

    csv_files = _get_csv_files(dump_path)
    if not csv_files:
        console.print(f"[red]No CSV files found in: {dump_path}[/red]")
        return ("0", "0")

    console.print(f"Found {len(csv_files)} CSV files to process")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Processing CSV files", total=len(csv_files))

        for file_type, path, name in csv_files:
            progress.update(task, description=f"Processing {name}")

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

            progress.advance(task)

    if not redis_only:
        console.print("Saving global indexes to CSV...")
        with open('meta_br.csv', 'w', newline='') as f:
            write = csv.writer(f)
            for any_id in br_index:
                write.writerow([any_id, "; ".join(br_index[any_id])])

        with open('meta_ra.csv', 'w', newline='') as f:
            write = csv.writer(f)
            for any_id in ra_index:
                write.writerow([any_id, "; ".join(ra_index[any_id])])

    return (str(len(br_index)), str(len(ra_index)))


def main():
    if _config is None:
        raise RuntimeError("Configuration not loaded")

    parser = argparse.ArgumentParser(description='Store the metadata of OpenCitations Meta in Redis')
    parser.add_argument('--dump', type=str, required=True, help='The directory of CSVs or file (in ZIP or TAR.GZ) representing OpenCitations Meta dump')
    parser.add_argument('--redis-only', action='store_true', help='Only upload to Redis, do not save CSV files')
    args = parser.parse_args()

    console.print("Start uploading data to Redis.")

    res = upload2redis(
        dump_path=args.dump,
        redishost=_config.get("redis", "host"),
        redisport=_config.get("redis", "port"),
        redisbatchsize=_config.get("redis", "batch_size"),
        db_br=_config.get("cnc", "db_br"),
        db_ra=_config.get("cnc", "db_ra"),
        db_metadata=_config.get("INDEX", "db"),
        redis_only=args.redis_only
    )

    console.print(f"[green]Done![/green] Found {res[0]} unique BR OMIDs and {res[1]} unique RA OMIDs.")

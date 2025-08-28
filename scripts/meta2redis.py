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
import datetime
import io
import argparse
from redis import Redis
import re
import sys

from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
csv.field_size_limit(sys.maxsize)

# glob indexes
br_ids = _config.get("cnc", "br_ids").split(",")
ra_ids = _config.get("cnc", "ra_ids").split(",")

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

                if type == "br":
                    if _k in br_index:
                        br_index[_k].update( set(_v) )
                    _v = "; ".join(_v)

                elif type == "ra":
                    if _k in ra_index:
                        ra_index[_k].update( set(_v) )
                    _v = "; ".join(_v)

                self.rconn.set(_k, _v)

            return len(data)
        return 0

def get_key_ids(text):
    return text.split(" ")

def get_att_ids(text):
    bracket_contents = re.findall(r'\[(.*?)\]', text)
    return [part.split() for part in bracket_contents]

def get_id_val(l_ids,l_id_type = []):
    res = []
    for _id in l_ids:
        for _id_type in l_id_type:
            if _id.startswith(_id_type):
                res.append(_id)
    return res

def _p_csvfile(a_csv_file,csv_name,rconn_db_br, rconn_db_ra, rconn_db_metadata):

    global _config
    logger = get_logger()

    # set buffers
    db_br_buffer = []
    db_ra_buffer = []
    db_metadata_buffer = []

    l_brs = list(csv.DictReader(io.TextIOWrapper(a_csv_file)))

    # walk through each citation in the CSV
    logger.info("Walking through all the "+str( len(l_brs) )+" BRs (rows) in: "+str(csv_name) )
    for o_row in tqdm(l_brs):

        # list of BR ids
        # > update the <db_br_buffer> to be added in REDIS (<rconn_db_br>)
        br_ids = get_key_ids(o_row["id"])
        br_ids_omid = get_id_val(br_ids,"omid")
        br_ids_other = [x for x in br_ids if x not in br_ids_omid]
        # Add it to the list of BRs
        for __oid in br_ids_other:
            db_br_buffer.append(
                (
                    __oid,
                    br_ids_omid
                )
            )

        # list of RA ids
        # > update the <db_ra_buffer> to be added in REDIS (<rconn_db_ra>)
        ra_ids = get_att_ids(o_row["author"])
        ra_ids_omid = get_id_val(ra_ids,"omid")
        ra_ids_other = [x for x in ra_ids if x not in ra_ids_omid]
        # Add it to the list of RAs
        for __oid in ra_ids_other:
            db_ra_buffer.append(
                (
                    __oid,
                    ra_ids_omid
                )
            )

        # metadata of each br
        # > update the <db_metadata_buffer> to be added in REDIS (<rconn_db_metadata>)
        for _omid in br_ids_omid:
            orcids = get_id_val(ra_ids,"orcid")
            issns = get_id_val( get_att_ids(o_row["venue"]), "issn" )
            db_metadata_buffer.append(
                (
                    _omid,
                    json.dumps(
                            {
                                "date": str(o_row["pub_date"]),
                                "valid": True,
                                "orcid": [a.replace("orcid:","") for a in orcids] if len(orcids) > 0 else [],
                                "issn": [a.replace("issn:","") for a in issns] if len(issns) > 0 else []
                            }
                    )
                )
            )

    # Set last data in Redis
    logger.info("Updating Redis ... ")
    rconn_db_metadata.set_data(db_metadata_buffer, True)
    rconn_db_br.set_data(db_br_buffer, True, type= "br")
    rconn_db_ra.set_data(db_ra_buffer, True, type= "ra")


def upload2redis(dump_path="", redishost="localhost", redisport="6379", redisbatchsize="10000", db_omid = "9", db_br="10", db_ra="11", db_metadata="12"):
    global _config
    logger = get_logger()

    #rconn_db_omid =  RedisDB(redishost, redisport, redisbatchsize, db_omid)
    rconn_db_br =  RedisDB(redishost, redisport, redisbatchsize, db_br)
    rconn_db_ra = RedisDB(redishost, redisport, redisbatchsize, db_ra)
    rconn_db_metadata = RedisDB(redishost, redisport, redisbatchsize, db_metadata)

    # Check if dump_path is a single archive file
    if os.path.isfile(dump_path):
        if dump_path.endswith(".zip"):
            # Handle single ZIP file
            with ZipFile(dump_path) as archive:
                logger.info(f"ZIP: Total number of files in {os.path.basename(dump_path)}: {len(archive.namelist())}")
                for csv_name in archive.namelist():
                    if csv_name.endswith('.csv'):
                        with archive.open(csv_name) as csv_file:
                            _p_csvfile(csv_file, csv_name, rconn_db_br, rconn_db_ra, rconn_db_metadata)

        elif dump_path.endswith(".tar.gz") or dump_path.endswith(".tgz"):
            # Handle single TAR.GZ file
            with tarfile.open(dump_path, 'r:gz') as archive:
                logger.info(f"TAR.GZ: Total number of files in {os.path.basename(dump_path)}: {len(archive.getnames())}")
                for csv_name in archive.getnames():
                    if csv_name.endswith('.csv'):
                        csv_file = archive.extractfile(csv_name)
                        if csv_file:
                            _p_csvfile(csv_file, csv_name, rconn_db_br, rconn_db_ra, rconn_db_metadata)

        elif dump_path.endswith(".csv"):
            # Handle single CSV file
            csv_name = os.path.basename(dump_path)
            logger.info(f"CSV: Processing direct CSV file: {csv_name}")
            with open(dump_path, 'r', encoding='utf-8') as csv_file:
                _p_csvfile(csv_file,csv_name, rconn_db_br, rconn_db_ra, rconn_db_metadata)
        else:
            logger.warning(f"Unsupported file type: {dump_path}")

    # Check if dump_path is a directory
    elif os.path.isdir(dump_path):
        # Directory contains only CSV files
        for filename in os.listdir(dump_path):
            filepath = os.path.join(dump_path, filename)
            # Skip if it's not a file
            if not os.path.isfile(filepath):
                continue

            if filename.endswith(".csv"):
                logger.info(f"CSV: Processing direct CSV file: {filename}")
                with open(filepath, 'r', encoding='utf-8') as csv_file:
                    _p_csvfile(csv_file, filename, rconn_db_br, rconn_db_ra, rconn_db_metadata)
    else:
        logger.error(f"Path does not exist or is neither a file nor directory: {dump_path}")

    #print glob indexes to file
    logger.info("Saving (in CSV) global indexes...")
    with open('meta_br.csv', 'a+') as f:
        write = csv.writer(f)
        for any_id in br_index:
            write.writerow([any_id,"; ".join(list(br_index[any_id]))])

    with open('meta_ra.csv', 'a+') as f:
        write = csv.writer(f)
        for any_id in ra_index:
            write.writerow([any_id,"; ".join(list(ra_index[any_id]))])

    return (str(len(br_index)), str(len(ra_index)))


def main():
    global _config

    parser = argparse.ArgumentParser(description='Store the metadata of OpenCitations Meta in Redis')
    parser.add_argument('--dump', type=str, required=True,help='The directory of CSVs or file (in ZIP or TAR.GZ) representing OpenCitations Meta dump')
    #parser.add_argument('--db', type=str, required=True,help='The destination DB in redis. The specified DB is used to store <any-id>:<omid> data, while DB+1 is used to store <omid>:{METADATA}')
    #parser.add_argument('--port', type=str, required=False,help='The port of redis', default="6379")

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Start uploading data to Redis.")

    res = upload2redis(
        dump_path = args.dump,
        # Redis main conf
        redishost = _config.get("redis", "host"),
        redisport = _config.get("redis", "port"),
        redisbatchsize = _config.get("redis", "batch_size"),
        db_omid = _config.get("cnc", "db_omid"),
        db_br = _config.get("cnc", "db_br"),
        db_ra = _config.get("cnc", "db_ra"),
        db_metadata = _config.get("INDEX", "db")
    )

    logger.info("A total of unique "+str(res[0])+" BR OMIDs and "+str(res[1])+" RA OMIDs have been found and added to Redis.")

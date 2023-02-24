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
import os
import datetime
import io
import argparse
from redis import Redis
import re
import sys

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
csv.field_size_limit(sys.maxsize)


def upload2redis(dump_path="", redishost="localhost", redisport="6379", redisbatchsize="10000", br_ids =[], ra_ids=[], db_br="10", db_ra="11", db_metadata="12"):

    rconn_db_br = Redis(host=redishost, port=redisport, db=db_br)
    rconn_db_ra = Redis(host=redishost, port=redisport, db=db_ra)
    rconn_db_metadata = Redis(host=redishost, port=redisport,db=db_metadata)

    # set buffers
    db_br_buffer = []
    db_ra_buffer = []
    db_metadata_buffer = []

    count_br = 0
    count_ra = 0
    for filename in os.listdir(dump_path):
        fzip = os.path.join(dump_path, filename)
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                # Each CSV file contain (i.e., CSV header):
                # "id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
                for csv_name in archive.namelist():
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.DictReader(
                            io.TextIOWrapper(csv_file)))

                        # elaborate each citation in the DUMP
                        for o_row in l_cits:
                            #check BRs
                            for col in ["id","publisher"]:
                                re_id = re.search("(meta\:br\S[^\]\s]+)", o_row[col])
                                if re_id:
                                    meta_id = re_id.group(1).replace("meta:br/","omid:")
                                    entity_value = {
                                        "date": str(o_row["pub_date"]),
                                        "valid": True,
                                        "orcid": re.findall("orcid\:(\S[^\]\s]+)", o_row["author"]),
                                        "issn": re.findall("issn\:(\S[^\]\s]+)", o_row["venue"])
                                    }
                                    db_metadata_buffer.append( (meta_id,json.dumps(entity_value)) )

                                    other_ids = re.findall("(("+"|".join(br_ids)+")\:\S[^\]\s]+)", o_row[col])
                                    for oid in other_ids:
                                        db_br_buffer.append( (oid[0],meta_id) )
                                        count_br += 1

                            #check RAs
                            for col in ["author","editor"]:
                                for item in o_row[col].split(";"):
                                    re_id = re.search("(meta\:ra\S[^\]\s]+)", item)
                                    if re_id:
                                        meta_id = re_id.group(1).replace("meta:ra/","omid:")
                                        other_ids = re.findall("(("+"|".join(ra_ids)+")\:\S[^\]\s]+)", item)
                                        for oid in other_ids:
                                            db_ra_buffer.append( (oid[0],meta_id) )
                                            count_ra += 1

                            #update redis DBs
                            if len(db_metadata_buffer) >= int(redisbatchsize):
                                for item in db_metadata_buffer:
                                    rconn_db_metadata.set(item[0], item[1])
                                db_metadata_buffer = []

                            if len(db_br_buffer) >= int(redisbatchsize):
                                for item in db_br_buffer:
                                    rconn_db_br.set(item[0], item[1])
                                db_br_buffer = []

                            if len(db_ra_buffer) >= int(redisbatchsize):
                                for item in db_ra_buffer:
                                    rconn_db_ra.set(item[0], item[1])
                                db_ra_buffer = []

    return (count_br, count_ra)


def main():
    global _config

    parser = argparse.ArgumentParser(description='Upload the data of META to Redis. Creates 3 DB on Redis: (DB-1)BRs in META; (DB-2)RAs in META; (DB-3)Metadata (e.g., publication date) of BRs in META')
    parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the META dump')
    #parser.add_argument('--db', type=str, required=True,help='The destination DB in redis. The specified DB is used to store <any-id>:<omid> data, while DB+1 is used to store <omid>:{METADATA}')
    #parser.add_argument('--port', type=str, required=False,help='The port of redis', default="6379")

    args = parser.parse_args()
    logger = get_logger()


    res = upload2redis(
        dump_path = args.dump,
        redishost = _config.get("redis", "host"),
        redisport = _config.get("redis", "port"),
        redisbatchsize = _config.get("redis", "batch_size"),
        br_ids = _config.get("cnc", "br_ids").split(","),
        ra_ids = _config.get("cnc", "ra_ids").split(","),
        db_br = _config.get("cnc", "db_br"),
        db_ra = _config.get("cnc", "db_ra"),
        db_metadata = _config.get("INDEX", "db")
    )

    logger.info("A total of "+str(res[0])+" BRs and "+str(res[1])+" RAs have been added to Redis.")

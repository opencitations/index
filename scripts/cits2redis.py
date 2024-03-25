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

from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
csv.field_size_limit(sys.maxsize)

class RedisDB(object):

    def __init__(self, redishost, redisport, redisbatchsize, _db):
        self.redisbatchsize = int(redisbatchsize)
        self.rconn = Redis(host=redishost, port=redisport, db=_db)

    def set_data(self, data, force=False):
        if len(data) >= self.redisbatchsize or force:
            for item in data:
                self.rconn.set(item[0], item[1])
            return len(data)
        return 0

def upload2redis(dump_path="", redishost="localhost", redisport="6379", redisbatchsize="10000", db_cits="8"):
    global _config
    logger = get_logger()

    rconn_db_cits =  RedisDB(redishost, redisport, redisbatchsize, db_cits)

    # set buffers
    db_cits_buffer = []

    for filename in os.listdir(dump_path):
        fzip = os.path.join(dump_path, filename)
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                logger.info("Total number of files in the archive is:"+str(len(archive.namelist())))
                # Each CSV file contain (i.e., CSV header):
                # "id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
                for csv_name in archive.namelist():
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.reader(io.TextIOWrapper(csv_file)))
                        # walk through each citation in the CSV
                        logger.info("Walking through the citations of: "+str(csv_name))
                        for o_row in tqdm(l_cits):
                            oci = o_row[0]
                            db_cits_buffer.append( (oci,"1") )
                            #update redis DB
                            if rconn_db_cits.set_data(db_cits_buffer) > 0:
                                db_cits_buffer = []


    # Set last data in Redis
    rconn_db_cits.set_data(db_cits_buffer, True)
    return 1


def main():
    global _config

    parser = argparse.ArgumentParser(description='Upload the data of META to Redis. Creates 3 DB on Redis: (DB-1)BRs in META; (DB-2)RAs in META; (DB-3)Metadata (e.g., publication date) of BRs in META')
    parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the META dump')
    #parser.add_argument('--db', type=str, required=True,help='The destination DB in redis. The specified DB is used to store <any-id>:<omid> data, while DB+1 is used to store <omid>:{METADATA}')
    #parser.add_argument('--port', type=str, required=False,help='The port of redis', default="6379")

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Start uploading data to Redis.")

    res = upload2redis(
        dump_path = args.dump,
        redishost = _config.get("redis", "host"),
        redisport = _config.get("redis", "port"),
        redisbatchsize = _config.get("redis", "batch_size"),
        db_cits = _config.get("cnc", "db_cits")
    )

    logger.info("Done!")

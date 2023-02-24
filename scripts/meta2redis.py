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

from oc.index.utils.config import get_config

csv.field_size_limit(sys.maxsize)


def upload2redis(archive_path="", redisdb="10", redisport="6379"):
    rconn_ids = Redis(host="localhost", port=redisport, db=redisdb)
    rconn_data = Redis(host="localhost", port=redisport,
                       db=str(int(redisdb)+1))

    count = 0
    count_all = 0
    for filename in os.listdir(archive_path):
        fzip = os.path.join(archive_path, filename)
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                print("FILES_IN_ARCHIVE: "+str(len(archive.namelist())))
                # Each CSV file contain (i.e., CSV header):
                # "id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
                # To redis we need:
                # "date": <str>, "valid": true, "issn": [<str>], "orcid": [<str>]}"
                for csv_name in archive.namelist():
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.DictReader(
                            io.TextIOWrapper(csv_file)))

                        # elaborate each citation in the DUMP
                        count_all += len(l_cits)
                        for o_row in l_cits:
                            re_id = re.search(
                                "(meta\:br\S[^\]\s]+)", o_row["id"])
                            if re_id:
                                meta_id = re_id.group(1).replace("meta:br/","omid:")
                                entity_value = {
                                    "date": str(o_row["pub_date"]),
                                    "valid": True,
                                    "orcid": re.findall("orcid\:(\S[^\]\s]+)", o_row["author"]),
                                    "issn": re.findall("issn\:(\S[^\]\s]+)", o_row["venue"])
                                }
                                rconn_data.set(
                                    meta_id, json.dumps(entity_value))

                                other_ids = re.findall(
                                    "((doi|pmid)\:\S[^\]\s]+)", o_row["id"])
                                for oid in other_ids:
                                    rconn_ids.set(oid[0], meta_id)

                                count += 1
    return (count,count_all)


parser = argparse.ArgumentParser(
    description='Upload the data of META to Redis. Creates 3 DB on Redis: (DB-1)BRs in META; (DB-2)RAs in META; (DB-3)Metadata (e.g., publication date) of BRs in META')
parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the META dump')
#parser.add_argument('--db', type=str, required=True,help='The destination DB in redis. The specified DB is used to store <any-id>:<omid> data, while DB+1 is used to store <omid>:{METADATA}')
#parser.add_argument('--port', type=str, required=False,help='The port of redis', default="6379")

args = parser.parse_args()


# init the dump
res = upload2redis(args.dump, args.db, args.port)
print("A total of "+str(res[0])+" out of "+str(res[1])+" have been added to redis on DB="+ args.db+" and DB="+args.db+"+1")

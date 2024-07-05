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
import io
import argparse
from redis import Redis
import sys

from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
csv.field_size_limit(sys.maxsize)

def upload2redis(dump_zip="", redishost="localhost", redisport="6379", redisbatchsize="10000", db_cits="8"):
    global _config
    logger = get_logger()
    rconn = Redis(host=redishost, port=redisport, db=db_cits)

    # Get all the citations from the Dump: citing and cited entites
    # Populate the redis DB such that: <cited>: [<citing-1>, <citing-2>, ... <citing-n>]
    index_cited = defaultdict(list)
    citing_entities_set = set()
    all_entities_set = set()
    if fzip.endswith(".zip"):
        with ZipFile(fzip) as archive:
            for csv_name in tqdm(archive.namelist()):
                with archive.open(csv_name) as csv_file:
                    l_cits = list(csv.reader(io.TextIOWrapper(csv_file)))
                    for o_row in tqdm(l_cits):
                        oci = o_row[0]
                        citing = oci.split("-")[0]
                        cited = oci.split("-")[1]
                        index_cited[cited].append(citing)
                        citing_entities_set.add(citing)
                        all_entities_set.add(citing)
                        all_entities_set.add(cited)

    tot_cited_entities = str(len(index_cited.keys()))

    logger.info("Store coverage stats in CSV ...")
    with open('coverage_stats.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(["#all_unique_entities","#citing_unique_entities","#cited_unique_entities"])
        write.writerow([
            str(len(all_entities_set)),
            str(len(citing_entities_set)),
            str(len(index_cited.keys()))
        ])

    logger.info("Store in Redis and in CSV ...")
    with open('citations_index.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(["cited","citing"])
        for _k, _v in data.items():
            rconn.set(_k, _v)
            write.writerow([_k,"; ".join(_v)])

    logger.info("Store citation count (OMIDs) in CSV ...")
    with open('citations_index.csv', 'w') as f:
        write = csv.writer(f)
        write.writerow(["omid","citations"])
        for _k, _v in data.items():
            write.writerow([_k,str(len(_v))])


def main():
    global _config

    parser = argparse.ArgumentParser(description='Store the citations of OpenCitations Index in Redis')
    parser.add_argument('--dump', type=str, required=True,help='The ZIP file containing the CSV dump with the data (citations) of OpenCitations Index')

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Start uploading citations data to Redis...")

    res = upload2redis(
        # ZIP DUMP
        dump_zip = args.dump,
        # REDIS conf
        redishost = _config.get("redis", "host"),
        redisport = _config.get("redis", "port"),
        redisbatchsize = _config.get("redis", "batch_size"),
        # REDIS DBs
        db_cits = _config.get("cnc", "db_cits")
    )

    logger.info("Done!")

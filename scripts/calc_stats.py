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

csv.field_size_limit(sys.maxsize)

def export_redis_to_csv(redis_host, redis_port, redis_db, output_file):
    r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db)
    keys = r.keys('*')
    index = defaultdict(list)
    for key in keys:
        value = r.get(key)
        if value:
            omid = value.decode('utf-8')
            anyid = key.decode('utf-8')
            index[omid].append(anyid)

    data = []
    for omid in index:
        data.append(
            {
                "omid": omid,
                "id": " ".join(index[omid])
            }
        )

    with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
        fieldnames = ['omid', 'id']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def calc_stats(dump_path=None):
    logger = get_logger()

    logger.info("Create OMID mapper CSV ...")
    # OMID > ID(s)
    # E.G. omid:br/100, doi:10.123/12 pmid:123
    export_redis_to_csv("localhost", 6379, 10, "omid.csv")
    logger.info("Done!")

    str_stats = "Dump not found!"

    all_entities = set()
    citing_entities = defaultdict(int)
    cited_entities = defaultdict(int)

    if dump_path != None:
        for filename in os.listdir(dump_path):
            fzip = os.path.join(dump_path, filename)
            if fzip.endswith(".zip"):
                with ZipFile(fzip) as archive:
                    logger.info("Total number of files in the archive "+str(fzip)+" is: "+str(len(archive.namelist())))
                    # Each CSV file contain (i.e., CSV header):
                    # "oci", "citing", "cited", "creation", "timespan", "journal_sc", "author_sc"
                    for csv_name in archive.namelist():
                        if not csv_name.endswith(".csv"):
                            logger.info("File: "+str(csv_name)+" not a CSV file!")
                            continue
                        with archive.open(csv_name) as csv_file:
                            l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))
                            logger.info("Walking through the citations of: "+str(csv_name))
                            for o_row in tqdm(l_cits):
                                all_entities.add(o_row["citing"])
                                all_entities.add(o_row["cited"])
                                cited_entities[o_row["cited"]] += 1
                                citing_entities[o_row["citing"]] += 1

        with open('references.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(["omid","references"])
            for id in citing_entities:
                write.writerow([id,citing_entities[id]])

        with open('citations.csv', 'w') as f:
            write = csv.writer(f)
            write.writerow(["omid","citations"])
            for id in cited_entities:
                id = id.replace("omid:","")
                write.writerow([id,cited_entities[id]])

        str_stats = "#entites = "+str(len(all_entities)) + "\n"
        str_stats += "#citing_entities = "+str(len(citing_entities)) + "\n"
        str_stats += "#cited_entities = "+str(len(cited_entities)) + "\n"

    print(str_stats)
    return 1


def main():
    global _config

    parser = argparse.ArgumentParser(description='Calculates basic stats over the CSV dump of INDEX and produces the 3 support files (CSV): (1) citations.csv (citation count of each OMID), references.csv (reference count of each OMID), and omid.csv (OMID > ID(s) mapping)')
    parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the CSV dump of INDEX (as it is represented in the figshare dump)')

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Calculating stats ...")
    calc_stats(args.dump)

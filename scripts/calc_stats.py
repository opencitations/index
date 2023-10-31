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


def calc_stats(dump_path=None)

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

        with open('refrences.csv', 'w') as f:
            write = csv.writer(f)
            for id in citing_entites:
                write.writerow([id,citing_entites[id]])

        with open('citations.csv', 'w') as f:
            write = csv.writer(f)
            for id in cited_entities:
                write.writerow([id,cited_entities[id]])

        str_stats = "#entites = "+str(len(all_entities)) + "\n"
        str_stats = "#citing_entites = "+str(len(citing_entities)) + "\n"
        str_stats = "#cited_entites = "+str(len(cited_entities)) + "\n"

    print(str_stats)
    return 1


def main():
    global _config

    parser = argparse.ArgumentParser(description='Calculate basic stats over the CSV dump of INDEX')
    parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the CSV dump of INDEX')

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Calculating stats ...")
    calc_stats(args.dump)

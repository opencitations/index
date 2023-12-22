#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
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

import multiprocessing
import os
import time
import csv
import redis
from zipfile import ZipFile
import json
import io
import re

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime, timezone
from collections import defaultdict

from oc.index.parsing.base import CitationParser
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config
from oc.index.finder.base import ResourceFinderHandler
from oc.index.finder.base import OMIDResourceFinder
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.oci.citation import Citation, OCIManager
from oc.index.oci.storer import CitationStorer
from oc.index.glob.redis import RedisDataSource
from oc.index.glob.csv import CSVDataSource

_config = get_config()

def wr_with_buffer(data, f_out, buffer, force = False):
    if len(data) >= buffer or force:
        with open(f_out, 'a+') as f:
            write = csv.writer(f)
            write.writerows([[e] for e in data])
            return len(data)
    return 0

def get_from_redis(data, redis_db, buffer, force = False):
    not_in_redis = []
    if len(data) >= buffer or force:
        for k, val in zip( data , redis_db.mget(data) ):
            if val == None:
                not_in_redis.append(k)
    return not_in_redis


def normalize_dump(input_dir, output_dir, mapping_file):
    global _config
    logger = get_logger()

    # get the INDEX service values from the CONFIG.INI
    idbase_url = _config.get("INDEX", "idbaseurl")
    index_identifier = _config.get("INDEX", "identifier")
    agent = _config.get("INDEX", "agent")
    service_name = _config.get("INDEX", "service")
    baseurl = _config.get("INDEX", "baseurl")

    identifier = ""
    citing_col = "citing"
    cited_col = "cited"

    redis_cits = redis.Redis(
        host="127.0.0.1",
        port="6379",
        db=_config.get("cnc", "db_cits")
    )

    invalidated_cits = []
    valid_cits = []
    omid_mapper = dict()
    WR_BUFFER = 10000
    BUFFER_REDIS = 10000

    # Load the mapping table
    with open(mapping_file, 'r') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            if "meta/br/" in row[0]:
                correct_omid = row[0].split("https://w3id.org/oc/meta/br/")[1]
                for duplicated_omid in row[1].split("; "):
                    if "meta/br/" in duplicated_omid:
                        duplicated_omid = duplicated_omid.split("https://w3id.org/oc/meta/br/")[1]
                        omid_mapper[duplicated_omid] = correct_omid

    files_to_process = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith('.ttl'):
                files_to_process.append(os.path.join(root, file))

    for idx,file in enumerate(files_to_process):
        with open(os.path.join(input_dir, file), 'r') as ttl_file:
            lines = ttl_file.readlines()
            for line in lines:
                if line.strip() != "":
                    oci_pattern = r"https://w3id.org/oc/index/ci/(\d{1,}-\d{1,})"
                    oci = re.search(oci_pattern, line)
                    # if an OCI is found
                    if oci:
                        oci = oci.group(1)
                        citing_omid = oci.split("-")[0]
                        cited_omid = oci.split("-")[1]

                        new_citing_omid = citing_omid
                        new_cited_omid = cited_omid

                        if citing_omid in omid_mapper:
                            new_citing_omid = omid_mapper[citing_omid]

                        if cited_omid in omid_mapper:
                            new_cited_omid = omid_mapper[cited_omid]

                        new_oci = new_citing_omid + "-" + new_cited_omid

                        #check if the citation has been modified
                        if new_oci != oci:
                            invalidated_cits.append(oci)
                            valid_cits.append(new_oci)

            num_wr_invalidated = wr_with_buffer(invalidated_cits, output_dir+'invalidated_cits.csv', WR_BUFFER, idx == len(files_to_process)-1 )
            if num_wr_invalidated > 0:
                logger.info("> "+str(num_wr_invalidated)+" citations have been invalidated!")
                invalidated_cits = []

            new_cits = get_from_redis(valid_cits, redis_cits, BUFFER_REDIS, idx == len(files_to_process)-1)
            num_new_cits = wr_with_buffer(new_cits, output_dir+'new_cits.csv', WR_BUFFER, idx == len(files_to_process)-1)
            if num_new_cits > 0:
                logger.info("> "+str(num_new_cits)+" new citations!")
                valid_cits = []


def main():
    global _config
    logger = get_logger()

    arg_parser = ArgumentParser(description="Normalize the data of OpenCitations Index")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory contatining compressed file(s) (ZIP format) or the original files in TTL",
    )
    arg_parser.add_argument(
        "-m",
        "--map",
        required=True,
        help="CSV file to map duplicates",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The destination directory to save outputs",
    )

    args = arg_parser.parse_args()

    # input directory/file
    input_dir = args.input + "/" if args.input[-1] != "/" else args.input

    # output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #mapping file
    mapping_file = args.map

    # call the normalize_dump function
    normalize_dump(input_dir, output_dir, mapping_file)

    logger.info("Done !!")

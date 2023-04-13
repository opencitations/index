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

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime, timezone

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

def normalize_dump(service, input_files, output_dir):
    global _config
    logger = get_logger()

    # get the service values from the CONFIG.INI
    idbase_url = _config.get("INDEX", "idbaseurl")
    index_identifier = _config.get("INDEX", "identifier")
    agent = _config.get("INDEX", "agent")
    service_name = _config.get("INDEX", "service")
    baseurl = _config.get("INDEX", "baseurl")

    # service variables
    identifier = _config.get(service, "identifier")
    source = _config.get(service, "ocdump")

    # redis DB of <ANYID>:<OMID>
    redis_br = redis.Redis(
        host="127.0.0.1",
        port="6379",
        db=_config.get("cnc", "db_br")
    )

    # redis DB of <OCI>:1
    redis_cits = redis.Redis(
        host="127.0.0.1",
        port="6379",
        db=_config.get("cnc", "db_cits")
    )

    # redis DB of <OMID>:<METADATA>
    redis_index = RedisDataSource("INDEX")


    for fzip in input_files:
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                logger.info("Working on the archive:"+str(fzip))
                logger.info("Total number of files in archive is:"+str(len(archive.namelist())))

                # CSV header: oci,citing,cited,creation,timespan,journal_sc,author_sc
                for csv_name in archive.namelist():

                    if not csv_name.endswith(".csv"):
                        logger.info("Skip file (not a CSV): "+str(csv_name))
                        continue

                    citing_entities_with_no_omid = set()
                    cited_entities_with_no_omid = set()
                    citing_entities_with_omid = set()
                    cache = dict()

                    logger.info("Converting the citations in: "+str(csv_name))
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))

                        logger.info("#citations to check is: "+str(len(l_cits)))
                        # iterate citations (CSV rows)
                        for row in tqdm(l_cits):

                            citing = row["citing"]
                            citing_omid = cache[citing] if citing in cache else None
                            if citing_omid == None:
                                citing_omid = redis_br.get(identifier+":"+citing)
                                cache[citing] = citing_omid

                            cited = row["cited"]
                            cited_omid = cache[cited] if cited in cache else None
                            if cited_omid == None:
                                cited_omid = redis_br.get(identifier+":"+cited)
                                cache[cited] = cited_omid

                            if citing_omid == None:
                                citing_entities_with_no_omid.add(citing)
                            else:
                                citing_entities_with_omid.add(citing)

                            if cited_omid == None:
                                cited_entities_with_no_omid.add(cited)

                    logger.info("citing entities with NO OMID="+str(len(citing_entities_with_no_omid)))
                    logger.info("Cited entities with NO OMID="+str(len(cited_entities_with_no_omid)))
                    logger.info("citing entities with OMID="+str(len(citing_entities_with_omid)))

                    # Store citing_entities_with_no_omid
                    logger.info("Saving citing entities with NO omid...")
                    with open(output_dir+'citing_entities_with_no_omid.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerows([[e] for e in citing_entities_with_no_omid])

                    # Store citing_entities_with_no_omid
                    logger.info("Saving cited entities with NO omid...")
                    with open(output_dir+'cited_entities_with_no_omid.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerows([[e] for e in cited_entities_with_no_omid])

                    # Store citing_entities_with_no_omid
                    logger.info("Saving citing entities with omid...")
                    with open(output_dir+'citing_entities_with_omid.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerows([[e] for e in citing_entities_with_omid])

    # remove duplicates from files
    for f in ["citing_entities_with_no_omid.csv","cited_entities_with_no_omid.csv","citing_entities_with_omid.csv"]:
        index_entities = set()
        with open(output_dir+f) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                index_entities.add(row)
        with open(output_dir+f, 'w') as f:
            csv.writer(f).writerows([[e] for e in index_entities])


def main():
    global _config

    arg_parser = ArgumentParser(description="Check entities")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory/Zipfile",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        default="COCI",
        help="The source of the dump (e.g. COCI, DOCI)",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The output directory where citations will be stored",
    )
    args = arg_parser.parse_args()
    service = args.service

    # input directory/file
    input_files = []
    if os.path.isdir(args.input):
        input = args.input + "/" if args.input[-1] != "/" else args.input
        for filename in os.listdir(input):
            input_files.append(os.path.join(input, filename))
    else:
        input_files.append(args.input)

    # output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # call the normalize_dump function
    normalize_dump(service, input_files, output_dir)

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
_logger = get_logger()
csv.field_size_limit(sys.maxsize)
rconn = Redis(
    host=_config.get("redis", "host"),
    port=_config.get("redis", "port"),
    db=_config.get("redis", "db_cits")
)

def upload2redis(dump_path="", intype=""):
    """
    ...
    Args:
        dump_path (string, mandatory): ...
        intype (string, mandatory): ...

    Returns:
        ...
    """

    # Get all the citations from the Dump: citing and cited entites
    # Populate the redis DB such that: <cited>: [<citing-1>, <citing-2>, ... <citing-n>]
    index_cited = defaultdict(list)
    all_ocis = []
    citing_entities_set = set()

    if intype == "ZIP":
        for filename in os.listdir(dump_path):
            fzip = os.path.join(dump_path, filename)
            # checking if it is a file
            if fzip.endswith(".zip"):
                _logger.info("Reading "+str(fzip)+ " ...")
                with ZipFile(fzip) as archive:
                    for filename in tqdm(archive.namelist()):
                        if filename.endswith(".ttl"):
                            with open(filename, "r", encoding="utf-8") as f:
                                for line in f:
                                    if needle in line:
                                        # extract the part between "ci/" and ">"
                                        start = line.find("ci/") + 3
                                        end = line.find(">", start)
                                        oci = line[start:end]
                                        all_ocis.append(oci)
    elif intype == "TTL":
        for filename in os.listdir(dump_path):
            if filename.endswith(".ttl"):
                with open(filename, "r", encoding="utf-8") as f:
                    for line in f:
                        if needle in line:
                            # extract the part between "ci/" and ">"
                            start = line.find("ci/") + 3
                            end = line.find(">", start)
                            oci = line[start:end]
                            all_ocis.append(oci)

    for oci in all_ocis:
        citing = oci.split("-")[0]
        cited = oci.split("-")[1]
        index_cited[cited].append(citing)
        citing_entities_set.add(citing)

    _logger.info(f"Storing {len(all_ocis)} citations in Redis (<CITED>:[<CITING-1>, ... <CITING-n>]) ...")
    rconn.mset( { _k:json.dumps(_v) for _k, _v in index_cited.items() } )


def main():
    parser = argparse.ArgumentParser(description='Store the citations of OpenCitations Index in Redis')
    parser.add_argument(
        '--dump',
        type=str,
        required=True,
        help='The directory containing ZIP files storing the CSV dump with the data (citations) of OpenCitations Index'
    )
    parser.add_argument(
        "-t",
        "--intype",
        required=True,
        help="The format of the files in the input directory, i.e. TTL or ZIP",
    )
    args = parser.parse_args()

    _logger.info("Uploading citations in RDF format to Redis ...")
    upload2redis(args.dump,args.intype)
    _logger.info("Done!")

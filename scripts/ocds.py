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

import os
import time
from sys import platform

from argparse import ArgumentParser
from subprocess import check_output
from tqdm import tqdm
from errno import ENOENT

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config
from oc.index.glob.redis import RedisDataSource


def process_glob_file(ds, filename, column, append=False):
    logger = get_logger()
    config = get_config()
    logger.info("Processing " + filename)
    tqdm_disabled = False
    lines = 0
    if platform == "win32":
        tqdm_disabled = True
    else:
        lines = int(
            check_output(
                ["wc", "-l", filename],
            ).split()[0]
        )

    logger.info("Reading values...")
    fp = open(filename, "r")
    fp.readline()
    pbar = tqdm(total=lines, disable=tqdm_disabled)

    batch_size = config.getint("redis", "batch_size")
    buffer_keys = []
    buffer_values = []
    while True:
        line = fp.readline()
        resources = {}

        # Flushing buffered data into the datasource
        if len(buffer_keys) > batch_size or not line:
            if len(buffer_keys) > 0:
                entries = ds.mget(buffer_keys)
                for key in buffer_keys:
                    entry = entries[key]
                    if entries[key] is None:
                        entry = ds.new()
                    if append:
                        entry[column].append(value)
                    else:
                        if column == "valid":
                            entry[column] = value.strip() == "v"
                        else:
                            entry[column] = value.strip()
                    resources[key] = entry
                ds.mset(resources)
                pbar.update(len(buffer_keys))
                buffer_keys = []
                buffer_values = []

        if not line:
            break

        if "," in line:
            splits = line.split('",')
            if len(splits) > 2:
                key = splits[0].replace('"', "")
                value = splits[1].replace('"', "")
                if len(key) > 0 and len(value) > 0:
                    buffer_keys.append(key)
                    buffer_values.append(value)

    logger.info("Values read")
    logger.info("Updating the datasource...")
    start = time.time()
    logger.info(f"Datasource updated in {start-time.time()} seconds")
    fp.close()
    logger.info(filename + " processed")


def main():
    arg_parser = ArgumentParser(description="OCDS - OpenCitations Data Source Manager")
    arg_parser.add_argument(
        "-o",
        "--operation",
        required=True,
        choices=["populate"],
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        required=False,
        help="Input to parse and use for the operation",
    )
    args = arg_parser.parse_args()

    logger = get_logger()

    # Arguments
    input = args.input

    id_date = os.path.join(input, "id_date.csv")
    if not os.path.exists(id_date):
        logger.error("id_date.csv not found in the input directory")

    id_issn = os.path.join(input, "id_issn.csv")
    if not os.path.exists(id_issn):
        logger.error("id_issn.csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), id_issn)

    id_orcid = os.path.join(input, "id_orcid.csv")
    if not os.path.exists(id_orcid):
        logger.error("id_orcid.csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), id_orcid)

    valid_doi = os.path.join(input, "valid_doi.csv")
    if not os.path.exists(valid_doi):
        logger.error("valid_doi.csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), valid_doi)

    ds = RedisDataSource()

    logger.info("Populating the datasource with glob files...")
    start = time.time()
    process_glob_file(ds, valid_doi, "valid")
    process_glob_file(ds, id_date, "date")
    process_glob_file(ds, id_issn, "issn", True)
    process_glob_file(ds, id_orcid, "orcid", True)
    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

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

_config = get_config()


def process_glob_file(ds, filename, column, identifier, append=False):
    logger = get_logger()
    global _config
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

    batch_size = _config.getint("redis", "batch_size")
    buffer_keys = []
    buffer_values = []
    while True:
        line = fp.readline()
        resources = {}

        # Flushing buffered data into the datasource
        if len(buffer_keys) > batch_size or not line:
            if len(buffer_keys) > 0:
                entries = ds.mget(list(set(buffer_keys)))
                for i, key in enumerate(buffer_keys):
                    value = buffer_values[i]
                    entry = entries[key]
                    if entries[key] is None:
                        entry = ds.new()
                    if append:
                        if not value in entry[column]:
                            entry[column].append(value)
                    else:
                        if column == "valid":
                            entry[column] = value.strip() == "v"
                        else:
                            entry[column] = value.strip()
                    resources[key] = entry
                    entries[key] = entry
                ds.mset(resources)
                pbar.update(len(buffer_keys))
                buffer_keys = []
                buffer_values = []

        if not line:
            break

        if "," in line:
            splits = line.replace("\n", "").split('",')
            if len(splits) >= 2:
                key = splits[0].replace('"', "")
                if identifier not in key:
                    key = identifier + ":" + key
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
    global _config
    arg_parser = ArgumentParser(description="OCDS - OpenCitations Data Source Manager")
    arg_parser.add_argument(
        "-o",
        "--operation",
        required=True,
        choices=["csv2redis"],
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        required=True,
        choices=_config.get("cnc", "services").split(","),
        help="Service config to use, e.g. for parser, identifier type, etc..",
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Input to parse and use for the operation",
    )
    arg_parser.add_argument(
        "-id",
        "--identifier",
        required=True,
        choices=_config.get("cnc", "identifiers").split(","),
        help="The identifier used for citing and cited in the input documents",
    )
    args = arg_parser.parse_args()

    logger = get_logger()

    # Arguments
    input = args.input
    service = args.service
    identifier = args.identifier

    id_orcid = os.path.join(input, "id_orcid.csv")
    if not os.path.exists(id_orcid):
        logger.error("id_orcid.csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), id_orcid)

    id_issn = os.path.join(input, "id_issn.csv")
    if not os.path.exists(id_issn):
        logger.error("id_issn.csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), id_issn)

    id_date = os.path.join(input, "id_date.csv")
    if not os.path.exists(id_date):
        logger.error("id_date.csv not found in the input directory")

    valid_id = os.path.join(input, "valid_" + identifier + ".csv")
    if not os.path.exists(valid_id):
        logger.error("valid_" + identifier + ".csv not found in the input directory")
        raise FileNotFoundError(ENOENT, os.strerror(ENOENT), valid_id)

    ds = RedisDataSource(service)

    logger.info("Populating the datasource with glob files...")
    start = time.time()
    process_glob_file(ds, id_orcid, "orcid", identifier, True)
    process_glob_file(ds, id_date, "date", identifier)
    process_glob_file(ds, valid_id, "valid", identifier)
    process_glob_file(ds, id_issn, "issn", identifier, True)
    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

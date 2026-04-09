#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021, 2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021, 2022 Giuseppe Grieco <g.grieco1997@gmail.com>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv
import json
from zipfile import ZipFile
import os
import argparse
from redis import Redis
import sys

from tqdm import tqdm
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

csv.field_size_limit(sys.maxsize)

NEEDLE = "ci/"
BATCH_SIZE = 50_000


def extract_oci_from_line(line):
    """Extract OCI from a TTL line."""
    if NEEDLE not in line:
        return None

    start = line.find("ci/") + 3
    end = line.find(">", start)

    if start == -1 or end == -1:
        return None

    return line[start:end]


def upload2redis(rconn, logger, dump_path="", intype=""):
    intype = intype.upper()
    pipe = rconn.pipeline()
    counter = 0
    total = 0

    def flush_pipeline():
        nonlocal counter
        if counter > 0:
            pipe.execute()
            counter = 0

    logger.info("Starting streaming upload to Redis...")

    if intype == "ZIP":
        for filename in os.listdir(dump_path):
            fzip = os.path.join(dump_path, filename)

            if fzip.endswith(".zip") and os.path.isfile(fzip):
                logger.info(f"Reading {fzip} ...")

                with ZipFile(fzip) as archive:
                    for member in tqdm(archive.namelist()):
                        if member.endswith(".ttl"):
                            with archive.open(member) as f:
                                for raw_line in f:
                                    line = raw_line.decode("utf-8", errors="ignore")
                                    oci = extract_oci_from_line(line)

                                    if not oci:
                                        continue

                                    try:
                                        citing, cited = oci.split("-", 1)
                                    except ValueError:
                                        continue

                                    pipe.sadd(cited, citing)
                                    counter += 1
                                    total += 1

                                    if counter >= BATCH_SIZE:
                                        flush_pipeline()

    elif intype == "TTL":
        for filename in os.listdir(dump_path):
            fttl = os.path.join(dump_path, filename)

            if fttl.endswith(".ttl") and os.path.isfile(fttl):
                logger.info(f"Reading {fttl} ...")

                with open(fttl, "r", encoding="utf-8") as f:
                    for line in f:
                        oci = extract_oci_from_line(line)

                        if not oci:
                            continue

                        try:
                            citing, cited = oci.split("-", 1)
                        except ValueError:
                            continue

                        pipe.sadd(cited, citing)
                        counter += 1
                        total += 1

                        if counter >= BATCH_SIZE:
                            flush_pipeline()

    else:
        raise ValueError("intype must be either 'ZIP' or 'TTL'")

    # Flush remaining operations
    flush_pipeline()

    logger.info(f"Stored {total} citations in Redis successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Store the citations of OpenCitations Index in Redis"
    )

    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to the configuration file (config.ini)",
    )

    parser.add_argument(
        "--dump",
        type=str,
        required=True,
        help="Directory containing TTL or ZIP RDF dump files"
    )

    parser.add_argument(
        "-t",
        "--intype",
        required=True,
        help="Input file type: TTL or ZIP",
    )

    args = parser.parse_args()

    _config = get_config(args.config)
    _logger = get_logger()

    rconn = Redis(
        host=_config.get("redis", "host"),
        port=int(_config.get("redis", "port")),
        db=int(_config.get("cnc", "db_cits"))
    )

    _logger.info("Uploading citations in RDF format to Redis ...")
    upload2redis(rconn, _logger, args.dump, args.intype)
    _logger.info("Done!")


if __name__ == "__main__":
    main()

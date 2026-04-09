# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021, 2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021, 2022 Giuseppe Grieco <g.grieco1997@gmail.com>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import csv

from argparse import ArgumentParser
from redis import Redis
from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config



def main():

    arg_parser = ArgumentParser(description="summary/Report dumps regarding OpenCitations Index")
    arg_parser.add_argument(
        "--config",
        required=True,
        help="Path to the configuration file (config.ini)",
    )
    args = arg_parser.parse_args()

    _config = get_config(args.config)
    _logger = get_logger()

    r_dbcits = Redis(
        host=_config.get("redis", "host"),
        port=int(_config.get("redis", "port")),
        db=int(_config.get("cnc", "db_cits"))
    )

    CITED_BATCH_SIZE = 1500
    cursor = 0

    # iterate over all the citing entities
    while True:

        # index of entites to process
        # <citing_omid>: [<cited_omid_1>, <cited_omid_2>, <cited_omid_3> ... ]
        cits_pairs_to_process = []
        br_meta = {}

        # get from redis first CITED_BATCH_SIZE citing entites
        cursor, cited_keys = r_dbcits.scan(cursor=cursor, count=CITED_BATCH_SIZE)
        if cited_keys:  # only fetch if we got keys
            citing_values = r_dbcits.mget(cited_keys)

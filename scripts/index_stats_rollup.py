import redis
import csv

from argparse import ArgumentParser
from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
_logger = get_logger()


r_dbcits = Redis(
    host=_config.get("redis", "host"),
    port=_config.get("redis", "port"),
    db=_config.get("redis", "db_cits")
)



def main():

    arg_parser = ArgumentParser(description="summary/Report dumps regarding OpenCitations Index")

    # iterate over all the citing entities
    while True:

        # index of entites to process
        # <citing_omid>: [<cited_omid_1>, <cited_omid_2>, <cited_omid_3> ... ]
        cits_pairs_to_process = []
        br_meta = {}

        # get from redis first CITED_BATCH_SIZE citing entites
        cursor, cited_keys = redis_cits.scan(cursor=cursor, count=CITED_BATCH_SIZE)
        if cited_keys:  # only fetch if we got keys
            citing_values = redis_cits.mget(cited_keys)

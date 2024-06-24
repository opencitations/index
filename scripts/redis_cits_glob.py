import csv
import json
from zipfile import ZipFile
import os
import datetime
import io
from argparse import ArgumentParser
from redis import Redis
import re
import sys

from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

# _config = get_config()

def extract_str_part(s_ttl, part = "citing"):

    g_part = None
    pattern = None

    if part == "source":
            g_part = 1
            pattern = r'<https://w3id\.org/oc/index/([A-Za-z]+)/>'

    elif part == "citing" or part == "cited":
            g_part = 1 if part == "citing" else 2
            pattern = r'<https://w3id\.org/oc/index/ci/([0-9A-Za-z]+)-([0-9A-Za-z]+)>'

    if g_part and pattern:
        match = re.search(pattern, s_ttl)
        if match:
            return match.group(g_part)

    return None


def norm_citations(dict_citations):
    l_res = []
    for k_cited in dict_citations:
        l_res.append( str(k_cited)+":"+";".join(dict_citations[k_cited]) )
    return " ".join(l_res)

def dict_citations(val_citations):
    l_res = {}
    for val_citing in val_citations.decode().split(" "):
        citing_sources = val_citing.split(":")[1].split(";")
        l_res[ val_citing.split(":")[0] ] = [ str(a) for a in citing_sources ]
    return l_res

def process_file(input_file):

    # redis DB of citations glob
    redis_cits = Redis(
        host="127.0.0.1",
        port="6379",
        #db=_config.get("cnc", "db_cits")
        db="4"
    )

    with open(input_file, 'r') as file:
        lines = file.readlines()

    cits_buffer = defaultdict(dict)
    for line in lines:
        # line – EXAMPLE:
        # <https://w3id.org/oc/index/ci/06501832922-06801258849> <http://www.w3.org/ns/prov#atLocation> <https://w3id.org/oc/index/coci/> .
        if line.strip() != "":
            citing=  extract_str_part(line, "citing")
            cited = extract_str_part(line, "cited")
            source = extract_str_part(line, "source")
            if citing not in cits_buffer[cited]:
                cits_buffer[cited][citing] = []
            cits_buffer[cited][citing].append(source)

    # cits_buffer
    # E.G. {'CITING-1': {'CITED-1': [SOURCE], 'CITED-2': [SOURCE]} ... }
    # E.G.
    # {
    #     "06501832922": "06501832922:coci;doci 0680125232:coci 06501832111:coci;poci"
    # }

    # update redis
    infile_cited = cits_buffer.keys()
    inredis_cited = {key: dict_citations(value) for key, value in zip(infile_cited, redis_cits.mget(infile_cited)) if value is not None}

    # first insert new ones in REDIS
    diff_new_items = {key: norm_citations(value) for key, value in cits_buffer.items() if key not in inredis_cited}
    if len(diff_new_items.keys()) > 0:
        redis_cits.mset(diff_new_items)

    # update the ones that are already in REDIS
    for k_cited in inredis_cited:
        for citing in cits_buffer[k_cited]:
            if citing not in inredis_cited[k_cited]:
                inredis_cited[k_cited][citing] = set()
            inredis_cited[k_cited][citing] = list(
                set(inredis_cited[k_cited][citing]).union(cits_buffer[k_cited][citing])
            )

    # second insert updated ones in REDIS
    inredis_cited = {key: norm_citations(value) for key, value in inredis_cited.items()}
    if len(inredis_cited.keys()) > 0:
        redis_cits.mset(inredis_cited)


def main():
    arg_parser = ArgumentParser(description="Build the Redis Global directory of all the citations")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The directory storing the TTL files contatining info about the original data sources / services",
    )

    args = arg_parser.parse_args()
    directory = args.input if args.input[-1] != "/" else args.input[0:-1]

    source_name = ""
    for filename in tqdm(os.listdir(directory)):

        if source_name != directory:
            print("Processing source: "+filename + "  ...")
            source_name = directory

        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            if filename.endswith(".ttl"):
                process_file(file_path)

    print("Done!")


if __name__ == "__main__":
    main()

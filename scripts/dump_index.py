#!python
# Copyright (c) 2025 Ivan Heibi.
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

import redis
import os
import json
import zipfile
from urllib.parse import quote
from datetime import datetime, timezone
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from multiprocessing import Process
# from tqdm import tqdm

# Libraries needed
from oc.index.oci.citation import Citation, OCIManager
from oc.index.utils.config import get_config
from oc.index.utils.logging import get_logger
from oc.index.oci.storer import CitationStorer

# Globals
_config = get_config()
_logger = get_logger()

# <CORE>:[ <List of citations> ]
data_to_dump = defaultdict(list)

# === CONF.INI ===
idbase_url = _config.get("INDEX", "idbaseurl")
baseurl = _config.get("INDEX", "baseurl")
agent = _config.get("INDEX", "agent")
source = _config.get("INDEX", "source")
service_name = _config.get("INDEX", "service")
index_identifier = _config.get("INDEX", "identifier")

# === CONFIGURATION ===
CITED_BATCH_SIZE = 1500
CITED_PER_FILE = 10000
FILES_PER_ZIP = 1000
FILE_OUTPUT_DIR = "_out_"


def zip_and_cleanup(csv_dir, rdf_dir, slx_dir, files_per_zip, force = False, pnum=1):
    """It compresses (zip) the oc index data files (csv,rdf,slx) already in the output directory
    if the number is higher than <files_per_zip> or if <force> is True

    Args:
        csv_dir (string, mandatory): the output of the csv data directory
        rdf_dir (string, mandatory): the output of the rdf data directory
        slx_dir (string, mandatory): the output of the scholix data directory
        files_per_zip (int, mandatory): number if files per zip
        force (bool, optional): when true the compression is always done

    Returns:
        tuple: the full path of the zipped files
    """

    global _logger
    res = []
    data_formats = {
        "csv": csv_dir,
        "ttl": rdf_dir,
        "scholix": slx_dir
    }

    for _f, _subdir in data_formats.items():
        dir_path = os.path.join(_subdir)

        if os.path.isdir(dir_path):

            # files with non-zip extensions
            files = [f for f in os.listdir(dir_path)
                     if os.path.isfile(os.path.join(dir_path, f)) and f.endswith("_"+str(pnum)+"_1."+_f)]

            if (len(files) >= files_per_zip) or ( force and (len(files)>0) ):

                zip_files = [f for f in os.listdir(dir_path)
                         if os.path.isfile(os.path.join(dir_path, f)) and f.endswith("_"+str(pnum)+".zip")]

                zip_name = f"{len(zip_files)}_{pnum}.zip"
                zip_path = os.path.join(dir_path, zip_name)

                # Create zip
                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                    for filename in files:
                        file_path = os.path.join(dir_path, filename)
                        zipf.write(file_path, arcname=filename)
                        os.remove(file_path)  # Remove after adding
                res.append(zip_path)

    return tuple(res)


def chunk_list(lst, n):
    """Split list into n chunks as evenly as possible."""
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


def main():

    global _config
    global _logger
    global FILE_OUTPUT_DIR

    arg_parser = ArgumentParser(description="Dump OpenCitations Index data. This process reads all the data in Redis and creates a new data dump for the OpenCitations Index. The outputs are compressed, to all dump formats: CSV, RDF, SCHOLIX. **Make sure the Redis datasets are populated before running this script**")
    arg_parser.add_argument(
        "-d",
        "--date",
        required=False,
        help="The release date of the dump. Provide the date in format YYYYMMDD",
    )
    arg_parser.add_argument(
        "-w",
        "--workers",
        required=False,
        default=1,
        help="Maximum number of workers for parallel execution (default is set to 1, Recommended not higher than between 3 and 6)",
    )

    # Date of the dump
    dump_date = datetime.now().strftime("%Y%m%d") # format: YYYYMMDD
    args = arg_parser.parse_args()
    if args.date:
        dump_date = args.date


    _logger.info("Dumping all the citations in OpenCitations Index ...")

    # === CONF.INI ===
    # idbase_url = _config.get("INDEX", "idbaseurl")
    # baseurl = _config.get("INDEX", "baseurl")
    # agent = _config.get("INDEX", "agent")
    # source = _config.get("INDEX", "source")
    # service_name = _config.get("INDEX", "service")
    # index_identifier = _config.get("INDEX", "identifier")

    _logger.info(
        "--------- Configurations ----------\n"
        f"idbase_url: {idbase_url}\n"
        f"agent: {agent}\n"
        f"source: {source}\n"
        f"service: {service_name}\n"
        f"identifier: {index_identifier}"
    )

    # === CONFIGURATION ===
    # CITED_BATCH_SIZE = 1000
    # CITED_PER_FILE = 50000
    # FILES_PER_ZIP = 100
    WORKERS = int(args.workers)

    _logger.info(
        "--------- Process ----------\n"
        f"CITED_BATCH_SIZE: {CITED_BATCH_SIZE}\n"
        f"CITED_PER_FILE: {CITED_PER_FILE}\n"
        f"FILES_PER_ZIP: {FILES_PER_ZIP}\n"
        f"WORKERS: {WORKERS}\n"
    )

    # === REDIS ===
    REDIS_CITS_DB = _config.get("cnc", "db_cits")
    REDIS_METADATA_DB = _config.get("INDEX", "db")

    redis_cits = redis.Redis(host='localhost', port=6379, db=REDIS_CITS_DB, decode_responses=True)
    # Sample data of redis_cits:
    # "06304836421": "[\"06290442260\", \"0606973973\", \"06290442260\", \"061204315925\"]"

    redis_metadata = redis.Redis(host='localhost', port=6379, db=REDIS_METADATA_DB, decode_responses=True)
    # Sample data of redis_metadata:
    # "omid:br/061601556475": "{\"date\": \"2019\", \"valid\": true, \"orcid\": [\"0000-0002-6819-0387\"], \"issn\": [\"0886-022X\", \"1525-6049\"]}"

    _logger.info(
        "--------- Redis ----------\n"
        f"REDIS_CITS_DB: {REDIS_CITS_DB}\n"
        f"REDIS_METADATA_DB: {REDIS_METADATA_DB}\n"
    )

    # create the output directory
    FILE_OUTPUT_DIR = dump_date
    # init_fs(FILE_OUTPUT_DIR)
    _logger.info("Data will be stored in: "+FILE_OUTPUT_DIR)


    cursor = 0

    # iterate over all the citing entities
    while True:

        # index of entites to process
        # <citing_omid>: [<cited_omid_1>, <cited_omid_2>, <cited_omid_3> ... ]
        cits_pairs_to_process = []
        br_meta = {}

        # get from redis first CITED_BATCH_SIZE citing entites
        cursor, cited_keys = redis_cits.scan(cursor=cursor, count=CITED_BATCH_SIZE)
        if cited_keys:  # only fetch if we got keys

            pipe = redis_cits.pipeline()
            for key in cited_keys:
                pipe.smembers(key)

            citing_values = pipe.execute()

            for _a_cited, _val_citing in zip(cited_keys, citing_values):
                # to_process
                _a_cited = "omid:br/"+_a_cited
                _l_citing = ["omid:br/"+_a for _a in _val_citing]

                cits_pairs_to_process += [(_a_citing, _a_cited) for _a_citing in _l_citing]
                # get also the metadata of the BRs involved
                br_keys = _l_citing + [_a_cited]
                metadata_values = redis_metadata.mget(br_keys)
                br_meta.update( dict(zip(br_keys, metadata_values)) )

        # in case there are some entities to process iterate over all citation pairs
        if cits_pairs_to_process:

            chunks = chunk_list(cits_pairs_to_process, WORKERS)

            processes = []
            for idx,chunk in enumerate(chunks):
                p = Process(target=process_pair, args=(chunk, idx, br_meta, cursor == 0))
                p.start()
                processes.append(p)

            # Wait for all processes to finish
            for p in processes:
                p.join()

        # in case there are some entities to process iterate over all citation pairs
        # if cits_pairs_to_process:
        #
        #     chunks = chunk_list(cits_pairs_to_process, WORKERS)
        #
        #     results = []
        #     with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        #         futures = [
        #             executor.submit(process_pair, chunk, idx, br_meta, cursor == 0)
        #             for idx, chunk in enumerate(chunks)
        #         ]
        #
        #         # Option 1: process results as they complete
        #         # for f in as_completed(futures):
        #         #     results.append(f.result())
        #
        #         # Option 2: just wait and collect all at once
        #         results = [f.result() for f in futures]


        # when <cursor> is 0 then break, scan completed
        if cursor == 0:
            break


def process_pair(pairs, pnum, br_meta, end_cursor = False):

    global data_to_dump
    p_data_to_dump = data_to_dump[pnum]

    for pair in pairs:

        citing, cited = pair
        m_citing = br_meta.get(citing)
        m_cited = br_meta.get(cited)

        # in case one of two entites has no metadata move to next citation
        if not m_citing or not m_cited:
            continue

        # get the json obj
        m_citing = json.loads(m_citing)
        m_cited = json.loads(m_cited)

        oci_val = "oci:"+citing.replace("omid:br/","")+"-"+cited.replace("omid:br/","")
        p_data_to_dump.append(
            Citation(
                oci_val, # oci,
                idbase_url + quote(citing.replace("omid:","")), # citing_url,
                m_citing["date"], # citing_pub_date,
                idbase_url + quote(cited.replace("omid:","")), # cited_url,
                m_cited["date"], # cited_pub_date,
                None, # creation,
                None, # timespan,
                1, # prov_entity_number,
                agent, # prov_agent_url,
                source, # source,
                datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat(sep="T"), # prov_date,
                service_name, # service_name,
                index_identifier, # id_type,
                idbase_url + "([[XXX__decode]])", # id_shape,
                "reference", # citation_type,
                bool(set(m_citing["issn"]) & set(m_cited["issn"])), # journal_sc=False,
                bool(set(m_citing["orcid"]) & set(m_cited["orcid"])), # journal_sc=False,
                None, # prov_inv_date=None,
                "Creation of the citation", # prov_description=None,
                None, # prov_update=None,
            )
        )

    # write p_data_to_dump to files when range CITED_PER_FILE is reached
    if len(p_data_to_dump) >= CITED_PER_FILE or end_cursor:
        _logger.info(f"Storing {len(p_data_to_dump)} citations data of task {pnum}...")
        # write to files
        index_ts_storer = CitationStorer(
            FILE_OUTPUT_DIR,
            baseurl + "/" if not baseurl.endswith("/") else baseurl,
            store_as=["csv_data","rdf_data","scholix_data"],
            suffix= str(pnum)
        )
        BATCH_SAVE = 100000
        for idx in range(0, len(p_data_to_dump), BATCH_SAVE):
            batch_citations = p_data_to_dump[idx:idx+BATCH_SAVE]
            index_ts_storer.store_citation(batch_citations)
        # reset data_to_dump
        p_data_to_dump = []

        # check if the number of files already created should be zipped
        zip_and_cleanup(
            index_ts_storer.data_csv_dir,
            index_ts_storer.data_rdf_dir,
            index_ts_storer.data_slx_dir,
            FILES_PER_ZIP ,
            force = end_cursor,
            pnum = pnum
        )

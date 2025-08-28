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
import zipfile
from datetime import datetime
from argparse import ArgumentParser
# from tqdm import tqdm

# Libraries needed
from oc.index.oci.citation import Citation, OCIManager
from oc.index.utils.config import get_config
from oc.index.utils.logging import get_logger
from oc.index.oci.storer import CitationStorer

# Globals
_config = get_config()
_logger = get_logger()


def zip_and_cleanup(base_dir, files_per_zip, force = False):
    """It compresses (zip) the oc index data files (csv,rdf,slx) already in the output directory
    if the number is higher than <files_per_zip> or if <force> is True

    Args:
        base_dir (string, mandatory): the output base directory
        files_per_zip (int, mandatory): number if files per zip
        force (bool, optional): when true the compression is always done

    Returns:
        tuple: the full path of the zipped files
    """

    global _logger
    res = []
    data_formats = {
        "csv": "data/csv",
        "ttl": "data/rdf",
        "scholix":"data/slx"
    }

    for _f, _subdir in data_formats.items():
        dir_path = os.path.join(base_dir, _subdir)

        # files with non-zip extensions
        files = [f for f in os.listdir(dir_path)
                 if os.path.isfile(os.path.join(dir_path, f)) and f.endswith("."+_f)]

        if (len(files) >= files_per_zip) or ( force and (len(files)>0) ):

            zip_files = [f for f in os.listdir(dir_path)
                     if os.path.isfile(os.path.join(dir_path, f)) and f.endswith(".zip")]

            zip_name = f"{len(zip_files)}.zip"
            zip_path = os.path.join(dir_path, zip_name)

            # Create zip
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                for _, filename in files:
                    file_path = os.path.join(dir_path, filename)
                    zipf.write(file_path, arcname=filename)
                    os.remove(file_path)  # Remove after adding
            res.append(zip_path)

        return tuple(res)


def main():

    global _config
    global _logger

    arg_parser = ArgumentParser(description="Dump OpenCitations Index data. This process reads all the data in Redis and creates a new data dump for the OpenCitations Index. The outputs are compressed, to all dump formats: CSV, RDF, SCHOLIX. **Make sure the Redis datasets are populated before running this script**")
    arg_parser.add_argument(
        "-d",
        "--date",
        required=False,
        help="The release date of the dump. Provide the date in format YYYYMMDD",
    )

    # Date of the dump
    dump_date = datetime.now().strftime("%Y%m%d") # format: YYYYMMDD
    args = arg_parser.parse_args()
    if args.date:
        dump_date = args.date


    _logger.info("Dumping all the citations in OpenCitations Index ...")

    # === CONF.INI ===
    idbase_url = _config.get("INDEX", "idbaseurl")
    baseurl = _config.get("INDEX", "baseurl")
    agent = _config.get("INDEX", "agent")
    source = _config.get("INDEX", "source")
    service_name = _config.get("INDEX", "service")
    index_identifier = _config.get("INDEX", "identifier")

    _logger.info(
        "--------- Configurations ----------\n"
        f"idbase_url: {idbase_url}\n"
        f"agent: {agent}\n"
        f"source: {source}\n"
        f"service: {service_name}\n"
        f"identifier: {index_identifier}"
    )

    # === CONFIGURATION ===
    CITING_BATCH_SIZE = 1000
    CITING_PER_FILE = 50000
    FILES_PER_ZIP = 100

    _logger.info(
        "--------- Process ----------\n"
        f"CITING_BATCH_SIZE: {CITING_BATCH_SIZE}\n"
        f"CITING_PER_FILE: {CITING_PER_FILE}\n"
        f"FILES_PER_ZIP: {FILES_PER_ZIP}\n"
    )

    # === REDIS ===
    REDIS_CITS_DB = _config.get("cnc", "db_cits")
    REDIS_METADATA_DB = _config.get("INDEX", "db_br")
    redis_cits = redis.Redis(host='localhost', port=6379, db=REDIS_CITS_DB, decode_responses=True)
    # Sample data of redis_metadata:
    # "0604212254": "{\"date\": \"1967\", \"valid\": true, \"orcid\": [], \"issn\": [\"0371-1838\", \"2433-2895\"]}"
    redis_metadata = redis.Redis(host='localhost', port=6379, db=REDIS_METADATA_DB, decode_responses=True)

    _logger.info(
        "--------- Redis ----------\n"
        f"REDIS_CITS_DB: {REDIS_CITS_DB}\n"
        f"REDIS_METADATA_DB: {REDIS_METADATA_DB}\n"
    )

    # create the output directory
    FILE_OUTPUT_DIR = 'oc_index_dump_'+dump_date
    # init_fs(FILE_OUTPUT_DIR)
    _logger.info("Data will be stored in: "+FILE_OUTPUT_DIR)


    cursor = 0
    file_id = 1
    data_to_dump = []

    # iterate over all the citing entities
    while True:

        # index of entites to process
        # <citing_omid>: [<cited_omid_1>, <cited_omid_2>, <cited_omid_3> ... ]
        cits_pairs_to_process = []
        br_meta = {}

        # get from redis first CITING_BATCH_SIZE citing entites
        cursor, citing_keys = redis_cits.scan(cursor=cursor, count=CITING_BATCH_SIZE)
        if citing_keys:  # only fetch if we got keys
            cited_values = redis_cits.mget(citing_keys)
            for _a_citing, _val_cited in zip(citing_keys, cited_values):
                # to_process
                _l_cited = eval( _val_cited.decode("utf-8") )
                cits_pairs_to_process += [(_a_citing, _a_cited) for _a_cited in _l_cited]
                # get also the metadata of the BRs involved
                br_keys = _l_cited.append(_a_citing)
                metadata_values = redis_metadata.mget(br_keys)
                br_meta.update( dict(zip(br_keys, metadata_values)) )

        # in case there are some entities to process iterate over all citation pairs
        if len(cits_pairs_to_process) > 0:
            for citing, cited in cits_pairs_to_process:
                m_citing = br_meta.get( citing )
                m_cited = br_meta.get( br_meta[cited] )

                # in case one of two entites has no metadata move to next citation
                if not m_citing or not m_cited:
                    continue

                data_to_dump.append(
                    Citation(
                        "oci:"+citing+"-"+cited, # oci,
                        idbase_url + quote("br/"+citing), # citing_url,
                        m_citing["pub_date"], # citing_pub_date,
                        idbase_url + quote("br/"+cited), # cited_url,
                        m_cited["pub_date"], # cited_pub_date,
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

        # write data_to_dump to files when range CITING_PER_FILE is reached
        if len(data_to_dump) >= CITING_PER_FILE :
            _logger.info(f"Storing {len(data_to_dump)} citations data ...")
            # write to files
            index_ts_storer = CitationStorer(
                FILE_OUTPUT_DIR,
                baseurl + "/" if not baseurl.endswith("/") else baseurl,
                store_as=["csv_data","rdf_data","scholix_data"]
            )
            BATCH_SAVE = 100000
            for idx in range(0, len(data_to_dump), BATCH_SAVE):
                batch_citations = data_to_dump[idx:idx+BATCH_SAVE]
                index_ts_storer.store_citation(batch_citations)
            # reset data_to_dump
            data_to_dump = []

        # check if the number of files already created should be zipped
        zip_and_cleanup(FILE_OUTPUT_DIR, FILES_PER_ZIP , force = cursor == 0)

        # when <cursor> is 0 then break, scan completed
        if cursor == 0:
            break

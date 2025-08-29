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
import tarfile
import json
import io

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime, timezone
from collections import defaultdict

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config
from oc.index.oci.citation import Citation
from oc.index.oci.storer import CitationStorer
from oc.index.glob.redis import RedisDataSource

_config = get_config()
_logger = get_logger()


def save_to_disk(output_dir, baseurl, entities_with_no_omid,  index_citations, pnum = 0):

    # Store entities_with_no_omid
    _logger.info("Saving entities with no omid...")
    with open(output_dir+'entities_with_no_omid.csv', 'a+') as f:
        write = csv.writer(f)
        write.writerows([[e] for e in entities_with_no_omid])

    BATCH_SAVE = 100000
    index_ts_storer = CitationStorer(
        output_dir+"/index-dump",
        baseurl + "/" if not baseurl.endswith("/") else baseurl,
        store_as= ["csv_prov","rdf_data","rdf_prov"],
        suffix= str(pnum)
    )
    _logger.info("Saving Index citations to dump...")
    for idx in range(0, len(index_citations), BATCH_SAVE):
        batch_citations = index_citations[idx:idx+BATCH_SAVE]
        index_ts_storer.store_citation(batch_citations)
    _logger.info(f"{len(index_citations)} citations saved")


def mark_as_processed(fname_processed, csv_name):
    # Saving CSV processed file in the index
    with open(fname_processed, 'a+') as f:
        write = csv.writer(f)
        write.writerow([str(csv_name)])


def rewrite_entities_with_no_omid(output_dir):
    # remove duplicates from entities_with_no_omid
    if os.path.exists(output_dir+'entities_with_no_omid.csv'):
        index_entities = set()
        with open(output_dir+'entities_with_no_omid.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                index_entities.add(row[0])
        with open(output_dir+'entities_with_no_omid.csv', 'w') as f_out:
            csv.writer(f_out).writerows([[e] for e in index_entities])


def proc_cits(l_cits, redis_br, agent, source, service_name, index_identifier, idbase_url):
    """
    Process a list of citations to create the corresponding RDF data
    """

    global _logger

    index_citations = []
    citations_duplicated = 0
    entities_with_no_omid = set()
    cits_buffer = []

    # iterate over all the citation list
    for idx, cit in tqdm(enumerate(l_cits)):
        # add the citing and cited entities to be further retrivied from redis
        cits_buffer.append(list(cit))
        # Process when the buffer is full or I have reached the last element of the list
        if len(cits_buffer) >= REDIS_R_BUFFER_CITS or idx == len(l_cits) - 1:

            # ==== (1) GET OMIDs of all the BRs involved ====
            index_ocis = dict()
            br_anyids = [x for c in cits_buffer for x in c]
            br_omids = {key: value for key, value in zip(br_anyids, redis_br.mget(br_anyids))}

            # iterate by couples – each couple is a list
            for cit in cits_buffer:

                citing_id, val_citing_omid = cit[0], br_omids[cit[0]]
                cited_id, val_cited_omid = cit[1], br_omids[cit[1]]

                for omid, eid in [(val_citing_omid, citing_id), (val_cited_omid, cited_id)]:
                    if omid is None:
                        entities_with_no_omid.add(eid)

                # check if citing or cited entities have an OMID
                if val_citing_omid == None or val_cited_omid == None:
                    continue

                l_citing_omid = val_citing_omid.decode("utf-8").split("; ")
                l_cited_omid = val_cited_omid.decode("utf-8").split("; ")

                # since an ANYID miught have multiple OMIDs, we need to get all of them and iterate over all pairs
                cit_pairs = [(x, y) for x in l_citing_omid for y in l_cited_omid]
                for citing_omid, cited_omid in cit_pairs:
                    oci_omid = citing_omid.replace("omid:br/","")+"-"+cited_omid.replace("omid:br/","")
                    index_ocis[oci_omid] = (citing_omid,cited_omid)


            # ==== (2) Keep citations that are not in cache (have not been processed before)
            ocis_to_process = dict()
            for _oci, in_redis in zip(index_ocis.keys(), redis_cits_cache.mget(index_ocis.keys())):
                # check if it has not been already processed before – in Redis cache
                if in_redis == None:
                    ocis_to_process[_oci] = index_ocis[_oci]
                else:
                    citations_duplicated += 1


            # ==== (3) Process citations that are not in cache
            for oci_omid, (citing_omid, cited_omid) in ocis_to_process.items():

                try:
                    index_citations.append(
                        Citation(
                            "oci:"+oci_omid, # oci,
                            idbase_url + quote(citing_omid.replace("omid:","")), # citing_url,
                            None, # citing_pub_date,
                            idbase_url + quote(cited_omid.replace("omid:","")), # cited_url,
                            None, # cited_pub_date,
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
                            None, # journal_sc=False,
                            None,# author_sc=False,
                            None, # prov_inv_date=None,
                            "Creation of the citation", # prov_description=None,
                            None, # prov_update=None,
                        )
                    )
                except Exception as e:
                    _logger.info(f"An error to oci= {oci_omid} occurred: {e}")

                # Reset buffer and write in cache
                cits_buffer = []
                redis_cits_cache.mset( {_k:1 for _k in ocis_to_process.keys()} )

    _logger.info("[STATS] #Duplicated citations= "+str(citations_duplicated))
    _logger.info("[STATS] #BRs without OMID= "+str(len(entities_with_no_omid)))
    _logger.info("[STATS] #Citations lost (not processed)="+str(len(l_cits) - len(index_citations)))

    return entities_with_no_omid, index_citations


def cnc(service, input_files, intype, output_dir):
    """
    Creates RDF data for the new citations ready to be ingested in OpenCitations Index – OMID to OMID citations

    Args:
        service (string, mandatory): name if the source: "COCI","DOCI", etc.
        input_files (list, mandatory): a list of Zipped files contatining CSVs storing the citations of the source
        intype (string, mandatory): the type of the expected files in the input_files list
        output_dir (string, mandatory): path to the output directory

    Returns:
        tuple: the full path of the zipped files
    """
    global _config
    global _logger

    # === CONF.INI ===
    idbase_url = _config.get("INDEX", "idbaseurl")
    index_identifier = _config.get("INDEX", "identifier")
    agent = _config.get("INDEX", "agent")
    service_name = _config.get("INDEX", "service")
    baseurl = _config.get("INDEX", "baseurl")
    source = _config.get(service, "source")
    _logger.info(
        "--------- Configurations ----------\n"
        f"idbase_url: {idbase_url}\n"
        f"agent: {agent}\n"
        f"source: {source}\n"
        f"service: {service_name}\n"
        f"identifier: {index_identifier}"
    )

    # === CONFIGURATION ===

    # COCI Datasource Converter – provides citations in CSV as:
    # "citing","cited"
    # "doi:10.4000/geocarrefour.7195","doi:10.3917/her.123.0088"
    # "doi:10.4000/geocarrefour.7195","doi:10.1177/0002716207311877"
    # ...

    identifier = ""
    citing_col = "citing"
    cited_col = "cited"

    # === REDIS ===
    # Redis BRs mapping: data sample to get from redis: "doi:10.1080/0886022x.2019.1635892": "omid:br/061601556467; omid:br/061601556468"
    redis_br = redis.Redis( host="127.0.0.1", port="6379", db=_config.get("cnc", "db_br") )
    # Redis processing cache: data sample to set in redis: <OCI>:1
    redis_cits_cache = redis.Redis( host="127.0.0.1", port="6379", db=_config.get("cnc", "db_omid") )
    # Redis DB of <OMID>:<METADATA>
    redis_index = RedisDataSource("INDEX")
    # cache variables
    REDIS_W_BUFFER = 300000
    REDIS_R_BUFFER_CITS = 100000


    # === PROCESS ===
    entities_with_no_omid = None
    index_citations = None

    # Index of the CSV file processed (in case of script re-running)
    FNAME_PROCESSED_FILES = output_dir+'files_processed.csv'
    processed_files = set()
    if os.path.exists(FNAME_PROCESSED_FILES):
        with open(FNAME_PROCESSED_FILES, 'r') as file:
            for row in csv.reader(file):
                processed_files.add(row[0])

    # Check the type of the input given and process it as a function of that
    for _f in input_files:

        if intype=="ZIP" and _f.endswith(".zip"):
            # Handle single ZIP file
            with ZipFile(_f) as archive:
                logger.info(f"ZIP: Total number of files in {os.path.basename(_f)}: {len(archive.namelist())}")
                for csv_name in archive.namelist():
                    if csv_name.endswith('.csv'):
                        if csv_name in processed_files:
                            _logger.info("Already processed, skip file: "+str(csv_name))
                        else:
                            # ... PROCESS the CSV file
                            _logger.info("Processing the "+service+" citations inside: "+str(csv_name))
                            with archive.open(csv_name) as csv_file:
                                l_cits = [(identifier+row[citing_col],identifier+row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                                entities_with_no_omid,index_citations = proc_cits(l_cits, redis_br, agent, source, service_name, index_identifier, idbase_url)
                            save_to_disk(output_dir, baseurl, entities_with_no_omid,  index_citations, pnum = 0)
                            mark_as_processed(FNAME_PROCESSED_FILES, csv_name)


        elif intype=="TARGZ" and (_f.endswith(".tar.gz") or _f.endswith(".tgz")):
            # Handle single TAR.GZ file
            with tarfile.open(_f, 'r:gz') as archive:
                logger.info(f"TAR.GZ: Total number of files in {os.path.basename(_f)}: {len(archive.getnames())}")
                for csv_name in archive.getnames():
                    if csv_name.endswith('.csv'):
                        if csv_name in processed_files:
                            _logger.info("Already processed, skip file: "+str(csv_name))
                        else:
                            # ... PROCESS the CSV file
                            _logger.info("Processing the "+service+" citations inside: "+str(csv_name))
                            csv_file = archive.extractfile(csv_name)
                            if csv_file:
                                l_cits = [(identifier+row[citing_col],identifier+row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                                entities_with_no_omid,index_citations = proc_cits(l_cits, redis_br, agent, source, service_name, index_identifier, idbase_url)
                            save_to_disk(output_dir, baseurl, entities_with_no_omid,  index_citations, pnum = 0)
                            mark_as_processed(FNAME_PROCESSED_FILES, csv_name)


        elif intype=="CSV" and _f.endswith(".csv"):
            # Handle single CSV file
            csv_name = os.path.basename(_f)
            logger.info(f"CSV: Processing direct CSV file: {csv_name}")
            if csv_name in processed_files:
                _logger.info("Already processed, skip file: "+str(csv_name))
            else:
                # ... PROCESS the CSV file
                _logger.info("Processing the "+service+" citations inside: "+str(csv_name))
                with open(_f, 'r', encoding='utf-8') as csv_file:
                    l_cits = [(identifier+row[citing_col],identifier+row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                    entities_with_no_omid,index_citations = proc_cits(l_cits, redis_br, agent, source, service_name, index_identifier, idbase_url)
                save_to_disk(output_dir, baseurl, entities_with_no_omid,  index_citations, pnum = 0)
                mark_as_processed(FNAME_PROCESSED_FILES, csv_name)
        else:
            logger.warning(f"Unsupported file type: {_f}")

    # remove duplicates from entities_with_no_omid
    rewrite_entities_with_no_omid()


def main():

    arg_parser = ArgumentParser(description="Create new citations of OC INDEX. This scripts converts citations ANYID-ANYID comming form different data sources (e.g., COCI, DOCI) into OMID-OMID citations. It produces RDF data and provenance (RDF,CSV)")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory contatining compressed file(s) (ZIP format) having all the CSV file(s) of the datasource citations",
    )
    arg_parser.add_argument(
        "-t",
        "--intype",
        default="CSV",
        help="The type of the files in the input directory, e.g. CSV, ZIP",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        default="COCI",
        help="The datasource of the dump to process (e.g. COCI, DOCI)",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The destination directory to save outputs",
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

    # type of the input files
    intype = args.intype.strip().upper()

    # output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Call CNC on all the files inside the input directory
    # <intype> specifies what is the expected format of such files: CSV, ZIP, TAR .. etc
    cnc(
        service,
        input_files,
        intype,
        output_dir
    )

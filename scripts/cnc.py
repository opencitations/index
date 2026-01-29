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
from multiprocessing import Process
import multiprocessing
from math import ceil
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

# === CONF.INI ===
collection_name = "INDEX"
idbase_url = _config.get(collection_name, "idbaseurl")
index_identifier = _config.get(collection_name, "identifier")
agent = _config.get(collection_name, "agent")
service_name = _config.get(collection_name, "service")
baseurl = _config.get(collection_name, "baseurl")
source = _config.get(collection_name, "source")
_logger.info(
    "--------- Configurations ----------\n"
    f"idbase_url: {idbase_url}\n"
    f"agent: {agent}\n"
    f"source: {source}\n"
    f"service: {service_name}\n"
    f"identifier: {index_identifier}"
)
BATCH_SAVE = 100000

# === REDIS ===
# Redis BRs mapping: data sample to get from redis: "doi:10.1080/0886022x.2019.1635892": "omid:br/061601556467; omid:br/061601556468"
redis_br = redis.Redis( host="127.0.0.1", port="6379", db=_config.get("cnc", "db_br") )
# Redis processing cache: data sample to set in redis: <OCI>:1
redis_cits_cache = redis.Redis( host="127.0.0.1", port="6379", db=_config.get("cnc", "db_omid") )
# Redis cache variables
REDIS_W_BUFFER = 300000
REDIS_R_BUFFER_CITS = 100000


def save_data(output_dir, cits_obj, pid = 0):
    # define the storer
    index_ts_storer = CitationStorer(
        output_dir+"/ocindex-data",
        baseurl + "/" if not baseurl.endswith("/") else baseurl,
        store_as= ["csv_prov","rdf_data","rdf_prov"],
        suffix= str(pid)
    )
    # store the citations moving by the size of BATCH_SAVE
    for idx in range(0, len(cits_obj), BATCH_SAVE):
        batch_citations = cits_obj[idx:idx+BATCH_SAVE]
        index_ts_storer.store_citation(batch_citations)


def gen_cits(cits, pid = 0):
    """
    Generate the citaions (class Citation)
    Args:
        cits (dict, mandatory): <oci>:(<citing>,<cited>)
    """
    global _logger

    # ==== Keep citations that are not in cache (have not been processed before)
    # and count the duplicated citations
    ocis_to_process = dict()
    cits_to_process = []
    citations_duplicated = 0
    res_citations = []

    for _oci, in_redis in zip(cits.keys(), redis_cits_cache.mget(cits.keys())):
        # check if it has not been already processed before – in Redis cache
        if in_redis == None:
            cits_to_process[_oci] = cits[_oci]
        else:
            citations_duplicated += 1

    # ==== Process citations that are not in cache
    for oci_omid, (citing_omid, cited_omid) in ocis_to_process.items():

        try:
            res_citations.append(
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

    # write in cache
    redis_cits_cache.mset( {_k:1 for _k in ocis_to_process.keys()} )
    _logger.info("[STATS] #Generated citations= "+str(len(res_citations)))
    _logger.info("[STATS] #Duplicated citations= "+str(citations_duplicated))

    return res_citations


def set_cits(l_cits, pid = 0):
    """
    Set in Redis a list of citations

    Args:
        l_cits (list, mandatory): list of tuples, each tuple has a citing and cited entity
    """
    global _logger

    citations_duplicated = 0
    # entities_with_no_omid = set()
    cits_buffer = []
    res_cits = dict()

    # iterate over all the citation list
    for idx, cit in tqdm(enumerate(l_cits)):
        # add the citing and cited entities to be further retrivied from redis
        cits_buffer.append(cit)
        # Process when the buffer is full or I have reached the last element of the list
        if len(cits_buffer) >= REDIS_R_BUFFER_CITS or idx == len(l_cits) - 1:

            # ==== (1) GET OMIDs of all the BRs involved ====
            br_anyids = [x for c in cits_buffer for x in c]
            br_omids = {key: value for key, value in zip(br_anyids, redis_br.mget(br_anyids))}

            # iterate by couples – each couple is a list
            for cit in cits_buffer:

                citing_id, val_citing_omid = cit[0], br_omids[cit[0]]
                cited_id, val_cited_omid = cit[1], br_omids[cit[1]]

                # for omid, eid in [(val_citing_omid, citing_id), (val_cited_omid, cited_id)]:
                #     if omid is None:
                #         entities_with_no_omid.add(eid)

                # check if citing or cited entities have an OMID
                if val_citing_omid == None or val_cited_omid == None:
                    continue

                l_citing_omid = val_citing_omid.decode("utf-8").split("; ")
                l_cited_omid = val_cited_omid.decode("utf-8").split("; ")

                # since an ANYID miught have multiple OMIDs, we need to get all of them and iterate over all pairs
                cit_pairs = [(x, y) for x in l_citing_omid for y in l_cited_omid]
                for citing_omid, cited_omid in cit_pairs:
                    oci_omid = citing_omid.replace("omid:br/","")+"-"+cited_omid.replace("omid:br/","")
                    res_cits[oci_omid] = (citing_omid,cited_omid)

            # Reset buffer and write in cache
            cits_buffer = []

    # return a list of citations in omid
    # each citation is a tuple: (<OCI-VAL>,<CITING-OMID>,<CITED-OMID>,)
    return res_cits


def cnc(collection, input_files, intype, output_dir, pid = 0):
    """
    Creates RDF data for the new citations ready to be ingested in OpenCitations Index – OMID to OMID citations
    The process creates also Provenance data in RDF and CSV.

    Args:
        collection (string, mandatory): name if the source collection in OpenCitations: "COCI","DOCI", etc.
        input_files (list, mandatory): a list of files (CSVs, ZIPs, or TARGZ) contatining storing the citations of the source
        intype (string, mandatory): the type/format of the expected files in the input_files list
        output_dir (string, mandatory): path to the output directory
        pid: the id of the running process

    Returns:
        tuple: the full path of the zipped files
    """
    global _config
    global _logger

    # === CONFIGURATION ===
    # All the citations produced by DS-Converter already have the ID Prefix
    # so there is no need to append any identifier before such value
    # E.G.
    # COCI Datasource Converter – provides citations in CSV as:
    # > "citing","cited"
    # > "doi:10.4000/geocarrefour.7195","doi:10.3917/her.123.0088"
    # > "doi:10.4000/geocarrefour.7195","doi:10.1177/0002716207311877"
    # > ...
    citing_col = "citing"
    cited_col = "cited"

    # === PROCESS ===
    entities_with_no_omid = None
    index_citations = None

    # Check the type of the input given and process it as a function of that
    for _f in input_files:

        if intype=="ZIP" and _f.endswith(".zip"):
            # Handle single ZIP file
            with ZipFile(_f) as archive:
                _logger.info(f"ZIP: Total number of files in {os.path.basename(_f)}: {len(archive.namelist())}")
                for csv_name in archive.namelist():
                    if csv_name.endswith('.csv'):
                        if csv_name in processed_files:
                            _logger.info("Already processed, skip file: "+str(csv_name))
                        else:
                            # ... PROCESS the CSV file
                            _logger.info("Processing the "+collection+" citations inside: "+str(csv_name))
                            with archive.open(csv_name) as csv_file:
                                cits_in_file = [(row[citing_col],row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                                ocindex_cits = set_cits(
                                    cits_in_file,
                                    pid
                                )
                                cits_objs = gen_cits(ocindex_cits, pid)
                                save_data(output_dir, cits_objs, pid)



        elif intype=="TARGZ" and (_f.endswith(".tar.gz") or _f.endswith(".tgz")):
            # Handle single TAR.GZ file
            with tarfile.open(_f, 'r:gz') as archive:
                _logger.info(f"TAR.GZ: Total number of files in {os.path.basename(_f)}: {len(archive.getnames())}")
                for csv_name in archive.getnames():
                    if csv_name.endswith('.csv'):
                        if csv_name in processed_files:
                            _logger.info("Already processed, skip file: "+str(csv_name))
                        else:
                            # ... PROCESS the CSV file
                            _logger.info("Processing the "+collection+" citations inside: "+str(csv_name))
                            csv_file = archive.extractfile(csv_name)
                            if csv_file:
                                cits_in_file = [(row[citing_col],row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                                ocindex_cits = set_cits(
                                    cits_in_file,
                                    pid
                                )
                                cits_objs = gen_cits(ocindex_cits, pid)
                                save_data(output_dir, cits_objs, pid)



        elif intype=="CSV" and _f.endswith(".csv"):
            # Handle single CSV file
            csv_name = os.path.basename(_f)
            _logger.info(f"CSV: Processing direct CSV file: {csv_name}")
            if csv_name in processed_files:
                _logger.info("Already processed, skip file: "+str(csv_name))
            else:
                # ... PROCESS the CSV file
                _logger.info("Processing the "+collection+" citations inside: "+str(csv_name))
                with open(_f, 'r', encoding='utf-8') as csv_file:
                    cits_in_file = [(row[citing_col],row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]
                    ocindex_cits = set_cits(
                        cits_in_file,
                        pid
                    )
                    cits_objs = gen_cits(ocindex_cits, pid)
                    save_data(output_dir, cits_objs, pid)

        else:
            _logger.warning(f"Unsupported file type: {_f}")

    # remove duplicates from entities_with_no_omid
    rewrite_entities_with_no_omid()



def chunk_list(lst, n):
    """
    Split list into n chunks as evenly as possible.

    Args:
        lst (list, mandatory): name if the source: "COCI","DOCI", etc.
        n (string, mandatory): path to the output directory

    Returns:
        list(list): different chunks
    """
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]


def main():
    arg_parser = ArgumentParser(description="Create new citations of OC INDEX. This scripts converts citations ANYID-ANYID comming form different data sources (e.g., COCI, DOCI) into OMID-OMID citations. It produces RDF data and provenance (RDF,CSV)")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory contatining the datasource citations",
    )
    arg_parser.add_argument(
        "-t",
        "--intype",
        required=True,
        help="The format of the files in the input directory, i.e. CSV, ZIP or TARGZ",
    )
    arg_parser.add_argument(
        "-c",
        "--collection",
        required=True,
        help="The opencitation collection of the datasource (e.g. COCI, DOCI)",
    )
    arg_parser.add_argument(
        "-s",
        "--source",
        required=False,
        help="The datasource data provenance, if not specified it is taken from CONFIG file, e.g. https://api.crossref.org/snapshots/monthly/2023/09/all.json.tar.gz",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The destination directory to save outputs",
    )
    arg_parser.add_argument(
        "-p",
        "--processes",
        type=int,
        default=1,
        help="Number of parallel processes to use (default: is set to 1)",
    )
    args = arg_parser.parse_args()


    # input directory/file
    input_files = []
    if os.path.isdir(args.input):
        input = args.input + "/" if args.input[-1] != "/" else args.input
        for filename in os.listdir(input):
            input_files.append(os.path.join(input, filename))
    else:
        input_files.append(args.input)

    # type of the input files
    # E.G. CSV, ZIP or TARGZ
    intype = args.intype.strip().upper()

    # The corresponding datasource collection in OpenCitations
    # E.G. COCI, DOCI etc
    collection = args.collection.strip().upper()

    # The corresponding datasource collection in OpenCitations
    # E.G. COCI, DOCI etc
    source = args.source.strip()

    # The output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    #  ==== Run CNC in  parallel ====
    if args.processes > 1 and len(input_files) > 1:
        num_processes = min(args.processes, len(input_files))
        chunks = chunk_list(input_files, num_processes)

        processes = []
        for idx,chunk in enumerate(chunks):
            p = Process(target=cnc, args=(collection, chunk, intype, output_dir, idx))
            p.start()
            processes.append(p)

        # Wait for all processes to finish
        for p in processes:
            p.join()
    else:
        # fallback: single process
        cnc(collection, input_files, intype, output_dir, 0)

    # 4. Continue with the rest of your code **after all files are done**
    # e.g., merging outputs, generating RDF/CSV summary, logging, etc.
    # >> post_processing(output_dir)

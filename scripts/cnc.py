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
import json
import io

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime, timezone
from collections import defaultdict

from oc.index.parsing.base import CitationParser
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config
from oc.index.finder.base import ResourceFinderHandler
from oc.index.finder.base import OMIDResourceFinder
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.oci.citation import Citation, OCIManager
from oc.index.oci.storer import CitationStorer
from oc.index.glob.redis import RedisDataSource
from oc.index.glob.csv import CSVDataSource

_config = get_config()

def normalize_dump(service, input_files, output_dir, newdump = False):
    global _config
    logger = get_logger()

    # get the service values from the CONFIG.INI
    idbase_url = _config.get("INDEX", "idbaseurl")
    index_identifier = _config.get("INDEX", "identifier")
    agent = _config.get("INDEX", "agent")
    service_name = _config.get("INDEX", "service")
    baseurl = _config.get("INDEX", "baseurl")

    # service variables
    identifier = ""
    source = _config.get(service, "source")
    citing_col = "citing"
    cited_col = "cited"
    if not newdump:
        identifier = _config.get(service, "identifier") + ":"
        source = _config.get(service, "ocdump")
        citing_col = "citing"
        cited_col = "cited"

    # redis DB of <ANYID>:<OMID>
    redis_br = redis.Redis(
        host="127.0.0.1",
        port="6379",
        db=_config.get("cnc", "db_br")
    )

    # redis DB of <OCI>:1
    redis_cits = redis.Redis(
        host="127.0.0.1",
        port="6379",
        db=_config.get("cnc", "db_cits")
    )

    # redis DB of <OMID>:<METADATA>
    redis_index = RedisDataSource("INDEX")

    #cache variables
    REDIS_W_BUFFER = 300000
    REDIS_R_BUFFER_CITS = 100000

    # file already processed
    processed_files = set()
    if os.path.exists(output_dir+'files_processed.csv'):
        with open(output_dir+'files_processed.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                processed_files.add(row[1])

    for fzip in input_files:
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                logger.info("Working on the archive:"+str(fzip))
                logger.info("Total number of files in archive is:"+str(len(archive.namelist())))

                # CSV header: oci,citing,cited,creation,timespan,journal_sc,author_sc
                for csv_name in archive.namelist():

                    if csv_name in processed_files:
                        logger.info("Already processed, skip file: "+str(csv_name))
                        continue

                    if not csv_name.endswith(".csv"):
                        logger.info("Not a CSV, skip file: "+str(csv_name))
                        continue

                    index_citations = []
                    citations_duplicated = 0
                    entities_with_no_omid = set()
                    service_citations = []
                    cits_buffer = []
                    ocis_processed_buffer = dict()

                    logger.info("Converting the citations in: "+str(csv_name))
                    with archive.open(csv_name) as csv_file:

                        l_cits = [(identifier+row[citing_col],identifier+row[cited_col]) for row in list(csv.DictReader(io.TextIOWrapper(csv_file)))]

                        logger.info("The #citations is: "+str(len(l_cits)))

                        # iterate citations (CSV rows)
                        for idx, cit in tqdm(enumerate(l_cits)):

                            # add the citing and cited entities to be further retrivied from redis
                            cits_buffer.append(list(cit))


                            # Process when the buffer is full or I have reached the last element
                            if len(cits_buffer) >= REDIS_R_BUFFER_CITS or idx == len(l_cits) - 1:

                                # (1) GET ALL OMIDS
                                index_ocis = dict()
                                keys_br_ids = []
                                for c in cits_buffer:
                                    keys_br_ids += c

                                br_omids = {key: value for key, value in zip(keys_br_ids, redis_br.mget(keys_br_ids))}

                                # iterate by couples
                                for buffer_cit in cits_buffer:

                                    citing_id, citing_omid = buffer_cit[0], br_omids[buffer_cit[0]]
                                    cited_id, cited_omid = buffer_cit[1], br_omids[buffer_cit[1]]

                                    # check if both citing and cited entities have omid
                                    if citing_omid != None and cited_omid != None:

                                        citing_omid = citing_omid.decode("utf-8")
                                        cited_omid = cited_omid.decode("utf-8")
                                        oci_omid = citing_omid[3:]+"-"+cited_omid[3:]

                                        index_ocis[oci_omid] = (citing_omid,cited_omid)

                                        service_citations.append(
                                            Citation(
                                                "oci:"+oci_omid, # oci,
                                                None, # citing_url,
                                                None, # citing_pub_date,
                                                None, # cited_url,
                                                None, # cited_pub_date,
                                                None, # creation,
                                                None, # timespan,
                                                None, # prov_entity_number,
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

                                    else:
                                        if citing_omid == None:
                                            entities_with_no_omid.add(citing_id)
                                        if cited_omid == None:
                                            entities_with_no_omid.add(cited_id)


                                # (2) GET ALL OMID-OCIs not processed yet
                                ocis_to_process = dict()
                                brs_to_process = []
                                for oci_omid, in_redis in zip(index_ocis.keys(), redis_cits.mget(index_ocis.keys())):

                                    # it has been already processed
                                    if in_redis != None:
                                        citations_duplicated += 1
                                    else:
                                        #check if is not in the cache too
                                        if oci_omid not in ocis_processed_buffer:
                                            brs_to_process.append(index_ocis[oci_omid][0])
                                            brs_to_process.append(index_ocis[oci_omid][1])
                                            ocis_to_process[oci_omid] = [index_ocis[oci_omid][0], index_ocis[oci_omid][1]]


                                # (3) GET ALL METADATA
                                # Create a dict which maps the omid_brs to their metadata
                                # br_meta = {key: value for key, value in zip(brs_to_process, redis_br.mget(brs_to_process))}
                                resources = redis_index.mget(brs_to_process)
                                rf_handler = ResourceFinderHandler([OMIDResourceFinder(resources)])
                                for oci_omid in ocis_to_process:

                                    citing_omid, cited_omid = ocis_to_process[oci_omid][0], ocis_to_process[oci_omid][1]

                                    citing_date = rf_handler.get_date(citing_omid)
                                    cited_date = rf_handler.get_date(cited_omid)
                                    journal_sc, citing_issn, cited_issn = rf_handler.share_issn(citing_omid, cited_omid)
                                    author_sc, citing_orcid, cited_orcid = rf_handler.share_orcid(citing_omid, cited_omid)

                                    index_citations.append(
                                        Citation(
                                            "oci:"+oci_omid, # oci,
                                            idbase_url + quote(citing_omid), # citing_url,
                                            citing_date, # citing_pub_date,
                                            idbase_url + quote(cited_omid), # cited_url,
                                            cited_date, # cited_pub_date,
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
                                            journal_sc, # journal_sc=False,
                                            author_sc,# author_sc=False,
                                            None, # prov_inv_date=None,
                                            "Creation of the citation", # prov_description=None,
                                            None, # prov_update=None,
                                        )
                                    )

                                    # update cache var
                                    ocis_processed_buffer[oci_omid] = 1

                                # reset buffer
                                cits_buffer = []

                            # write on redis cache var
                            if len(ocis_processed_buffer.keys()) >= REDIS_W_BUFFER:
                                redis_cits.mset(ocis_processed_buffer)
                                ocis_processed_buffer = dict()

                    logger.info("[STATS] duplicated citations="+str(citations_duplicated))
                    logger.info("[STATS] entities with no OMID="+str(len(entities_with_no_omid)))
                    logger.info("[STATS] number of citations lost="+str(len(l_cits) - len(service_citations)))

                    # write on redis cache var when done
                    if len(ocis_processed_buffer.keys()) > 0:
                        redis_cits.mset(ocis_processed_buffer)

                    # Store entities_with_no_omid
                    logger.info("Saving entities with no omid...")
                    with open(output_dir+'entities_with_no_omid.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerows([[e] for e in entities_with_no_omid])

                    BATCH_SAVE = 100000
                    index_ts_storer = CitationStorer(output_dir+"/index-dump", baseurl + "/" if not baseurl.endswith("/") else baseurl, suffix=str(0))
                    logger.info("Saving Index citations to dump...")
                    for idx in range(0, len(index_citations), BATCH_SAVE):
                        batch_citations = index_citations[idx:idx+BATCH_SAVE]
                        index_ts_storer.store_citation(batch_citations)
                    logger.info(f"{len(index_citations)} citations saved")

                    service_storer = CitationStorer(output_dir + "/service-rdf", baseurl + "/" if not baseurl.endswith("/") else baseurl, suffix=str(0), store_as=["rdf_data"])
                    logger.info("Saving service citations (in RDF)...")
                    for idx in range(0, len(service_citations), BATCH_SAVE):
                        batch_citations = service_citations[idx:idx+BATCH_SAVE]
                        service_storer.store_citation(batch_citations)
                    logger.info(f"{len(service_citations)} citations saved")

                    # Store files_processed
                    logger.info("Saving file processed...")
                    with open(output_dir+'files_processed.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerow([str(fzip),str(csv_name)])


    # remove duplicates from entities_with_no_omid
    if os.path.exists(output_dir+'entities_with_no_omid.csv'):
        index_entities = set()
        with open(output_dir+'entities_with_no_omid.csv') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                index_entities.add(row[0])
        with open(output_dir+'entities_with_no_omid.csv', 'w') as f_out:
            csv.writer(f_out).writerows([[e] for e in index_entities])

def main():
    global _config
    logger = get_logger()

    arg_parser = ArgumentParser(description="Create new citations of OC INDEX. This scripts converts citations ANYID-ANYID comming form different data sources (e.g., COCI, DOCI) into OMID-OMID citations. It produces: datasource RDF data, INDEX RDF data, and dump data/prov in RDF,CSV, and SCHOLIX format")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory contatining compressed file(s) (ZIP format) having all the CSV file(s) of the datasource citations",
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
    arg_parser.add_argument(
        '--newdump',
        action='store_true',
        default=False,
        help='Specify it in case the source of the data to convert is not an old dump of OpenCitations (rather a new dump)'
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

    # output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # call the normalize_dump function
    normalize_dump(service, input_files, output_dir, args.newdump)

    logger.info("Done !!")

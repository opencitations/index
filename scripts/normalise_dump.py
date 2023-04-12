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

def normalize_dump(service, input_files, output_dir):
    global _config
    logger = get_logger()

    # get the service values from the CONFIG.INI
    idbase_url = _config.get("INDEX", "idbaseurl")
    index_identifier = _config.get("INDEX", "identifier")
    agent = _config.get("INDEX", "agent")
    service_name = _config.get("INDEX", "service")
    baseurl = _config.get("INDEX", "baseurl")

    # service variables
    identifier = _config.get(service, "identifier")
    source = _config.get(service, "ocdump")

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


    for fzip in input_files:
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                logger.info("Working on the archive:"+str(fzip))
                logger.info("Total number of files in archive is:"+str(len(archive.namelist())))

                # CSV header: oci,citing,cited,creation,timespan,journal_sc,author_sc
                for csv_name in archive.namelist():

                    if not csv_name.endswith(".csv"):
                        logger.info("Skip file (not a CSV): "+str(csv_name))
                        continue

                    index_citations = []
                    citations_duplicated = 0
                    entities_with_no_omid = set()
                    service_citations = []

                    logger.info("Converting the citations in: "+str(csv_name))
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))

                        logger.info("The #citations is: "+str(len(l_cits)))
                        # iterate citations (CSV rows)
                        for row in l_cits:

                            citing = row["citing"]
                            citing_omid = redis_br.get(identifier+":"+citing)

                            cited = row["cited"]
                            cited_omid = redis_br.get(identifier+":"+cited)

                            if citing_omid != None and cited_omid != None:

                                citing_omid = citing_omid.decode("utf-8")
                                cited_omid = cited_omid.decode("utf-8")
                                oci_omid = citing_omid[3:]+"-"+cited_omid[3:]

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

                                #check duplicate
                                if redis_cits.get(oci_omid) == None:

                                    resources = redis_index.mget([citing_omid,cited_omid])
                                    rf_handler = ResourceFinderHandler([OMIDResourceFinder(resources)])

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

                                    # add the OCI of the produced citation to Redis
                                    redis_cits.set(oci_omid, "1")

                                else:
                                    citations_duplicated += 1
                            else:
                                if citing_omid == None:
                                    entities_with_no_omid.add(citing)
                                if cited_omid == None:
                                    entities_with_no_omid.add(cited)

                    logger.info("> duplicated citations="+str(citations_duplicated)+"; entities with no OMID="+str(len(entities_with_no_omid)))

                    # Store entities_with_no_omid
                    logger.info("Saving entities with no omid...")
                    with open(output_dir+'entities_with_no_omid.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerows(list(entities_with_no_omid))

                    # Store the citations of the CSV file
                    index_storer = CitationStorer(output_dir, baseurl + "/" if not baseurl.endswith("/") else baseurl, suffix=str(0))
                    logger.info("Saving Index citations...")
                    for citation in tqdm(index_citations):
                        index_storer.store_citation(citation)
                    logger.info(f"{len(index_citations)} citations saved")

                    service_storer = CitationStorer(output_dir + "/service-rdf", baseurl + "/" if not baseurl.endswith("/") else baseurl, suffix=str(0), store_as=["rdf_data"])
                    logger.info("Saving service citations (in RDF)...")
                    for citation in tqdm(service_citations):
                        service_storer.store_citation(citation)
                    logger.info(f"{len(service_citations)} citations saved")

                    # Store files_processed
                    logger.info("Saving file processed...")
                    with open(output_dir+'files_processed.csv', 'a+') as f:
                        write = csv.writer(f)
                        write.writerow([str(fzip),str(csv_name)])

def main():
    global _config

    arg_parser = ArgumentParser(description="Normalise citations of an index dump (e.g., COCI, DOCI) – convert citations into OMID-OMID format")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory/Zipfile",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        default="COCI",
        help="The source of the dump (e.g. COCI, DOCI)",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The output directory where citations will be stored",
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
    normalize_dump(service, input_files, output_dir)

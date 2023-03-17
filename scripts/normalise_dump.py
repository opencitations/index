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
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.oci.citation import Citation, OCIManager
from oc.index.oci.storer import CitationStorer
from oc.index.glob.redis import RedisDataSource
from oc.index.glob.csv import CSVDataSource

_config = get_config()

def normalize_dump(service, type, input_files, output_dir):
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
    source = _config.get(service, "source")

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

    citations = []
    citations_created = 0
    citations_duplicated = 0
    br_with_no_omid = 0

    for fzip in input_files:
        # checking if it is a file
        if fzip.endswith(".zip"):
            files_to_zip = []
            with ZipFile(fzip) as archive:
                logger.info("Working on the archive:"+str(fzip))
                logger.info("Total number of files in archive is:"+str(len(archive.namelist())))

                if type == "csv":
                    # CSV header: oci,citing,cited,creation,timespan,journal_sc,author_sc
                    for csv_name in archive.namelist():

                        logger.info("Converting the citations in:"+str(csv_name))
                        with archive.open(csv_name) as csv_file:
                            l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))

                            # iterate citations (CSV rows)
                            for row in l_cits:

                                citing = row["citing"]
                                citing_omid = redis_br.get(identifier+":"+citing)

                                cited = row["cited"]
                                cited_omid = redis_br.get(identifier+":"+cited)

                                br_with_no_omid += sum([citing_omid == None, cited_omid == None])

                                if citing_omid != None and cited_omid != None:

                                    citing_omid = citing_omid.decode("utf-8")
                                    cited_omid = cited_omid.decode("utf-8")
                                    oci_omid = citing_omid[3:]+"-"+cited_omid[3:]

                                    #check duplicate
                                    if redis_cits.get(oci_omid) == None:

                                        if "[[citing]]" in source:
                                            source = source.replace("[[citing]]",citing)

                                        creation_date = None
                                        if row["creation"] != "" and row["creation"] != None:
                                            creation_date = row["creation"]

                                        timespan = None
                                        if row["timespan"] != "" and row["timespan"] != None:
                                            timespan = row["timespan"]

                                        journal_sc = "yes" in row["journal_sc"]
                                        author_sc = "yes" in row["author_sc"]

                                        citations.append(
                                            Citation(
                                                oci_omid, # oci,
                                                idbase_url + quote(citing_omid), # citing_url,
                                                None, # citing_pub_date,
                                                idbase_url + quote(cited_omid), # cited_url,
                                                None, # cited_pub_date,
                                                creation_date, # creation,
                                                timespan, # timespan,
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

                                        citations_created += 1
                                        # add the OCI of the produced citation to Redis
                                        redis_cits.set(oci_omid, "1")

                                    else:
                                        citations_duplicated += 1

                            logger.info("> duplicated citations="+str(citations_duplicated)+"; entities with no OMID="+str(br_with_no_omid))

                            # Store the citations of the CSV file
                            storer = CitationStorer(output_dir, baseurl + "/" if not baseurl.endswith("/") else baseurl, suffix=str(0))
                            logger.info("Saving citations...")
                            for citation in tqdm(citations):
                                storer.store_citation(citation)
                            logger.info(f"{len(citations)} citations saved")


                # elif type == "rdf":
                #
                #     # Each citation is an RDF
                #     for rdf_name in archive.namelist():
                #
                #         rdf_file = open(rdf_name, 'r')
                #         cit_block = dict()
                #         while True:
                #             line = rdf_file.readline()
                #             if not line:
                #                 break
                #             line = line.strip()
                #
                #             if "<http://purl.org/spar/cito/Citation>" in line:
                #                 service_oci = re.findall("\<"+url_service_base.replace("/","\/")+"(\d{1,}-\d{1,})\>",line)
                #                 if len(service_oci) > 0:
                #                     cit_block["oci"] = service_oci[0]
                #
                #             if "<http://purl.org/spar/cito/hasCitingEntity>" in line:
                #                 citing = re.findall("\<"+url_prefix.replace("/","\/")+"(.{1,})"+"\>",line)
                #                 if len(citing) > 0:
                #                     citing = citing[0]
                #                     citing_omid = redis_br.get(prefix+citing)
                #                     cit_block["citing"] = citing_omid
                #
                #             if "<http://purl.org/spar/cito/hasCitedEntity>" in line:
                #                 cited = re.findall("\<"+url_prefix.replace("/","\/")+"(.{1,})"+"\>",line)
                #                 if len(cited) > 0:
                #                     cited = cited[0]
                #                     cited_omid = redis_br.get(prefix+cited)
                #                     cit_block["cited"] = cited_omid
                #
                #             if "<http://purl.org/spar/cito/hasCitationCreationDate>" in line:
                #                 creation = re.findall("\"(\d{4}-\d{2}-\d{2})\"\^\^",line)
                #                 if len(creation) > 0:
                #                     creation = creation[0]
                #                     cit_block["creation"] = creation
                #
                #         rdf_file.close()

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
        "-t",
        "--type",
        default="csv",
        help="The data type of the dump (e.g. csv)",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        default="COCI",
        help="The source of the dump (e.g. coci)",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The output directory where citations will be stored",
    )
    args = arg_parser.parse_args()
    type = args.type
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
    normalize_dump(service, type, input_files, output_dir)

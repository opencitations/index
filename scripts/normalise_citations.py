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
import math
import csv

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime

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


def normalise_cits(service, file, parser, ds, multiprocess):
    global _config

    oci_manager = OCIManager(
        lookup_file=os.path.expanduser(_config.get("cnc", "lookup"))
    )
    logger = get_logger()

    logger.info("Reading citation data from " + file)
    parser.parse(file)
    pbar = tqdm(total=parser.items, disable=multiprocess)
    citation_data = 1
    citation_data_list = []
    ids = []

    citation_data = parser.get_next_citation_data()
    identifier = _config.get(service, "identifier")
    while citation_data is not None:
        if isinstance(citation_data, list):
            citation_data_list = citation_data_list + citation_data
            for c_citation_data in citation_data:
                ids = ids + [
                    identifier + ":" + c_citation_data[0],
                    identifier + ":" + c_citation_data[1],
                ]
        else:
            citation_data_list.append(citation_data)
            ids = ids + [
                identifier + ":" + citation_data[0],
                identifier + ":" + citation_data[1],
            ]
        pbar.update(parser.current_item - pbar.n)
        citation_data = parser.get_next_citation_data()
    pbar.close()

    ids = list(set(ids))

    logger.info("Retrieving citation data informations from data source")
    resources = {}
    batch_size = _config.getint("redis", "batch_size")
    pbar = tqdm(total=len(ids), disable=multiprocess)
    while len(ids) > 0:
        current_size = min(len(ids), batch_size)
        batch = ids[:current_size]
        batch_result = ds.mget(batch)
        for key in batch_result.keys():
            resources[key.replace(identifier + ":", "")] = batch_result[key]
        ids = ids[batch_size:] if batch_size < len(ids) else []
        pbar.update(current_size)
    pbar.close()
    logger.info("Information retrivied")
    use_api = False
    crossref_rc = CrossrefResourceFinder(resources, use_api)
    rf_handler = ResourceFinderHandler(
        [
            crossref_rc,
            ORCIDResourceFinder(resources, use_api,""),
            DataCiteResourceFinder(resources, use_api),
        ]
    )

    logger.info(
        f"Working on {len(citation_data_list)} citation data with related support information"
    )
    citations_created = 0

    idbase_url = _config.get(service, "idbaseurl")
    prefix = _config.get("INDEX", "prefix")
    agent = _config.get(service, "agent")
    source = _config.get(service, "source")
    service_name = _config.get(service, "service")
    citations = []

    for citation_data in tqdm(citation_data_list, disable=multiprocess):
        (
            citing,
            cited,
            citing_date,
            cited_date,
            author_sc,
            journal_sc,
        ) = citation_data

        if crossref_rc.is_valid(citing) and crossref_rc.is_valid(cited):

            citing = rf_handler.get_omid(citing).replace("omid","")
            cited = rf_handler.get_omid(cited).replace("omid","")
            if citing != None and cited != None:
                citations.append([
                    (citing,cited),
                    Citation(
                        "oci:%s%s-%s%s" % (prefix,citing,prefix,cited,),
                        idbase_url + quote(citing),
                        None,
                        idbase_url + quote(cited),
                        None,
                        None,
                        None,
                        1,
                        agent,
                        source,
                        datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                        service_name,
                        identifier,
                        idbase_url + "([[XXX__decode]])",
                        "reference",
                        None,
                        None,
                        None,
                        "Creation of the citation",
                        None,
                    )
                  ]
                )

            citations_created += 1

    logger.info(f"{citations_created}/{len(citation_data_list)} Citations created")
    return citations


def worker_body(input_files, output, service, tid, multiprocess):
    global _config

    service_ds = _config.get(service, "datasource")
    ds = None
    if service_ds == "redis":
        #set redis to use the unified index in redis, i.e., META
        ds = RedisDataSource(service, True)
    elif service_ds == "csv":
        ds = CSVDataSource(service)
    else:
        raise Exception(service_ds + " is not a valid data source")

    logger = get_logger()
    parser = CitationParser.get_parser(service)
    baseurl = _config.get(service, "baseurl")
    unified_baseurl = _config.get("INDEX", "baseurl")
    index_storer = CitationStorer(
        output + "/service-rdf", unified_baseurl + "/" if not unified_baseurl.endswith("/") else unified_baseurl, suffix=str(tid), store_as=["rdf_data"]
    )

    logger.info("Working on " + str(len(input_files)) + " files")

    for file in input_files:
        citations = normalise_cits(service, file, parser, ds, multiprocess)

        if len(citations) > 0:
            logger.info("Saving normalised citations into CSV...")
            if not os.path.exists(output + "/dump"):
                os.makedirs(output + "/dump")
            output_norm_file = output + "/dump/"+".".join(file.split('/')[-1].split(".")[:-1])+".csv"
            with open(output_norm_file,'w+') as f:
                csv_out = csv.writer(f)
                for citation in tqdm(citations, disable=multiprocess):
                    csv_out.writerow(citation[0])

            logger.info("Saving RDF data to be loaded into INDEX triplestore...")
            for citation in tqdm(citations, disable=multiprocess):
                index_storer.store_citation(citation[1])

        logger.info(f"{len(citations)} citations saved")


def main():
    global _config

    arg_parser = ArgumentParser(description="Normalise citations – converted into OMID to OMID citations")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input file/directory to provide as input",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="The output directory where citations will be stored",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        required=True,
        choices=_config.get("cnc", "services").split(","),
        help="Service config to use, e.g. for parser, identifier type, etc..",
    )
    arg_parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="Number of workers to use, default is 1",
    )
    args = arg_parser.parse_args()

    logger = get_logger()

    # Arguments
    input = args.input
    output = args.output
    service = args.service
    workers = args.workers

    if not os.path.exists(input):
        logger.error(
            "The path specified as input value is not present in the file system."
        )

    logger.info("Browse input to find files to parse")
    input_files = []
    parser = CitationParser.get_parser(service)
    if os.path.isdir(input):
        for current_dir, _, current_files in os.walk(input):
            for current_file in current_files:
                file_path = os.path.join(current_dir, current_file)
                if parser.is_valid(file_path):
                    input_files.append(file_path)
    elif parser.is_valid(input):
        input_files.append(input)
    logger.info(f"{len(input_files)} files were found")

    start = time.time()
    workers_list = []
    last_index = 0
    multiprocess = workers > 1
    if multiprocess:
        # Disable tqdm for multithreading
        logger.info(f"Multitprocessing ON, starting {workers} workers")
        chunk_size = math.ceil(len(input_files) / workers)
        for tid in range(workers - 1):
            process = multiprocessing.Process(
                target=worker_body,
                args=(
                    input_files[last_index: (last_index + chunk_size)],
                    output,
                    service,
                    tid + 1,
                    multiprocess,
                ),
            )
            last_index += chunk_size
            process.name = "Process:" + str(tid + 1)
            workers_list.append(process)
            process.start()
        logger.info("All workers have been started")

    # No active wait also the main thread work on processing file
    worker_body(
        input_files[last_index: len(
            input_files)], output, service, 0, multiprocess
    )
    if multiprocess:
        for worker in workers_list:
            worker.join()

    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

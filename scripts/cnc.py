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

import threading
import os
import importlib
import time
import math

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config
from oc.index.glob.datasource import DataSource
from oc.index.finder.base import ResourceFinderHandler
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.oci.citation import Citation, OCIManager
from oc.index.oci.storer import CitationStorer

_config = get_config()
_oci_manager = OCIManager(lookup_file=os.path.expanduser(_config.get("cnc", "lookup")))
_storer = None
_storer_lock = threading.Lock()
_multithread = False
_citations_created = 0


def cnc(service, file, parser, ds):
    global _oci_manager
    global _multithread
    logger = get_logger()

    logger.info("Reading citation data from " + file)
    parser.parse(file)
    pbar = tqdm(total=parser.items, disable=_multithread)
    citation_data = 1
    citation_data_list = []
    ids = []

    citation_data = parser.get_next_citation_data()
    while citation_data is not None:
        if isinstance(citation_data, list):
            citation_data_list = citation_data_list + citation_data
            for c_citation_data in citation_data:
                ids = ids + [c_citation_data[0], c_citation_data[1]]
        else:
            citation_data_list.append(citation_data)
            ids = ids + [citation_data[0], citation_data[1]]
        pbar.update(parser.current_item - pbar.n)
        citation_data = parser.get_next_citation_data()
    pbar.close()

    ids = list(set(ids))

    logger.info("Retrieving citation data informations from data source")
    resources = {}
    batch_size = _config.getint("redis", "batch_size")
    pbar = tqdm(total=len(ids), disable=_multithread)
    while len(ids) > 0:
        current_size = min(len(ids), batch_size)
        batch = ids[:current_size]
        resources.update(ds.mget(batch))
        ids = ids[batch_size:] if batch_size < len(ids) else []
        pbar.update(current_size)
    pbar.close()
    logger.info("Information retrivied")
    use_api = _config.getboolean("cnc", "use_api")
    crossref_rc = CrossrefResourceFinder(resources, use_api)
    rf_handler = ResourceFinderHandler(
        [
            crossref_rc,
            ORCIDResourceFinder(resources, use_api, _config.get("cnc", "orcid")),
            DataCiteResourceFinder(resources, use_api),
        ]
    )

    logger.info("Creating Citation objects using citation data and support information")
    citations_created = 0
    idbase_url = _config.get(service, "idbaseurl")
    prefix = _config.get(service, "prefix")
    agent = _config.get(service, "agent")
    source = _config.get(service, "source")
    service_name = _config.get(service, "service")
    citations = []
    for citation_data in tqdm(citation_data_list, disable=_multithread):
        (
            citing,
            cited,
            citing_date,
            cited_date,
            author_sc,
            journal_sc,
        ) = citation_data

        if crossref_rc.is_valid(citing) and crossref_rc.is_valid(cited):
            if citing_date is None:
                citing_date = rf_handler.get_date(citing)

            if cited_date is None:
                cited_date = rf_handler.get_date(cited)

            if journal_sc is None or type(journal_sc) is not bool:
                journal_sc = rf_handler.share_issn(citing, cited)

            if author_sc is None or type(author_sc) is not bool:
                author_sc = rf_handler.share_orcid(citing, cited)

            citations.append(
                Citation(
                    _oci_manager.get_oci(citing, cited, prefix),
                    idbase_url + quote(citing),
                    citing_date,
                    idbase_url + quote(cited),
                    cited_date,
                    None,
                    None,
                    1,
                    agent,
                    source,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    service_name,
                    "doi",
                    idbase_url + "([[XXX__decode]])",
                    "reference",
                    journal_sc,
                    author_sc,
                    None,
                    "Creation of the citation",
                    None,
                )
            )

            citations_created += 1
    logger.info(f"{citations_created}/{len(citation_data_list)} Citations created")
    return citations


def thread_body(input_files, service):
    global _storer
    global _storer_lock
    global _multithread
    global _citations_created

    ds = DataSource()
    logger = get_logger()
    parser = get_parser(service)

    for file in input_files:
        citations = cnc(service, file, parser, ds)
        logger.info("Saving citations...")
        _storer_lock.acquire()
        for citation in tqdm(citations, disable=_multithread):
            _storer.store_citation(citation)
        _citations_created += len(citations)
        _storer_lock.release()
        logger.info("Citations saved")
    return


def get_parser(service):
    # Initialize the parser
    module, classname = _config.get(service, "parser").split(":")
    return getattr(importlib.import_module(module), classname)()


def main():
    arg_parser = ArgumentParser(description="CNC - create new citations")
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
        help="Parser to use to read the input",
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

    global _multithread
    _multithread = workers > 1

    if not os.path.exists(input):
        logger.error(
            "The path specified as input value is not present in the file system."
        )

    logger.info("Browse input to find files to parse")
    input_files = []
    parser = get_parser(service)
    if os.path.isdir(input):
        for current_dir, _, current_files in os.walk(input):
            for current_file in current_files:
                file_path = os.path.join(current_dir, current_file)
                if parser.is_valid(file_path):
                    input_files.append(file_path)
    elif parser.is_valid(input):
        input_files.append(input)

    logger.info(f"{len(input_files)} files were found")

    baseurl = _config.get(service, "baseurl")
    global _storer
    _storer = CitationStorer(
        output, baseurl + "/" if not baseurl.endswith("/") else baseurl
    )

    start = time.time()
    threads = []
    last_index = 0
    if _multithread:
        # Disable tqdm for multithreading
        logger.info(f"Multithreading ON, starting {workers} workers")
        chunk_size = math.ceil(len(input_files) / workers)
        for tid in range(workers - 1):
            thread = threading.Thread(
                target=thread_body,
                args=(input_files[last_index : (last_index + chunk_size)], service),
            )
            last_index += chunk_size
            thread.name = "WorkerThread:" + str(tid)
            threads.append(thread)
            thread.start()
        logger.info("All workers have been started")

    # No active wait also the main thread work on processing file
    thread_body(input_files[last_index : len(input_files)], service)
    if _multithread:
        for thread in threads:
            thread.join()

    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )
    global _citations_created
    logger.info(f"{_citations_created} citations have been stored")

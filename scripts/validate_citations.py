import os
import time
import math
import multiprocessing

from argparse import ArgumentParser
from subprocess import check_output

from oc.index.parsing.base import CitationParser
from oc.index.utils.config import get_config
from oc.index.utils.logging import get_logger
from oc.index.validate.base import CitationValidator


def worker_body(input_files, service, oci_dir, moph_dir, queue, pid, multiprocess):
    logger = get_logger()
    validator = CitationValidator.get_validator(service)

    result_map = {}
    for filename in input_files:
        query = validator.build_oci_query(filename, result_map, multiprocess)

        # Create input file
        with open("input" + str(pid) + ".csv", "w") as f:
            for oci in query:
                f.write(oci + "\n")

        # Compute lookup result
        logger.info("Checking the oci to verify existing ones")
        query_result = str(
            check_output(
                [
                    "oc.index.lookup",
                    "-o",
                    oci_dir,
                    "-m",
                    moph_dir,
                    "-i",
                    "input" + str(pid) + ".csv",
                ],
            ).split()[0]
        )
        query_result = query_result[1:].replace("'", "")
        i = 0
        for result in query_result.split(","):
            result_map[query[i]] = int(result) == 1
            i += 1
        logger.info("Result map updated")

    queue.put(result_map)


def main():
    config = get_config()

    arg_parser = ArgumentParser(
        "Remove duplicates and existing citations from COCI",
        description="Process Crossref JSON files and remove duplicated citations and existing citations",
    )
    arg_parser.add_argument(
        "-s",
        "--service",
        required=True,
        choices=config.get("cnc", "services").split(","),
        help="Service config to use, e.g. for parser, identifier type, etc..",
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        dest="input",
        required=True,
        help="The directory contains the Crossref data dump of JSON files.",
    )
    arg_parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=1,
        help="Number of workers to use, default is 1",
    )
    arg_parser.add_argument(
        "-oci",
        "--oci_dir",
        dest="oci_dir",
        required=True,
        help="Path to the OCIs directory.",
    )
    arg_parser.add_argument(
        "-m",
        "--moph_dir",
        dest="moph_dir",
        required=True,
        help="Path to the moph directory.",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        dest="output",
        required=True,
        help="The directory where the Crossref citations are stored.",
    )

    logger = get_logger()

    # Parse argument
    args = arg_parser.parse_args()
    workers = args.workers
    service = args.service

    start = time.time()

    # Create output directory if does not exist
    if not os.path.exists(args.output):
        os.makedirs(args.output)

    parser = CitationParser.get_parser(service)

    # Get all the input files
    input_files = []
    if os.path.isdir(args.input):
        for current_dir, _, current_files in os.walk(args.input):
            for current_file in current_files:
                file_path = os.path.join(current_dir, current_file)
                if parser.is_valid(file_path):
                    input_files.append(file_path)

    logger.info("Updating OCI lookup table")
    validator = CitationValidator.get_validator(service)
    # Update oci lookup synchronous
    for filename in input_files:
        validator.build_oci_query(filename, {}, False)
    logger.info("OCI lookup table updated")

    # Extract the result map containing oci => value
    # value is 1 if the citations exists, 0 otherwise.
    queue = multiprocessing.Queue()
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
                    input_files[last_index : (last_index + chunk_size)],
                    service,
                    args.oci_dir,
                    args.moph_dir,
                    queue,
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
        input_files[last_index : len(input_files)],
        service,
        args.oci_dir,
        args.moph_dir,
        queue,
        0,
        multiprocess,
    )

    # Building the oci map with existing info
    result_map = {}
    logger.info("Building global result map")
    for _ in range(workers):
        result_map.update(queue.get())
    logger.info("Result map built")

    # Validate citations according to the result map
    validator.validate_citations(args.input, result_map, args.output)

    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

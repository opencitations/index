import os
import time
import json
from argparse import ArgumentParser
from tqdm import tqdm
import math
import multiprocessing
from subprocess import check_output


from oc.index.identifier.doi import DOIManager
from oc.index.oci.citation import OCIManager
from oc.index.utils.config import get_config
from oc.index.utils.logging import get_logger

_multiprocess = 0


def worker_body(input_dir, input_files, oci_dir, moph_dir, queue):
    logger = get_logger()
    config = get_config()
    oci_manager = OCIManager(
        lookup_file=os.path.expanduser(config.get("cnc", "lookup"))
    )
    prefix = config.get("COCI", "prefix")
    doi_manager = DOIManager()

    result_map = {}
    for filename in input_files:
        json_content = {"items": []}

        # Build the OCI lookup query
        logger.info("1/4 Reading citation data from " + filename)
        query = []
        with open(os.path.join(input_dir, filename), encoding="utf8") as fp:
            json_content = json.load(fp)
        for row in tqdm(json_content["items"], disable=_multiprocess):
            citing = doi_manager.normalise(row.get("DOI"))
            if citing is not None and "reference" in row:
                for ref in row["reference"]:
                    cited = doi_manager.normalise(ref.get("DOI"))
                    if cited is not None:
                        oci = oci_manager.get_oci(citing, cited, prefix=prefix).replace(
                            "oci:", ""
                        )
                        # Add oci only if has not been processed in the past
                        # in the case this is a duplicate.
                        if oci not in result_map:
                            query.append(oci)

        # Create input file
        with open("input.csv", "w") as f:
            for oci in query:
                f.write(oci + "\n")

        # Compute lookup result
        logger.info("2/4 Checking the oci to verify existing ones")
        query_result = str(
            check_output(
                [
                    "oci_lookup",
                    "-o",
                    oci_dir,
                    "-m",
                    moph_dir,
                    "-i",
                    "./input.csv",
                ],
            ).split()[0]
        )
        query_result = query_result[1:].replace("'", "")
        i = 0
        for result in query_result.split(","):
            result_map[query[i]] = int(result) == 1
            i += 1

    queue.append(result_map)


def main():
    arg_parser = ArgumentParser(
        "Remove duplicates and existing citations from COCI",
        description="Process Crossref JSON files and remove duplicated citations and existing citations",
    )
    arg_parser.add_argument(
        "-i",
        "--input_dir",
        dest="input_dir",
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
        "--output_dir",
        dest="output_dir",
        required=True,
        help="The directory where the Crossref citations are stored.",
    )
    logger = get_logger()
    config = get_config()
    args = arg_parser.parse_args()
    workers = args.workers
    result_map = {}
    start = time.time()
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # Get all the input files
    input_files = []
    for filename in os.listdir(args.input_dir):
        if filename.endswith(".json"):
            input_files.append(filename)

    # Building the oci map with existing info
    queue = multiprocessing.Queue()
    workers_list = []
    last_index = 0
    global _multiprocess
    _multiprocess = workers > 1
    if _multiprocess:
        # Disable tqdm for multithreading
        logger.info(f"Multitprocessing ON, starting {workers} workers")
        chunk_size = math.ceil(len(input_files) / workers)
        for tid in range(workers - 1):
            process = multiprocessing.Process(
                target=worker_body,
                args=(
                    args.input_dir,
                    input_files[last_index : (last_index + chunk_size)],
                    args.oci_dir,
                    args.moph_dir,
                    queue,
                ),
            )
            last_index += chunk_size
            process.name = "Process:" + str(tid + 1)
            workers_list.append(process)
            process.start()
        logger.info("All workers have been started")

    # No active wait also the main thread work on processing file
    worker_body(
        args.input_dir,
        input_files[last_index : len(input_files)],
        args.oci_dir,
        args.moph_dir,
        queue,
    )
    if _multiprocess:
        for worker in workers_list:
            worker.join()

    oci_manager = OCIManager(
        lookup_file=os.path.expanduser(config.get("cnc", "lookup"))
    )
    prefix = config.get("COCI", "prefix")
    doi_manager = DOIManager()
    result_map = {}
    for _ in range(workers):
        result_map.update(queue.get())

    for filename in os.listdir(args.input_dir):
        if filename.endswith(".json"):
            json_content = {"items": []}

            # Build the OCI lookup query
            logger.info("Reading citation data from " + filename)
            query = []
            with open(os.path.join(args.input_dir, filename), encoding="utf8") as fp:
                json_content = json.load(fp)
            for row in tqdm(json_content["items"]):
                citing = doi_manager.normalise(row.get("DOI"))
                if citing is not None and "reference" in row:
                    for ref in row["reference"]:
                        cited = doi_manager.normalise(ref.get("DOI"))
                        if cited is not None:
                            oci = oci_manager.get_oci(
                                citing, cited, prefix=prefix
                            ).replace("oci:", "")
                            # Add oci only if has not been processed in the past
                            # in the case this is a duplicate.
                            if oci not in result_map:
                                query.append(oci)

            # Create input file
            with open("input.csv", "w") as f:
                for oci in query:
                    f.write(oci + "\n")

            # Remove the processed citations
            logger.info("Remove duplicates and existiting citations")
            duplicated = 0
            items = []
            for row in tqdm(json_content["items"]):
                citing = doi_manager.normalise(row.get("DOI"))
                if citing is not None and "reference" in row:
                    reference = []
                    for ref in row["reference"]:
                        cited = doi_manager.normalise(ref.get("DOI"))
                        if cited is not None:
                            oci = oci_manager.get_oci(
                                citing, cited, prefix=prefix
                            ).replace("oci:", "")
                            # Add oci only if has not been preprocessed and it is not a duplicate
                            if oci in result_map and not result_map[oci]:
                                # Set result map true for the oci to avoid duplicates
                                result_map[oci] = True
                                reference.append(ref)
                            else:
                                duplicated += 1
                    row["reference"] = reference
                    items.append(row)

            # Save validated citations
            logger.info(str(duplicated) + " citations deleted")
            logger.info("Saving validated citations...")
            with open(os.path.join(args.output_dir, filename), "w") as fp:
                json.dump({"items": items}, fp)

    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

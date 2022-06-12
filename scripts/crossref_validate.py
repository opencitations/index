import os
import time
import json
from argparse import ArgumentParser
from tqdm import tqdm
from subprocess import check_output


from oc.index.identifier.doi import DOIManager
from oc.index.oci.citation import OCIManager
from oc.index.utils.config import get_config
from oc.index.utils.logging import get_logger


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
        "-o",
        "--output_dir",
        dest="output_dir",
        required=True,
        help="The directory where the Crossref citations are stored.",
    )
    logger = get_logger()
    config = get_config()
    oci_manager = OCIManager(
        lookup_file=os.path.expanduser(config.get("cnc", "lookup"))
    )
    prefix = config.get("COCI", "prefix")
    doi_manager = DOIManager()
    args = arg_parser.parse_args()
    result_map = {}
    start = time.time()
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    for filename in os.listdir(args.input_dir):
        if filename.endswith(".json"):
            json_content = {"items": []}

            # Build the OCI lookup query
            logger.info("1/4 Reading citation data from " + filename)
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

            # Compute lookup result
            logger.info("2/4 Checking the oci to verify existing ones")
            query_result = str(
                check_output(
                    [
                        "oci_lookup",
                        "-o",
                        "../coci",
                        "-m",
                        "../coci_moph",
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

            # Remove the processed citations
            logger.info("3/4 Remove duplicates and existiting citations")
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
            logger.info(duplicated + " citations deleted")
            logger.info("4/4 Saving validated citations...")
            with open(os.path.join(args.output_dir, filename), "w") as fp:
                json.dump({"items": items}, fp)

    logger.info(
        f"All the files have been processed in {(time.time() - start)/ 60} minutes"
    )

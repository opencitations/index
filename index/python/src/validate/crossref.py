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

import json
import os

from tqdm import tqdm

from oc.index.validate.base import CitationValidator
from oc.index.identifier.doi import DOIManager
from oc.index.utils.logging import get_logger


class CrossrefValidator(CitationValidator):
    def __init__(self,service):
        super().__init__(service)
        self._doi_manager = DOIManager()
        self._logger = get_logger()

    def build_oci_query(self, input_file, result_map, disable_tqdm=False):
        json_content = {"items": []}

        # Build the OCI lookup query
        self._logger.info("Reading citation data from " + input_file)
        query = []
        with open(input_file, encoding="utf8") as fp:
            json_content = json.load(fp)
        for row in tqdm(json_content["items"], disable=disable_tqdm):
            citing = self._doi_manager.normalise(row.get("DOI"))
            if citing is not None and "reference" in row:
                for ref in row["reference"]:
                    cited = self._doi_manager.normalise(ref.get("DOI"))
                    if cited is not None:
                        oci = self._oci_manager.get_oci(
                            citing, cited, prefix=self._prefix
                        ).replace("oci:", "")
                        # Add oci only if has not been processed in the past
                        # in the case this is a duplicate.
                        if oci not in result_map:
                            query.append(oci)
        return query

    def validate_citations(self, input_directory, result_map, output_directory):
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        for filename in os.listdir(input_directory):
            if filename.endswith(".json"):
                json_content = {"items": []}

                # Build the OCI lookup query
                self._logger.info("Reading citation data from " + filename)
                query = []
                with open(
                    os.path.join(input_directory, filename), encoding="utf8"
                ) as fp:
                    json_content = json.load(fp)
                for row in tqdm(json_content["items"]):
                    citing = self._doi_manager.normalise(row.get("DOI"))
                    if citing is not None and "reference" in row:
                        for ref in row["reference"]:
                            cited = self._doi_manager.normalise(ref.get("DOI"))
                            if cited is not None:
                                oci = self._oci_manager.get_oci(
                                    citing, cited, prefix=self._prefix
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
                self._logger.info("Remove duplicates and existiting citations")
                duplicated = 0
                items = []
                for row in tqdm(json_content["items"]):
                    citing = self._doi_manager.normalise(row.get("DOI"))
                    if citing is not None and "reference" in row:
                        reference = []
                        for ref in row["reference"]:
                            cited = self._doi_manager.normalise(ref.get("DOI"))
                            if cited is not None:
                                oci = self._oci_manager.get_oci(
                                    citing, cited, prefix=self._prefix
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
                self._logger.info(str(duplicated) + " citations deleted")
                self._logger.info("Saving validated citations...")
                with open(os.path.join(output_directory, filename), "w") as fp:
                    json.dump({"items": items}, fp)

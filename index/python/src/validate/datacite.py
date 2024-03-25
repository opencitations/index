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
import pandas as pd
import csv
from tqdm import tqdm

from oc.index.validate.base import CitationValidator
from oc.index.identifier.doi import DOIManager
from oc.index.utils.logging import get_logger


class DataciteValidator(CitationValidator):
    def __init__(self, service):
        super().__init__(service)
        self._doi_manager = DOIManager()
        self._logger = get_logger()


    def build_oci_query(self, input_file, result_map, disable_tqdm=False):
        csv_content = []

        # Build the OCI lookup query
        self._logger.info("Reading citation data from " + input_file)
        query = []

        df = pd.DataFrame()
        for chunk in pd.read_csv(input_file, chunksize=1000):
            f = pd.concat([df, chunk], ignore_index=True)
            f.fillna("", inplace=True)
            csv_content = f.to_dict("records")
            for row in tqdm(csv_content, disable=disable_tqdm):
                citing = self._doi_manager.normalise(row.get("citing"))
                cited = self._doi_manager.normalise(row.get("referenced"))

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
            if filename.endswith(".csv"):
                csv_content = []

                # Build the OCI lookup query
                self._logger.info("Reading citation data from " + filename)
                query = []
                df = pd.DataFrame()
                for chunk in pd.read_csv(
                        os.path.join(input_directory, filename), chunksize=1000
                ):
                    f = pd.concat([df, chunk], ignore_index=True)
                    f.fillna("", inplace=True)
                    csv_content = f.to_dict("records")
                    for row in tqdm(csv_content):
                        citing = self._doi_manager.normalise(row.get("citing"))
                        cited = self._doi_manager.normalise(row.get("referenced"))
                        if citing is not None and cited is not None:
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
                for row in tqdm(csv_content):
                    citing = self._doi_manager.normalise(row.get("citing"))
                    cited = self._doi_manager.normalise(row.get("referenced"))
                    if citing is not None and cited is not None:
                        oci = self._oci_manager.get_oci(
                            citing, cited, prefix=self._prefix
                        ).replace("oci:", "")
                        if oci in result_map and not result_map[oci]:
                            # Set result map true for the oci to avoid duplicates
                            result_map[oci] = True
                            items.append(row)
                        else:
                            duplicated += 1

                # Save validated citations
                self._logger.info(str(duplicated) + " citations deleted")
                self._logger.info("Saving validated citations...")
                keys = items[0].keys()
                with open(
                        os.path.join(output_directory, filename), "w", newline=""
                ) as output_file:
                    dict_writer = csv.DictWriter(output_file, keys)
                    dict_writer.writeheader()
                    dict_writer.writerows(items)

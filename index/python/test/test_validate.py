#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021, 2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021, 2022 Giuseppe Grieco <g.grieco1997@gmail.com>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import unittest
import os
from os.path import join
from csv import DictReader

from oc.index.validate.crossref import CrossrefValidator
from oc.index.validate.datacite import DataciteValidator
from oc.index.validate.nih import NIHValidator
from oc.index.oci.citation import OCIManager
from oc.index.utils.config import get_config, reset_config


class ValidateTest(unittest.TestCase):
    """This class aim at testing the methods of the class
    belongs to package oc.index.validate"""

    def setUp(self):
        reset_config()
        config = get_config(join("index", "python", "test", "config.ini"))
        test_dir = join("index", "python", "test", "data")

        self.coci_input = join(test_dir, "coci_validate")
        self.coci_validate = CrossrefValidator("COCI")

        self.doci_input = join(test_dir, "doci_validate")
        self.doci_validate = DataciteValidator("DOCI")

        self.noci_input = join(test_dir, "noci_validate")
        self.noci_validate = NIHValidator("NOCI")

        oci_manager = OCIManager(
            lookup_file=os.path.expanduser(config.get("cnc", "lookup"))
        )

        with open(join(test_dir, "crossref_citations.csv"), encoding="utf8") as f:
            self.coci_truth = []
            for citation in list(DictReader(f)):
                self.coci_truth.append(
                    oci_manager.get_oci(
                        citation["citing"], citation["cited"], prefix="020"
                    ).replace("oci:", "")
                )

        with open(join(test_dir, "doci_citations.csv"), encoding="utf8") as f0:
            self.doci_truth = []
            for citation in list(DictReader(f0)):
                self.doci_truth.append(
                    oci_manager.get_oci(
                        citation["citing"], citation["cited"], prefix="080"
                    ).replace("oci:", "")
                )

        with open(join(test_dir, "noci_citations.csv"), encoding="utf8") as f1:
            self.noci_truth = []
            for citation in list(DictReader(f1)):
                self.noci_truth.append(
                    oci_manager.get_oci(
                        citation["citing"], citation["cited"], prefix="0160"
                    ).replace("oci:", "")
                )

    def test_crossref_query_build(self):
        query = self.coci_validate.build_oci_query(
            join(self.coci_input, "0.json"), {}, disable_tqdm=True
        )
        self.assertEqual(set(query), set(self.coci_truth))

    def test_crossref_validate(self):
        query_old = self.coci_validate.build_oci_query(
            join(self.coci_input, "0.json"), {}, disable_tqdm=True
        )
        result_map = {key: False for key in query_old}
        result_map[
            "020070701073625141427193704030705-0200100000236211410253701000201"
        ] = True

        self.coci_validate.validate_citations(
            self.coci_input, result_map, join("tmp", "coci_validate")
        )
        query_new = self.coci_validate.build_oci_query(
            join(join("tmp", "coci_validate"), "0.json"), {}, disable_tqdm=True
        )
        self.assertEqual(len(query_old) - 1, len(query_new))
        pass

    def test_datacite_query_build(self):
        query = self.doci_validate.build_oci_query(
            join(self.doci_input, "0.csv"), {}, disable_tqdm=True
        )
        self.assertEqual(set(query), set(self.doci_truth))

    def test_datacite_validate(self):
        query_old = self.doci_validate.build_oci_query(
            join(self.doci_input, "0.csv"), {}, disable_tqdm=True
        )
        result_map = {key: False for key in query_old}
        result_map[
            "0800100000236020001041611000004090705-0800100020936020000021912000001070309"
        ] = True
        self.doci_validate.validate_citations(
            self.doci_input, result_map, join("tmp", "doci_validate")
        )
        query_new = self.doci_validate.build_oci_query(
            join(join("tmp", "doci_validate"), "0.csv"), {}, disable_tqdm=True
        )
        self.assertEqual(len(query_old) - 1, len(query_new))
        pass

    def test_nih_query_build(self):
        query = self.noci_validate.build_oci_query(
            join(self.noci_input, "0.csv"), {}, disable_tqdm=True
        )
        self.assertEqual(set(query), set(self.noci_truth))

    def test_nih_validate(self):
        query_old = self.noci_validate.build_oci_query(
            join(self.noci_input, "0.csv"), {}, disable_tqdm=True
        )
        result_map = {key: False for key in query_old}
        result_map["016001050203050709-016007000907050609"] = True

        self.noci_validate.validate_citations(
            self.noci_input, result_map, join("tmp", "noci_validate")
        )
        query_new = self.noci_validate.build_oci_query(
            join(join("tmp", "noci_validate"), "0.csv"), {}, disable_tqdm=True
        )
        self.assertEqual(len(query_old) - 1, len(query_new))
        pass

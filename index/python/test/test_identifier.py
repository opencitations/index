#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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

import unittest
import json

from os.path import join

from oc.index.identifier.doi import DOIManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager


class IdentifierManagerTest(unittest.TestCase):
    """This class aim at testing identifiers manager."""

    def setUp(self):
        self.valid_doi_1 = "10.1108/jd-12-2013-0166"
        self.valid_doi_2 = "10.1130/2015.2513(00)"
        self.invalid_doi_1 = "10.1108/12-2013-0166"
        self.invalid_doi_2 = "10.1371"

        test_dir = join("index", "python", "test", "data")
        with open(join(test_dir, "glob.json"), encoding="utf-8") as fp:
            self.data = json.load(fp)

        self.valid_issn_1 = "2376-5992"
        self.valid_issn_2 = "1474-175X"
        self.invalid_issn_1 = "2376-599C"
        self.invalid_issn_2 = "2376-5995"
        self.invalid_issn_3 = "2376-599"

        self.valid_orcid_1 = "0000-0003-0530-4305"
        self.valid_orcid_2 = "0000-0001-5506-523X"
        self.invalid_orcid_1 = "0000-0003-0530-430C"
        self.invalid_orcid_2 = "0000-0001-5506-5232"
        self.invalid_orcid_3 = "0000-0001-5506-523"
        self.invalid_orcid_4 = "1-5506-5232"

    def test_doi_normalise(self):
        dm = DOIManager()
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi: 10. ")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(self.valid_doi_1.upper().replace("10.", "doi:10.")),
        )
        self.assertEqual(
            self.valid_doi_1,
            dm.normalise(
                self.valid_doi_1.upper().replace("10.", "https://doi.org/10.")
            ),
        )

    def test_doi_is_valid(self):
        dm_nofile = DOIManager()
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_1))
        self.assertTrue(dm_nofile.is_valid(self.valid_doi_2))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_1))
        self.assertFalse(dm_nofile.is_valid(self.invalid_doi_2))

        dm_file = DOIManager(self.data, use_api_service=False)
        self.assertTrue(dm_file.is_valid(self.valid_doi_1))
        self.assertFalse(dm_file.is_valid(self.invalid_doi_1))

        dm_nofile_noapi = DOIManager(use_api_service=False)
        self.assertFalse(dm_nofile_noapi.is_valid(self.valid_doi_1))
        self.assertFalse(dm_nofile_noapi.is_valid(self.invalid_doi_1))

    def test_issn_normalise(self):
        im = ISSNManager()
        self.assertEqual(
            self.valid_issn_1, im.normalise(self.valid_issn_1.replace("-", "  "))
        )
        self.assertEqual(
            self.valid_issn_2, im.normalise(self.valid_issn_2.replace("-", "  "))
        )
        self.assertEqual(
            self.invalid_issn_3, im.normalise(self.invalid_issn_3.replace("-", "  "))
        )

    def test_issn_is_valid(self):
        im = ISSNManager()
        self.assertTrue(im.is_valid(self.valid_issn_1))
        self.assertTrue(im.is_valid(self.valid_issn_2))
        self.assertFalse(im.is_valid(self.invalid_issn_1))
        self.assertFalse(im.is_valid(self.invalid_issn_2))
        self.assertFalse(im.is_valid(self.invalid_issn_3))

    def test_orcid_normalise(self):
        om = ORCIDManager()
        self.assertEqual(
            self.valid_orcid_1, om.normalise(self.valid_orcid_1.replace("-", "  "))
        )
        self.assertEqual(
            self.valid_orcid_1, om.normalise("https://orcid.org/" + self.valid_orcid_1)
        )
        self.assertEqual(
            self.valid_orcid_2, om.normalise(self.valid_orcid_2.replace("-", "  "))
        )
        self.assertEqual(
            self.invalid_orcid_3, om.normalise(self.invalid_orcid_3.replace("-", "  "))
        )

    def test_orcid_is_valid(self):
        om = ORCIDManager()
        self.assertTrue(om.is_valid(self.valid_orcid_1))
        self.assertTrue(om.is_valid(self.valid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_1))
        self.assertFalse(om.is_valid(self.invalid_orcid_2))
        self.assertFalse(om.is_valid(self.invalid_orcid_3))
        self.assertFalse(om.is_valid(self.invalid_orcid_4))

#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import unittest
from os import makedirs
from os.path import join, exists
from csv import DictReader

from oc_index.parsing.crowdsourced import CrowdsourcedParser
from oc_index.oci.citation import Citation


class CROCITest(unittest.TestCase):
    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")
        test_dir = join("tests", "data")
        self.input = join(test_dir, "croci_dump.csv")
        self.citations = join(test_dir, "croci_citations.csv")

    def test_citation_source(self):
        parser = CrowdsourcedParser()
        parser.parse(self.input)
        new = []
        cit = parser.get_next_citation_data()
        while cit is not None:
            citing, cited, citing_date, cited_date, journal_sc, author_sc = cit
            new.append(
                {
                    "citing": citing,
                    "cited": cited,
                    "creation": "" if citing_date is None else citing_date,
                    "timespan": ""
                    if cited_date is None
                    else Citation(
                        None,
                        None,
                        citing_date,
                        None,
                        cited_date,
                        None,
                        None,
                        None,
                        None,
                        "",
                        None,
                        None,
                        None,
                        None,
                        None,
                    ).duration,
                    "journal_sc": "no" if journal_sc is None else journal_sc,
                    "author_sc": "no" if author_sc is None else author_sc,
                }
            )
            cit = parser.get_next_citation_data()

        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))
        self.assertEqual(new, old)

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
from oc.index.parsing.crossref import CrossrefParser


class COCITest(unittest.TestCase):
    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")
        test_dir = join("index", "python", "test", "data")
        self.input = join(test_dir, "crossref_dump.json")
        self.citations = join(test_dir, "crossref_citations.csv")

    def test_citation_source(self):
        parser = CrossrefParser()
        parser.parse(self.input)
        new = []
        cit = parser.get_next_citation_data()
        while cit is not None:
            for citation_data in cit:
                citing, cited, creation, timespan, journal_sc, author_sc = citation_data
                new.append(
                    {
                        "citing": citing,
                        "cited": cited,
                        "creation": "" if creation is None else creation,
                        "timespan": "" if timespan is None else timespan,
                        "journal_sc": "" if journal_sc is None else journal_sc,
                        "author_sc": "" if author_sc is None else author_sc,
                    }
                )
            cit = parser.get_next_citation_data()

        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))

        self.assertCountEqual(new, old)

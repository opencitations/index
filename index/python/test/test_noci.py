#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import unittest
from os import makedirs
from os.path import join, exists
from os.path import join
from csv import DictReader
from oc.index.parsing.nih import NIHParser


class NOCITest(unittest.TestCase):
    """This class aims at testing the methods of the class NIHParser."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")
        test_dir = join("index", "python", "test", "data")
        self.input = join(test_dir, "noci_dump.csv")
        self.citations = join(test_dir, "noci_citations.csv")

    def test_citation_source(self):
        parser = NIHParser()
        parser.parse(self.input)
        new = []
        counter = 0
        cit = parser.get_next_citation_data()
        while cit is not None:
            # print("PROCESSING CIT N.", counter, ":", cit)
            citing, cited, creation, timespan, journal_sc, author_sc = cit
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
            counter += 1

        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))

        self.assertEqual(new, old)

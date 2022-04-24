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
from os import sep
from os.path import join
from csv import DictReader

from oc.index.parsing.crossref import CrossrefParser


class COCITest(unittest.TestCase):
    def setUp(self):
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

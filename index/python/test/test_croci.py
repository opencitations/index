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

import unittest
from os import sep
from os.path import join
from csv import DictReader

from oc.index.parsing.crowdsourced import CrowdsourcedParser
from oc.index.oci.citation import Citation


class CROCITest(unittest.TestCase):
    def setUp(self):
        test_dir = join("index", "python", "test", "data")
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

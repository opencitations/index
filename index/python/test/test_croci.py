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
import os
from os import makedirs
from os.path import join, exists
from csv import DictReader
from subprocess import Popen
from oc.index.parsing.crowdsourced import CrowdsourcedParser
from oc.index.oci.citation import Citation
import wget


class CROCITest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        if not exists("tmp"):
            makedirs("tmp")
        test_dir = join("index", "python", "test", "data")
        self.input = join(test_dir, "croci_dump.csv")
        self.citations = join(test_dir, "croci_citations.csv")
        # TODO: remove when meta is out
        if not os.path.isfile("blazegraph.jnl"):
            url = "https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar"
            wget.download(url=url, out=".")
        Popen(
            [
                "java",
                "-server",
                "-Xmx4g",
                "-Dcom.bigdata.journal.AbstractJournal.file=./blazegraph.jnl",
                f"-Djetty.port=9999",
                "-jar",
                f"./blazegraph.jar",
            ]
        )

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
                    "journal_sc": "" if journal_sc is None else journal_sc,
                    "author_sc": "" if author_sc is None else author_sc,
                }
            )
            cit = parser.get_next_citation_data()
        with open(self.citations, encoding="utf8") as f:
            old = list(DictReader(f))
        for i in range(len(new)):
            with self.subTest(i=i):
                self.assertEqual(new[i], old[i])

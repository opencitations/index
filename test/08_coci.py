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
from index.coci.glob import process
from os import sep, makedirs
from os.path import exists
from shutil import rmtree
from index.storer.csvmanager import CSVManager
from index.coci.crossrefcitationsource import CrossrefCitationSource
from csv import DictReader


class COCITest(unittest.TestCase):

    def setUp(self):
        self.input_dir = "index%stest_data%scrossref_dump" % (sep, sep)
        self.output_dir = "index%stest_data%scrossref_glob" % (sep, sep)
        self.citations = "index%stest_data%scrossref_dump%scitations.csv" % (sep, sep, sep)

    def test_glob(self):
        if exists(self.output_dir):
            rmtree(self.output_dir)
        makedirs(self.output_dir)

        process(self.input_dir, self.output_dir)

        orig = CSVManager(self.input_dir + sep + "valid_doi.csv")
        new = CSVManager(self.output_dir + sep + "valid_doi.csv")
        self.assertDictEqual(orig.data, new.data)

        orig = CSVManager(self.input_dir + sep + "id_date.csv")
        new = CSVManager(self.output_dir + sep + "id_date.csv")
        self.assertDictEqual(orig.data, new.data)

        orig = CSVManager(self.input_dir + sep + "id_issn.csv")
        new = CSVManager(self.output_dir + sep + "id_issn.csv")
        self.assertDictEqual(orig.data, new.data)

        orig = CSVManager(self.input_dir + sep + "id_orcid.csv")
        new = CSVManager(self.output_dir + sep + "id_orcid.csv")
        self.assertDictEqual(orig.data, new.data)

    def test_citation_source(self):
        ccs = CrossrefCitationSource(self.input_dir)
        new = []
        cit = ccs.get_next_citation_data()
        while cit is not None:
            citing, cited, creation, timespan, journal_sc, author_sc = cit
            new.append({
                "citing": citing,
                "cited": cited,
                "creation": "" if creation is None else creation,
                "timespan": "" if timespan is None else timespan,
                "journal_sc": "" if journal_sc is None else journal_sc,
                "author_sc": "" if author_sc is None else author_sc
            })
            cit = ccs.get_next_citation_data()

        with open(self.citations) as f:
            old = list(DictReader(f))

        self.assertEqual(new, old)

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
from os import sep, remove
from os.path import exists
from index.citation.citationsource import CSVFileCitationSource
from index.citation.oci import Citation, OCIManager
from urllib.parse import quote
from index.storer.citationstorer import CitationStorer


class CitationSourceTest(unittest.TestCase):
    """This class aim at testing the methods of the class CSVManager."""

    def setUp(self):
        info_file_path = "index%stest_data%stmp_store%sdata%s.dir_citation_source" % (sep, sep, sep, sep)
        if exists(info_file_path):
            remove(info_file_path)
        self.oci = OCIManager(lookup_file="index%stest_data%slookup_full.csv" % (sep, sep))
        self.citation_list = CitationStorer.load_citations_from_file(
            "index%stest_data%scitations_data.csv" % (sep, sep),
            "index%stest_data%scitations_prov.csv" % (sep, sep),
            baseurl="http://dx.doi.org/",
            service_name="OpenCitations Index: COCI", id_type="doi",
            id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None)

    def __create_citation(self, citing, cited, created, timespan, journal_sc, author_sc):
        return Citation(
            self.oci.get_oci(citing, cited, "020"),
            "http://dx.doi.org/" + quote(citing), None,
            "http://dx.doi.org/" + quote(cited), None,
            created, timespan,
            1, "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/" + quote(citing), "2018-01-01T00:00:00",
            "OpenCitations Index: COCI", "doi", "http://dx.doi.org/([[XXX__decode]])", None,
            journal_sc, author_sc, prov_description="Creation of the citation")

    def __citations_csv(self, origin_citation_list, stored_citation_list):
        l1 = [cit.get_citation_csv() for cit in origin_citation_list]
        l2 = [cit.get_citation_csv() for cit in stored_citation_list]
        self.assertEqual(len(l1), len(l2))
        self.assertEqual(set(l1), set(l2))

    def test_get_next_citation_data(self):
        cs = CSVFileCitationSource("index%stest_data%stmp_store%sdata" % (sep, sep, sep))
        citation_1 = self.__create_citation(*cs.get_next_citation_data())
        citation_2 = self.__create_citation(*cs.get_next_citation_data())
        self.__citations_csv(self.citation_list[:2], [citation_1, citation_2])

        cs = CSVFileCitationSource("index%stest_data%stmp_store%sdata" % (sep, sep, sep))
        citation_3 = self.__create_citation(*cs.get_next_citation_data())
        citation_4 = self.__create_citation(*cs.get_next_citation_data())
        citation_5 = self.__create_citation(*cs.get_next_citation_data())
        citation_6 = self.__create_citation(*cs.get_next_citation_data())
        self.__citations_csv(self.citation_list[2:6], [citation_3, citation_4, citation_5, citation_6])

        idx = 0
        while cs.get_next_citation_data() is not None:
            idx += 1
        self.assertEqual(idx, 6)

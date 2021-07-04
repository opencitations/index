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
from cnc import execute_workflow
from os.path import exists
from index.citation.citationsource import CSVFileCitationSource
from index.citation.oci import Citation, OCIManager
from index.storer.citationstorer import CitationStorer
from glob import glob
from shutil import rmtree


class CreateNewCitationsTest(unittest.TestCase):

    def setUp(self):
        self.idbaseurl = "http://dx.doi.org/"
        self.baseurl = "https://w3id.org/oc/index/coci/"
        self.pclass = "csv"
        self.input = "index%stest_data%scitations_partial.csv" % (sep, sep)
        self.doi_file = "index%stest_data%scnc_valid_doi.csv" % (sep, sep)
        self.date_file = "index%stest_data%scnc_id_date.csv" % (sep, sep)
        self.orcid_file = "index%stest_data%scnc_id_orcid.csv" % (sep, sep)
        self.issn_file = "index%stest_data%scnc_id_issn.csv" % (sep, sep)
        self.orcid = None
        self.lookup = "index%stest_data%slookup_full.csv" % (sep, sep)
        self.data = "index%stest_data%stmp_workflow" % (sep, sep)
        self.data_parallel = "index%stest_data%stmp_workflow_parallel" % (sep, sep)
        self.prefix = "020"
        self.agent = "https://w3id.org/oc/index/prov/ra/1"
        self.source = "https://api.crossref.org/works/[[citing]]"
        self.service = "OpenCitations Index: COCI"
        self.verbose = True
        self.no_api = False

        self.citation_list = self.__load_citations("index%stest_data%scitations_data.csv" % (sep, sep),
                                                   "index%stest_data%scitations_prov.csv" % (sep, sep))
        self.data_path = self.data + sep + "data" + sep + "**" + sep + "*.csv"
        self.prov_path = self.data + sep + "prov" + sep + "**" + sep + "*.csv"

        self.data_path_parallel = self.data_parallel + sep + "data" + sep + "**" + sep + "*.csv"
        self.prov_path_parallel = self.data_parallel + sep + "prov" + sep + "**" + sep + "*.csv"

        if exists(self.data):
            rmtree(self.data)
        
        if exists(self.data_parallel):
            rmtree(self.data_parallel)


    def __load_citations(self, data, prov):
        return CitationStorer.load_citations_from_file(data, prov, baseurl="http://dx.doi.org/",
            service_name=self.service, id_type="doi",
            id_shape="http://dx.doi.org/([[XXX__decode]])", citation_type=None)

    def __citations_csv(self, origin_citation_list, stored_citation_list):
        l1 = [cit.get_citation_csv() for cit in origin_citation_list]
        l2 = [cit.get_citation_csv() for cit in stored_citation_list]
        self.assertEqual(len(l1), len(l2))
        self.assertEqual(set(l1), set(l2))

    def __test_citations(self, d_path, p_path):
        data_csv = sorted(glob(d_path, recursive=True))
        prov_csv = sorted(glob(p_path, recursive=True))
        self.assertEqual(len(data_csv), len(prov_csv))

        stored_citation_list = []
        for i in range(len(data_csv)):
            data_file = data_csv[i]
            prov_file = prov_csv[i]
            cur_citations = self.__load_citations(data_file, prov_file)
            stored_citation_list.extend(cur_citations)

        self.__citations_csv(self.citation_list, stored_citation_list)

    def test_execute_workflow(self):
        new_citations_added, citations_already_present, error_in_dois_existence = \
            execute_workflow(self.idbaseurl, self.baseurl, self.pclass, self.input, self.doi_file,
                             self.date_file, self.orcid_file, self.issn_file, self.orcid, self.lookup, self.data,
                             self.prefix, self.agent, self.source, self.service, self.verbose, self.no_api, 1)
        self.assertEqual(new_citations_added, 6)
        self.assertEqual(citations_already_present, 0)
        self.assertEqual(error_in_dois_existence, 0)
        self.__test_citations(self.data_path, self.prov_path)

        new_citations_added, citations_already_present, error_in_dois_existence = \
            execute_workflow(self.idbaseurl, self.baseurl, self.pclass, self.input, self.doi_file,
                             self.date_file, self.orcid_file, self.issn_file, self.orcid, self.lookup, self.data,
                             self.prefix, self.agent, self.source, self.service, self.verbose, self.no_api, 1)
        self.assertEqual(new_citations_added, 0)
        self.assertEqual(citations_already_present, 6)
        self.assertEqual(error_in_dois_existence, 0)
        self.__test_citations(self.data_path, self.prov_path)


if __name__ == '__main__':
    unittest.main()  # Launch with "python text_cnc.py"

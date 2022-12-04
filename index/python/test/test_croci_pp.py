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
from os.path import join, exists
from oc.index.preprocessing.populator import *

class CROCIPPTest(unittest.TestCase):
    def setUp(self) -> None:
        self.author_pop = AuthorPopulator()
        self.id_pop = IDPopulator()
        self.metadata_pop = MetadataPopulator()
        self.input = []

    def test_author(self):
        result = []
        input = [{"id": {"doi":"10.1007/978-3-030-62466-8_28"}, "author":"Peroni, Silvio [orcid:0000-0003-0530-4305]", "title": "The OpenCitations Data Model"}, {"id": {"doi":"10.1371/journal.pone.0270872"},"author":"Heibi, Ivan", "title": "A protocol to gather, characterize and analyze incoming citations of retracted articles"},{"id":{"pmid":"19060306"}, "author":"Shotton, David [viaf:7484794]","title": "Linked data and provenance in biological data webs."}]
        for el in input:
            result.append(self.author_pop.get_author_info(el['id'],el))
        expected = ['Peroni, Silvio [orcid:0000-0003-0530-4305 viaf:309649450]', 'Heibi, Ivan [orcid:0000-0001-5366-5194]', 'Shotton, David [viaf:7484794 orcid:0000-0001-5506-523X]']
        for i in range(len(input)):
            with self.subTest(i=i):
                self.assertEqual(result[i], expected[i])

    def test_ids(self):
        result = []
        input = ["pmid:19060306","wikidata:Q46061806; pmid:18374409", "isbn:978-1-4724-2375-7", "doi:10.1038/NATURE13156"]
        expected = (({'pmid': '19060306', 'wikidata': 'Q33390322', 'doi': '10.1093/bib/bbn044'}, 0),
                    ({'wikidata': 'Q46061806', 'pmid': '18374409', 'doi': '10.1016/s0140-6736(08)60424-9'}, 1),
                    ({'isbn': '978-1-4724-2375-7', 'wikidata': 'Q55829735'}, 2), 
                    ({'doi': '10.1038/nature13156', 'wikidata': 'Q28051867', 'pmid': '24670765'}, 3))
        for el in input:
            result.append(self.id_pop.populate_ids(el))
        for i in range(len(input)):
            with self.subTest(i=i):
                self.assertEqual(result[i], expected[i])

    def test_metadata(self):
        result = []
        input = [{"doi":"10.5281/zenodo.6913873"},{"wikidata":"Q33626815"},{"doi":"10.25333/BGFG-D241"}]
        expected = [{'id': 'doi:10.5281/zenodo.6913873', 'type': 'dataset', 'publisher': 'Zenodo', 'author': 'Brembilla, Davide [orcid:0000-0002-9481-5053]; Catizone, Chiara [orcid:0000-0003-2445-2426]; Venditti, Giulia [orcid:0000-0001-7696-7574]', 'title': 'La Chouffe Dataset', 'pub_date': '2022', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'editor': ''},
        {'author': 'Orav, John; Orav, Endel; Middleton, Blackford', 'venue': 'AMIA Annual Symposium proceedings', 'pub_date': '2009', 'title': 'Survey analysis of patient experience using a practice-linked PHR for type 2 diabetes mellitus', 'volume': '2009', 'page': '678-682', 'id': 'wikidata:Q33626815', 'type': 'journal article', 'issue': '', 'publisher': '', 'editor': ''},
        {'id': 'doi:10.25333/BGFG-D241', 'type': 'other', 'publisher': 'OCLC Research', 'author': 'Bryant, Rebecca; Clements, Anna; De Castro, Pablo; Cantrell, Joanne; Dortmund, Annette; Fransen, Jan; Gallagher, Peggy; Mennielli, Michele', 'title': 'Practices and Patterns in Research Information Management: Findings from a Global Survey', 'pub_date': '2018', 'venue': '', 'volume': '', 'issue': '', 'page': '', 'editor': ''},
        ]
        for el in input:
            result.append(self.metadata_pop.launch_service(el))
        for i in range(len(result)):
            with self.subTest(i=i):
                self.assertEqual(result[i],expected[i])


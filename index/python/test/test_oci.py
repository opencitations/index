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
from os import remove
from os.path import exists, join
from csv import DictReader
from json import load, loads
from rdflib import ConjunctiveGraph
from io import StringIO
from rdflib.compare import isomorphic
from rdflib.term import _toPythonMapping
from rdflib import XSD
from re import findall

from oc.index.oci.citation import Citation, OCIManager


class CitationTest(unittest.TestCase):
    """This class aim at testing the methods of the class
    belongs to package oc.index.oci"""

    def setUp(self):
        test_dir = join("index", "python", "test", "data")
        self.citation_data_csv_path = join(test_dir, "citations_data.csv")
        self.citation_prov_csv_path = join(test_dir, "citations_prov.csv")
        self.citation_data_ttl_path = join(test_dir, "citations_data.ttl")
        self.citation_prov_ttl_path = join(test_dir, "citations_prov.ttl")
        self.citation_data_prov_scholix_path = join(
            test_dir, "citations_data_prov.scholix"
        )
        self.base_url = "https://w3id.org/oc/index/coci/"
        self.citation_1 = Citation(
            "02001000308362819371213133704040001020809-020010009063615193700006300030306151914",
            "http://dx.doi.org/10.1038/sj.cdd.4401289",
            "2003-10-24",
            "http://dx.doi.org/10.1096/fj.00-0336fje",
            "2001-01",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1038/sj.cdd.4401289",
            "2018-11-01T09:14:03",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )

        self.citation_2 = Citation(
            "02001000002361927283705040000-02001000002361927283705030002",
            "http://dx.doi.org/10.1002/jrs.5400",
            "2018-06",
            "http://dx.doi.org/10.1002/jrs.5302",
            "2017-12-05",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1002/jrs.5400",
            "2018-11-01T14:51:52",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=True,
            author_sc=True,
            prov_description="Creation of the citation",
        )

        self.citation_3 = Citation(
            "02001000002361927283705040000-020010003093612062710020603000720",
            "http://dx.doi.org/10.1002/jrs.5400",
            "2018-06",
            "http://dx.doi.org/10.1039/c6ra26307k",
            "2017",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1002/jrs.5400",
            "2018-11-01T14:51:52",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=True,
            prov_description="Creation of the citation",
        )

        self.citation_4 = Citation(
            "02001000308362819371213133704040001020804-02001000308362819371213133704040000030707",
            "http://dx.doi.org/10.1038/sj.cdd.4401284",
            "2003-08-22",
            "http://dx.doi.org/10.1038/sj.cdd.4400377",
            "1998-05",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1038/sj.cdd.4401284",
            "2018-11-01T09:14:03",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=True,
            author_sc=False,
            prov_description="Creation of the citation",
        )

        self.citation_5 = Citation(
            "020010000023625242110370100030001-02001010009361222251430273701090809370903040403",
            "http://dx.doi.org/10.1002/pola.10301",
            "2002-06-21",
            "http://dx.doi.org/10.1109/cmpeur.1989.93443",
            None,
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1002/pola.10301",
            "2018-10-31T16:13:26",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )

        self.citation_6 = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            None,
            "http://dx.doi.org/10.1134/s1028334x09040217",
            None,
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )

        # Hack for correct handling of date datatypes
        if XSD.gYear in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYear)
        if XSD.gYearMonth in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYearMonth)

    def test_inferred_leap_year_dates(self):
        cit = Citation(
            None,
            "http://dx.doi.org/10.1002/1097-0142%2820010815%2992%3A4%3C796%3A%3Aaid-cncr1385%3E3.0.co%3B2-3",
            "2001",
            "http://dx.doi.org/10.1002/%28sici%291097-0258%2819960229%2915%3A4%3C361%3A%3Aaid-sim168%3E3.0.co%3B2-4",
            "1996-02-29",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1002/1097-0142%2820010815%2992%3A4%3C796%3A%3Aaid-cncr1385%3E3.0.co%3B2-3",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertEqual(cit.duration, "P5Y")

    def test_invalid_date_for_citation(self):
        cit = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            "0000",
            "http://dx.doi.org/10.1134/s1028334x09040217",
            None,
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertIsNone(cit.citing_pub_date)
        self.assertIsNone(cit.creation_date)
        self.assertIsNone(cit.cited_pub_date)
        self.assertIsNone(cit.duration)

        cit = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            "2019",
            "http://dx.doi.org/10.1134/s1028334x09040217",
            "0000",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertIsNotNone(cit.citing_pub_date)
        self.assertIsNotNone(cit.creation_date)
        self.assertIsNone(cit.cited_pub_date)
        self.assertIsNone(cit.duration)

        cit = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            None,
            "http://dx.doi.org/10.1134/s1028334x09040217",
            "2011",
            None,
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertIsNone(cit.citing_pub_date)
        self.assertIsNone(cit.creation_date)
        self.assertIsNotNone(cit.cited_pub_date)
        self.assertIsNone(cit.duration)

        cit = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            None,
            "http://dx.doi.org/10.1134/s1028334x09040217",
            "2011",
            "2019",
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertIsNotNone(cit.citing_pub_date)
        self.assertIsNotNone(cit.creation_date)
        self.assertIsNotNone(cit.cited_pub_date)
        self.assertIsNotNone(cit.duration)

        cit = Citation(
            "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107",
            "http://dx.doi.org/10.1130/2015.2513%2800%29",
            None,
            "http://dx.doi.org/10.1134/s1028334x09040217",
            "2011",
            "2019-02-29",
            None,
            1,
            "https://w3id.org/oc/index/prov/ra/1",
            "https://api.crossref.org/works/10.1130/2015.2513%2800%29",
            "2018-10-31T16:17:07",
            "OpenCitations Index: COCI",
            "doi",
            "http://dx.doi.org/([[XXX__decode]])",
            None,
            journal_sc=False,
            author_sc=False,
            prov_description="Creation of the citation",
        )
        self.assertIsNone(cit.citing_pub_date)
        self.assertIsNone(cit.creation_date)
        self.assertIsNotNone(cit.cited_pub_date)
        self.assertIsNone(cit.duration)

    def test_citation_data_csv(self):
        citation_data_csv = None

        with open(self.citation_data_csv_path, encoding="utf8") as f:
            citation_data_csv = list(DictReader(f))

        self.assertEqual(
            list(DictReader(StringIO(self.citation_1.get_citation_csv())))[0],
            citation_data_csv[0],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_2.get_citation_csv())))[0],
            citation_data_csv[1],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_3.get_citation_csv())))[0],
            citation_data_csv[2],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_4.get_citation_csv())))[0],
            citation_data_csv[3],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_5.get_citation_csv())))[0],
            citation_data_csv[4],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_6.get_citation_csv())))[0],
            citation_data_csv[5],
        )

    def test_citation_prov_csv(self):
        citation_prov_csv = None

        with open(self.citation_prov_csv_path, encoding="utf8") as f:
            citation_prov_csv = list(DictReader(f))

        self.assertEqual(
            list(DictReader(StringIO(self.citation_1.get_citation_prov_csv())))[0],
            citation_prov_csv[0],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_2.get_citation_prov_csv())))[0],
            citation_prov_csv[1],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_3.get_citation_prov_csv())))[0],
            citation_prov_csv[2],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_4.get_citation_prov_csv())))[0],
            citation_prov_csv[3],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_5.get_citation_prov_csv())))[0],
            citation_prov_csv[4],
        )
        self.assertEqual(
            list(DictReader(StringIO(self.citation_6.get_citation_prov_csv())))[0],
            citation_prov_csv[5],
        )

    def test_citation_data_ttl(self):
        g1 = ConjunctiveGraph()
        # Changed from load to parse since load has been deprecated
        g1.parse(self.citation_data_ttl_path, format="nt11")

        g2 = ConjunctiveGraph()
        for c in [
            self.citation_1,
            self.citation_2,
            self.citation_3,
            self.citation_4,
            self.citation_5,
            self.citation_6,
        ]:
            for s, p, o in c.get_citation_rdf(self.base_url, False, False, False):
                g2.add((s, p, o))

        self.assertTrue(isomorphic(g1, g2))

    def test_citation_prov_ttl(self):
        g1 = ConjunctiveGraph()
        # Changed from load to parse since load has been deprecated
        g1.parse(self.citation_prov_ttl_path, format="nquads")

        g2 = ConjunctiveGraph()
        for c in [
            self.citation_1,
            self.citation_2,
            self.citation_3,
            self.citation_4,
            self.citation_5,
            self.citation_6,
        ]:
            for s, p, o, g in c.get_citation_prov_rdf(self.base_url).quads(
                (None, None, None, None)
            ):
                g2.add((s, p, o, g))

        self.assertTrue(isomorphic(g1, g2))

    def test_citation_data_prov_scholix(self):
        citation_data_prov_scholix = None

        with open(self.citation_data_prov_scholix_path, encoding="utf8") as f:
            citation_data_prov_scholix = load(f)

        self.assertEqual(
            loads(self.citation_1.get_citation_scholix()), citation_data_prov_scholix[0]
        )
        self.assertEqual(
            loads(self.citation_2.get_citation_scholix()), citation_data_prov_scholix[1]
        )
        self.assertEqual(
            loads(self.citation_3.get_citation_scholix()), citation_data_prov_scholix[2]
        )
        self.assertEqual(
            loads(self.citation_4.get_citation_scholix()), citation_data_prov_scholix[3]
        )
        self.assertEqual(
            loads(self.citation_5.get_citation_scholix()), citation_data_prov_scholix[4]
        )
        self.assertEqual(
            loads(self.citation_6.get_citation_scholix()), citation_data_prov_scholix[5]
        )

    def test_lookup(self):
        doi_1 = "10.1038/sj.cdd.4401289"
        doi_2 = "10.1096/fj.00-0336fje"
        doi_3 = "10.1002/jrs.5400"
        doi_4 = "10.1039/c6ra26307k"
        doi_5 = "10.1234/456789qwertyuiopasdfghjklzxcvbnmè+òàù,.-åß∂ƒ∞∆ªº¬∑≤†©√∫˜≥»”’¢‰"
        doi_6 = '10.1234/!"£$%&/()=?^é*ç°§;:_<>«“‘¥~‹÷´`￿ˆ[]@#¶…•–„Ω€®™æ¨øπ'

        # Test conversion without any file
        oci_man = OCIManager()
        oci = oci_man.get_oci(doi_1, doi_2, "020")
        self.assertEqual(
            doi_1.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[0].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            doi_2.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[1].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(len(oci_man.lookup.keys()), len(set(doi_1 + doi_2)))

        test_dir = join("index", "python", "test", "data")

        # Test conversion with full file
        oci_12 = "oci:02001000308362819371213133704040001020809-020010009063615193700006300030306151914"
        oci_man = OCIManager(lookup_file=join(test_dir, "lookup_full.csv"))
        self.assertEqual(oci_man.get_oci(doi_1, doi_2, "020"), oci_12)

        # Test conversion with new file
        new_file_path = join(test_dir, "lookup_new.csv")
        if exists(new_file_path):
            remove(new_file_path)
        oci_man = OCIManager(lookup_file=new_file_path)
        oci = oci_man.get_oci(doi_1, doi_2, "020")
        self.assertEqual(
            doi_1.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[0].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            doi_2.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[1].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(len(oci_man.lookup.keys()), len(set(doi_1 + doi_2)))

        # Test conversion with incomplete file (ver 1, existing DOIs)
        oci_man = OCIManager(lookup_file=new_file_path)
        oci = oci_man.get_oci(doi_3, doi_4, "020")
        self.assertEqual(
            doi_3.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[0].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            doi_4.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[1].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            len(oci_man.lookup.keys()), len(set(doi_1 + doi_2 + doi_3 + doi_4))
        )

        # Test conversion with incomplete file (ver 2, non-existing DOIs)
        oci_man = OCIManager(lookup_file=new_file_path)
        oci = oci_man.get_oci(doi_5, doi_6, "020")
        self.assertEqual(
            doi_5.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[0].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            doi_6.replace("10.", "", 1),
            "".join(
                [
                    oci_man.lookup[code]
                    for code in findall(
                        "(9*[0-8][0-9])",
                        oci.replace("oci:", "").split("-")[1].replace("020", "", 1),
                    )
                ]
            ),
        )
        self.assertEqual(
            len(oci_man.lookup.keys()),
            len(set(doi_1 + doi_2 + doi_3 + doi_4 + doi_5 + doi_6)),
        )

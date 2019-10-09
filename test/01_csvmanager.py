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
from index.storer.csvmanager import CSVManager


class CSVManagerTest(unittest.TestCase):
    """This class aim at testing the methods of the class CSVManager."""

    def setUp(self):
        self.initial_path = "index%stest_data%sinitial_data.csv" % (sep, sep)
        self.addition_path = "index%stest_data%sadditional_data.csv" % (sep, sep)
        self.citation_path = "index%stest_data%scitations_data.csv" % (sep, sep)

    def test_creation(self):
        csv_m = CSVManager(self.initial_path)
        self.assertDictEqual(csv_m.data, {
            "doi:10.1108/jd-12-2013-0166": {"2015-03-09"},
            "doi:10.7717/peerj.4375": {"2018-02-13"}
        })

    def test_get_value(self):
        csv_m = CSVManager(self.initial_path)
        retrieved_1 = csv_m.get_value("doi:10.1108/jd-12-2013-0166")
        self.assertEqual({"2015-03-09"}, retrieved_1)

        retrieved_2 = csv_m.get_value("doi:10.1108/jd-12-2013-0167")
        self.assertIsNone(retrieved_2)

    def test_add_value(self):
        if exists(self.addition_path):
            remove(self.addition_path)

        csv_m = CSVManager(self.addition_path)
        csv_m.add_value("doi:10.1108/jd-12-2013-0166", "orcid:0000-0003-0530-4305")
        csv_m.add_value("doi:10.7717/peerj.4375", "orcid:0000-0003-1613-5981")
        csv_m.add_value("doi:10.1108/jd-12-2013-0166", "orcid:0000-0001-5506-523X")

        self.assertDictEqual(csv_m.data, {
            "doi:10.1108/jd-12-2013-0166": {"orcid:0000-0003-0530-4305", "orcid:0000-0001-5506-523X"},
            "doi:10.7717/peerj.4375": {"orcid:0000-0003-1613-5981"}
        })

    def test_load_csv_column_as_set(self):
        oci_set = CSVManager.load_csv_column_as_set(self.citation_path, "oci", 4)
        self.assertSetEqual(oci_set,
                            {"02001000308362819371213133704040001020809-020010009063615193700006300030306151914",
                             "02001000002361927283705040000-02001000002361927283705030002",
                             "02001000002361927283705040000-020010003093612062710020603000720",
                             "02001000308362819371213133704040001020804-02001000308362819371213133704040000030707",
                             "020010000023625242110370100030001-02001010009361222251430273701090809370903040403",
                             "020010103003602000105370205010358000059-02001010304362801000208030304330009000400020107"})

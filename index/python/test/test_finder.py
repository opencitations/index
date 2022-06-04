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
import json

from os import makedirs
from os.path import join, exists

from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.finder.nih import NIHResourceFinder
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.base import ResourceFinderHandler


class ResourceFinderTest(unittest.TestCase):
    """This class aim at testing resource finders."""

    def setUp(self):
        if not exists("tmp"):
            makedirs("tmp")
        test_dir = join("index", "python", "test", "data")
        with open(join(test_dir, "glob.json"), encoding="utf-8") as fp:
            self.data = json.load(fp)

    def test_handler_get_date(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder()]
        )
        self.assertEqual("2019-05-27", handler.get_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual("2019-05-27", handler.get_date("10.1108/jd-12-2013-0166"))

    def test_handler_share_issn(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder()]
        )
        share_issn, _, __ = handler.share_issn(
            "10.1007/s11192-018-2988-z", "10.1007/s11192-015-1565-y"
        )
        self.assertTrue(share_issn)
        share_issn, _, __ = handler.share_issn(
            "10.1007/s11192-018-2988-z", "10.6092/issn.2532-8816/8555"
        )
        self.assertFalse(share_issn)

    def test_handler_share_orcid(self):
        handler = ResourceFinderHandler(
            [CrossrefResourceFinder(), DataCiteResourceFinder(), ORCIDResourceFinder()]
        )
        share_orcid, _, __ = handler.share_orcid(
            "10.1007/s11192-018-2988-z", "10.5281/zenodo.3344898"
        )
        self.assertTrue(share_orcid)
        share_orcid, _, __ = handler.share_orcid(
            "10.1007/s11192-018-2988-z", "10.1007/s11192-015-1565-y5"
        )
        self.assertFalse(share_orcid)

    def test_orcid_get_orcid(self):
        # Do not use support dict, only APIs
        of_1 = ORCIDResourceFinder()
        self.assertIn("0000-0003-0530-4305", of_1.get_orcid("10.1108/jd-12-2013-0166"))
        self.assertNotIn(
            "0000-0001-5506-523X", of_1.get_orcid("10.1108/jd-12-2013-0166")
        )

        # Do use support dict, but avoid using APIs
        of_2 = ORCIDResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("0000-0003-0530-4305", of_2.get_orcid("10.1108/jd-12-2013-0166"))
        self.assertNotIn(
            "0000-0001-5506-523X", of_2.get_orcid("10.1108/jd-12-2013-0166")
        )

        # Do not use support files neither APIs
        of_3 = ORCIDResourceFinder(use_api_service=False)
        self.assertIsNone(of_3.get_orcid("10.1108/jd-12-2013-0166"))

    def test_datacite_get_orcid(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("0000-0001-7734-8388", df_1.get_orcid("10.5065/d6b8565d"))
        self.assertNotIn("0000-0001-5506-523X", df_1.get_orcid("10.5065/d6b8565d"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("0000-0001-7734-8388", df_2.get_orcid("10.5065/d6b8565d"))
        self.assertNotIn("0000-0001-5506-523X", df_2.get_orcid("10.5065/d6b8565d"))

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_orcid("10.5065/d6b8565d"))

    def test_datacite_get_issn(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("1406-894X", df_1.get_container_issn("10.15159/ar.21.030"))
        self.assertNotIn("1588-2861", df_1.get_container_issn("10.15159/ar.21.030"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("2197-6775", df_2.get_container_issn("10.14763/2019.1.1389"))
        self.assertNotIn("1588-2861", df_2.get_container_issn("10.14763/2019.1.1389"))

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_container_issn("10.14763/2019.1.1389"))

    def test_datacite_get_pub_date(self):
        # Do not use support files, only APIs
        df_1 = DataCiteResourceFinder()
        self.assertIn("2019-05-27", df_1.get_pub_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual("2019", df_1.get_pub_date("10.6092/issn.2532-8816/8555"))

        # Do use support files, but avoid using APIs
        df_2 = DataCiteResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("2019-05-27", df_2.get_pub_date("10.6092/issn.2532-8816/8555"))
        self.assertNotEqual(
            "2018-01-02", df_2.get_pub_date("10.6092/issn.2532-8816/8555")
        )

        # Do not use support files neither APIs
        df_3 = DataCiteResourceFinder(use_api_service=False)
        self.assertIsNone(df_3.get_pub_date("10.6092/issn.2532-8816/8555"))

    def test_crossref_get_orcid(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn(
            "0000-0003-0530-4305", cf_1.get_orcid("10.1007/s11192-018-2988-z")
        )
        self.assertNotIn(
            "0000-0001-5506-523X", cf_1.get_orcid("10.1007/s11192-018-2988-z")
        )

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn(
            "0000-0003-0530-4305", cf_2.get_orcid("10.1007/s11192-018-2988-z")
        )
        self.assertNotIn(
            "0000-0001-5506-523X", cf_2.get_orcid("10.1007/s11192-018-2988-z")
        )

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_orcid("10.1007/s11192-018-2988-z"))

    def test_crossref_get_issn(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn("0138-9130", cf_1.get_container_issn("10.1007/s11192-018-2988-z"))
        self.assertNotIn(
            "0138-9000", cf_1.get_container_issn("10.1007/s11192-018-2988-z")
        )

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("1588-2861", cf_2.get_container_issn("10.1007/s11192-018-2988-z"))
        self.assertNotIn(
            "0138-9000", cf_2.get_container_issn("10.1007/s11192-018-2988-z")
        )

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_container_issn("10.1007/s11192-018-2988-z"))

    def test_crossref_get_pub_date(self):
        # Do not use support files, only APIs
        cf_1 = CrossrefResourceFinder()
        self.assertIn("2019-01-02", cf_1.get_pub_date("10.1007/s11192-018-2988-z"))
        self.assertNotEqual("2019", cf_1.get_pub_date("10.1007/s11192-018-2988-z"))

        # Do use support files, but avoid using APIs
        cf_2 = CrossrefResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("2019-01-02", cf_2.get_pub_date("10.1007/s11192-018-2988-z"))
        self.assertNotEqual(
            "2018-01-02", cf_2.get_pub_date("10.1007/s11192-018-2988-z")
        )

        # Do not use support files neither APIs
        cf_3 = CrossrefResourceFinder(use_api_service=False)
        self.assertIsNone(cf_3.get_pub_date("10.1007/s11192-018-2988-z"))

    def test_nationalinstititeofhealth_get_orcid(self):
        #Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertNotIn("0000-0002-1825-0097", nf_1.get_orcid("29998776"))
        self.assertNotIn("0000-0002-1825-0097", nf_1.get_orcid("7189714"))
        self.assertEqual([], nf_1.get_orcid("29998776"))
        self.assertEqual([], nf_1.get_orcid("1509982"))

        # Do use support files, but avoid using APIs
        nf_2 = NIHResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("0000-0001-8403-9735", nf_2.get_orcid("29998776"))
        self.assertNotIn("0000-0002-1825-0097", nf_2.get_orcid("1509982"))

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_orcid("7189714"))

    def test_nationalinstititeofhealth_get_issn(self):
        # Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertIn("0003-4819", nf_1.get_container_issn("2942070"))
        self.assertNotIn("0003-0000", nf_1.get_container_issn("2942070"))

        # Do use support files, but avoid using APIs
        nf_2 = NIHResourceFinder(
            self.data,
            use_api_service=False,
        )
        container_issn = nf_2.get_container_issn("1509982")
        self.assertIn("0065-4299", container_issn)
        self.assertNotIn("0065-4444", container_issn)

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_container_issn("7189714"))

    def test_nationalinstititeofhealth_get_pub_date(self):
        # Do not use support files, only APIs
        nf_1 = NIHResourceFinder()
        self.assertIn("1998-05-25", nf_1.get_pub_date("9689714"))
        self.assertNotEqual("1998", nf_1.get_pub_date("9689714"))

        # Do use support files, but avoid using APIs
        nf_2 = NIHResourceFinder(
            self.data,
            use_api_service=False,
        )
        self.assertIn("1980-06", nf_2.get_pub_date("7189714"))
        self.assertNotEqual("1980-06-22", nf_2.get_pub_date("7189714"))

        # Do not use support files neither APIs
        nf_3 = NIHResourceFinder(use_api_service=False)
        self.assertIsNone(nf_3.get_pub_date("2942070"))

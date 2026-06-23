#!python

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import unittest

import fakeredis

from oc_index.scripts import cnc


class SetCitsTest(unittest.TestCase):
    def setUp(self):
        self.fake_server = fakeredis.FakeServer()
        self.redis_br = fakeredis.FakeRedis(
            server=self.fake_server, db=10, decode_responses=True
        )
        self.had_redis_br = hasattr(cnc, "redis_br")
        self.original_redis_br = getattr(cnc, "redis_br", None)
        cnc.redis_br = self.redis_br

    def tearDown(self):
        if self.had_redis_br:
            cnc.redis_br = self.original_redis_br
        elif hasattr(cnc, "redis_br"):
            delattr(cnc, "redis_br")

    def test_set_cits_reads_meta2redis_set_values(self):
        self.redis_br.sadd("doi:10.1234/citing", "omid:br/0601")
        self.redis_br.sadd("doi:10.1234/cited", "omid:br/0602")

        result = cnc.set_cits([("doi:10.1234/citing", "doi:10.1234/cited")])

        assert result == {"0601-0602": ("omid:br/0601", "omid:br/0602")}

    def test_set_cits_expands_multiple_omids(self):
        self.redis_br.sadd("doi:10.1234/citing", "omid:br/0601", "omid:br/0603")
        self.redis_br.sadd("doi:10.1234/cited", "omid:br/0602", "omid:br/0604")

        result = cnc.set_cits([("doi:10.1234/citing", "doi:10.1234/cited")])

        assert result == {
            "0601-0602": ("omid:br/0601", "omid:br/0602"),
            "0601-0604": ("omid:br/0601", "omid:br/0604"),
            "0603-0602": ("omid:br/0603", "omid:br/0602"),
            "0603-0604": ("omid:br/0603", "omid:br/0604"),
        }

    def test_set_cits_skips_missing_omids(self):
        self.redis_br.sadd("doi:10.1234/citing", "omid:br/0601")

        result = cnc.set_cits([("doi:10.1234/citing", "doi:10.1234/missing")])

        assert result == {}

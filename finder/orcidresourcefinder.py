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

from index.finder.resourcefinder import ApiDOIResourceFinder
from requests import get
from urllib.parse import quote
from json import loads
import index.support.dictionary as sd
from datetime import datetime


class ORCIDResourceFinder(ApiDOIResourceFinder):
    def __init__(self, date=None, orcid=None, issn=None, doi=None, use_api_service=True, key=None):
        self.key = key
        self.use_api_service = use_api_service
        self.api = "https://pub.orcid.org/v2.1/search?q="
        super(ORCIDResourceFinder, self).__init__(date=date, orcid=orcid, issn=issn, doi=doi,
                                                  use_api_service=use_api_service)

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            for item in json_obj:
                orcid = item.get("orcid-identifier")
                if orcid is not None:
                    orcid_norm = self.om.normalise(orcid["path"])
                    if orcid_norm is not None:
                        result.add(orcid_norm)

        return result

    def _call_api(self, doi_full):
        if self.use_api_service:
            if self.key is not None:
                self.headers["Authorization"] = "Bearer %s" % self.key
            self.headers["Content-Type"] = "application/json"

            doi = self.dm.normalise(doi_full)
            r = get(self.api + quote("doi-self:\"%s\" OR doi-self:\"%s\"" % (doi, doi.upper())),
                    headers=self.headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                return json_res.get("result")

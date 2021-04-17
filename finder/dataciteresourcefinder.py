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
from json import loads
from urllib.parse import quote
from requests import get
import index.support.dictionary as sd


class DataCiteResourceFinder(ApiDOIResourceFinder):
    def __init__(self, date=None, orcid=None, issn=None, doi=None, use_api_service=True):
        self.api = "https://api.datacite.org/dois/"
        self.use_api_service = use_api_service
        super(DataCiteResourceFinder, self).__init__(date=date, orcid=orcid, issn=issn, doi=doi,
                                                     use_api_service=use_api_service)

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            authors = json_obj.get("creators")
            if authors is not None:
                for author in authors:
                    author_ids = author.get("nameIdentifiers")
                    if author_ids is not None:
                        for author_id in author_ids:
                            if sd.contains(author_id, "nameIdentifierScheme", "ORCID"):
                                orcid = self.om.normalise(author_id.get("nameIdentifier"))
                                if orcid is not None:
                                    result.add(orcid)

        return result

    def _get_issn(self, json_obj):
        result = set()
        
        if json_obj is not None:
            obj_types = json_obj.get("types")
            if obj_types is not None and sd.contains(obj_types, "citeproc", "journal"):
                container = json_obj.get("container")
                if container is not None and sd.contains(container, "identifierType", "ISSN"):
                    issn = self.im.normalise(container.get("identifier"))
                    if issn is not None:
                        result.add(issn)

        return result

    def _get_date(self, json_obj):
        if json_obj is not None:
            cur_date = None
            dates = json_obj.get("dates")
            for date in dates:
                if date.get("dateType") == "Issued":
                    cur_date = date.get("date")

            if cur_date is None:
                cur_date = json_obj.get("publicationYear")
                if cur_date is not None:
                    cur_date = str(cur_date)

            return cur_date

    def _call_api(self, doi_entity):
        if self.use_api_service:
            doi = self.dm.normalise(doi_entity)
            r = get(self.api + quote(doi), headers=self.headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                root = json_res.get("data")
                if root is not None:
                    return root.get("attributes")

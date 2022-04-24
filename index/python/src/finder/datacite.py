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

from json import loads
from urllib.parse import quote
from requests import get

import oc.index.utils.dictionary as dict_utils
from oc.index.finder.base import ApiDOIResourceFinder


class DataCiteResourceFinder(ApiDOIResourceFinder):
    """This class implements an identifier manager for data cite identifier"""

    def __init__(self, data={}, use_api_service=True):
        """Data cite resource finder constructor."""
        super().__init__(data, use_api_service=use_api_service)
        self._api = "https://api.datacite.org/dois/"

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            authors = json_obj.get("creators")
            if authors is not None:
                for author in authors:
                    author_ids = author.get("nameIdentifiers")
                    if author_ids is not None:
                        for author_id in author_ids:
                            if dict_utils.contains(
                                author_id, "nameIdentifierScheme", "ORCID"
                            ):
                                orcid = self._om.normalise(
                                    author_id.get("nameIdentifier")
                                )
                                if orcid is not None:
                                    result.add(orcid)

        return result

    def _get_issn(self, json_obj):
        result = set()

        if json_obj is not None:
            obj_types = json_obj.get("types")
            if obj_types is not None and dict_utils.contains(
                obj_types, "citeproc", "journal"
            ):
                container = json_obj.get("container")
                if container is not None and dict_utils.contains(
                    container, "identifierType", "ISSN"
                ):
                    issn = self._im.normalise(container.get("identifier"))
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
        if self._use_api_service:
            doi = self._dm.normalise(doi_entity)
            r = get(self._api + quote(doi), headers=self._headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                root = json_res.get("data")
                if root is not None:
                    return root.get("attributes")

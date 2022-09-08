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

from requests import get
from datetime import datetime

from urllib.parse import quote

import oc.index.utils.dictionary as dict_utils
from oc.index.finder.base import ApiDOIResourceFinder


class CrossrefResourceFinder(ApiDOIResourceFinder):
    """This class implements an api doi resource finder for crossref"""

    def __init__(self, data={}, use_api_service=True):
        """Crossref resource finder constructor."""
        super().__init__(data, use_api_service)
        self._api = "https://api.crossref.org/works/"

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is None:
            return result
        for author in json_obj.get("author", []):
            orcid = self._om.normalise(author.get("ORCID"))
            if orcid is not None:
                result.add(orcid)

        return result

    def _get_issn(self, json_obj):
        result = set()

        if json_obj is None:
            return result
        if not dict_utils.contains(json_obj, "type", "journal"):
            return result
        for issn in json_obj.get("ISSN", []):
            norm_issn = self._im.normalise(issn)
            if norm_issn is not None:
                result.add(norm_issn)

        return result

    def _get_date(self, json_obj):
        if json_obj is None:
            return
        date = json_obj.get("issued")
        if not date:
            return
        date_list = date["date-parts"][0]
        if date_list is None:
            return
        l_date_list = len(date_list)
        if l_date_list != 0 and date_list[0] is not None:
            if l_date_list == 3 and (
                (date_list[1] is not None and date_list[1] != 1)
                or (date_list[2] is not None and date_list[2] != 1)
            ):
                result = datetime(
                    date_list[0], date_list[1], date_list[2], 0, 0
                ).strftime("%Y-%m-%d")
            elif l_date_list == 2 and date_list[1] is not None:
                result = datetime(date_list[0], date_list[1], 1, 0, 0).strftime("%Y-%m")
            else:
                result = datetime(date_list[0], 1, 1, 0, 0).strftime("%Y")

            return result

    def _call_api(self, doi_full):
        if self._use_api_service:
            doi = self._dm.normalise(doi_full)
            r = get(self._api + quote(doi), headers=self._headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                return r.json().get("message")

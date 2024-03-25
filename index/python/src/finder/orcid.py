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
from urllib.parse import quote
from json import loads


from oc.index.finder.base import ApiDOIResourceFinder


class ORCIDResourceFinder(ApiDOIResourceFinder):
    """This class implements an identifier manager for orcid identifier"""

    def __init__(self, data={}, use_api_service=True, api_key=None):
        """ORCID resource finder constructor.

        Args:
            date (str, optional): path to date file. Defaults to None.
            orcid (str, optional): path to orcid file. Defaults to None.
            issn (str, optional): path to issn file. Defaults to None.
            doi (str, optional): path to doi file. Defaults to None.
            use_api_service (bool, optional): true if you want to use api service. Defaults to True.
            key (str, optional): api key. Defaults to None.
        """
        super().__init__(data, use_api_service=use_api_service)
        self._api = "https://pub.orcid.org/v2.1/search?q="
        self._api_key = api_key

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            for item in json_obj:
                orcid = item.get("orcid-identifier")
                if orcid is not None:
                    orcid_norm = self._om.normalise(orcid["path"])
                    if orcid_norm is not None:
                        result.add(orcid_norm)

        return result

    def _call_api(self, doi_full):
        if self._use_api_service:
            if self._api_key is not None:
                self._headers["Authorization"] = "Bearer %s" % self._api_key
            self._headers["Content-Type"] = "application/json"

            doi = self._dm.normalise(doi_full)
            r = get(
                self._api
                + quote('doi-self:"%s" OR doi-self:"%s"' % (doi, doi.upper())),
                headers=self._headers,
                timeout=30,
            )
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                return json_res.get("result")


class ORCIDResourceFinderPMID(ApiDOIResourceFinder):
    """This class implements an identifier manager for orcid identifier"""

    def __init__(self, data={}, use_api_service=True, api_key=None):
        """ORCID resource finder constructor.

        Args:
            date (str, optional): path to date file. Defaults to None.
            orcid (str, optional): path to orcid file. Defaults to None.
            issn (str, optional): path to issn file. Defaults to None.
            pmid (str, optional): path to pmid file. Defaults to None.
            use_api_service (bool, optional): true if you want to use api service. Defaults to True.
            key (str, optional): api key. Defaults to None.
        """
        super().__init__(data, use_api_service=use_api_service, id_type="pmid")
        self._api = "https://pub.orcid.org/v2.1/search?q="
        self._api_key = api_key

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            for item in json_obj:
                orcid = item.get("orcid-identifier")
                if orcid is not None:
                    orcid_norm = self._om.normalise(orcid["path"])
                    if orcid_norm is not None:
                        result.add(orcid_norm)

        return result

    def _call_api(self, pmid_full):
        if self._use_api_service:
            if self._api_key is not None:
                self._headers["Authorization"] = "Bearer %s" % self._api_key
            self._headers["Content-Type"] = "application/json"

            pmid = self._dm.normalise(pmid_full)
            r = get(
                self._api + quote('pmid-self:"%s"' % (pmid)),
                headers=self._headers,
                timeout=30,
            )
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                return json_res.get("result")

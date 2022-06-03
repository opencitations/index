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

from re import sub, match
from urllib.parse import unquote, quote
from requests import get
from json import loads
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep
from bs4 import BeautifulSoup

from oc.index.identifier.base import IdentifierManager

class PMIDManager(IdentifierManager):
    """This class implements an identifier manager for pmid identifier"""

    def __init__(self, data={}, use_api_service=True):
        """PMID manager constructor."""
        super().__init__()
        self._api = "https://pubmed.ncbi.nlm.nih.gov/"
        self._use_api_service = use_api_service
        self._p = "pmid:"
        self._data = data

    def is_valid(self, pmid):
        """Check if a pmid is valid.

        Args:
            id_string (str): the pmid to check

        Returns:
            bool: true if the doi is valid, false otherwise.
        """
        pmid = self.normalise(pmid, include_prefix=True)

        if pmid is None or match("^pmid:[1-9]\d*$", pmid) is None:
            return False
        else:
            if not pmid in self._data or self._data[pmid] is None:
                return self.__pmid_exists(pmid)
            return self._data[pmid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        """It returns the pmid normalized.

        Args:
            id_string (str): the pmid to normalize.
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.

        Returns:
            str: the normalized pmid
        """
        id_string = str(id_string)
        try:
            pmid_string = sub("^0+", "", sub("\0+", "", (sub( "[^\d+]", "", id_string))))
            return "%s%s" % (self._p if include_prefix else "", pmid_string)
        except:
            # Any error in processing the PMID will return None
            return None


    def __pmid_exists(self, pmid_full):
        if self._use_api_service:
            pmid = self.normalise(pmid_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    r = get(self._api + quote(pmid) + "/?format=pmid", headers=self._headers, timeout=30)
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        soup = BeautifulSoup(r.content, features="lxml")
                        for i in soup.find_all("meta", {"name": "uid"}):
                            id = i["content"]
                            if id == pmid:
                                return True

                except ReadTimeout:
                    # Do nothing, just try again
                    pass
                except ConnectionError:
                    # Sleep 5 seconds, then try again
                    sleep(5)

        return False

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


class OMIDManager(IdentifierManager):
    """This class implements an identifier manager for omid identifier"""

    def __init__(self, data={}, use_api_service=True):
        """OMID manager constructor."""
        super().__init__()
        self._api = "https://opencitations.net/meta/br/"
        self._use_api_service = use_api_service
        self._p = "omid:"
        self._data = data

    def is_valid(self, omid):
        """Check if a omid is valid.

        Args:
            id_string (str): the omid to check

        Returns:
            bool: true if the doi is valid, false otherwise.
        """
        omid = self.normalise(omid, include_prefix=False)

        if omid is None or match("^br\/[1-9]\d*$", omid) is None:
            return False
        else:
            return self._data[omid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        """It returns the omid normalized.

        Args:
            id_string (str): the omid to normalize.
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.

        Returns:
            str: the normalized omid
        """
        id_string = str(id_string)
        try:
            omid_string = id_string
            return "%s%s" % (self._p if include_prefix else "", omid_string)
        except:
            return None

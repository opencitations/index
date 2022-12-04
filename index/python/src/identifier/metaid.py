#!python
# -*- coding: utf-8 -*-
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
from re import sub
from urllib.parse import unquote, quote
from requests import get
from json import loads
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep

from oc.index.identifier.base import IdentifierManager

class MetaIDManager(IdentifierManager):
    def __init__(self, data = {}, use_api_service=False): 

        self.p = "meta:"
        self.use_api_service=use_api_service
        self._api = "https://w3id.org/oc/meta/"
        self._data = data
        super(MetaIDManager, self).__init__()

    def is_valid(self, id_string): #  verifies if is valid and is in the list of valid metaids
        metaid = self.normalise(id_string, include_prefix=True)
        if metaid is None:
            return False
        else: 
            if not metaid in self._data or self._data[metaid] is None:
                return self.__metaid_exists(metaid)
            return self._data[metaid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            metaid_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("/")-2:])))
            return "%s%s" % (self.p if include_prefix else "", metaid_string.lower().strip())
        except:  # Any error in processing the MetaID will return None
            return None

    def __metaid_exists(self, metaid_full):
        if self._use_api_service:
            metaid = self.normalise(metaid_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    r = get(self._api + quote(metaid), headers=self._headers, timeout=30)
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        json_res = loads(r.text)
                        return json_res.get("responseCode") == 1
                except ReadTimeout:
                    # Do nothing, just try again
                    pass
                except ConnectionError:
                    # Sleep 5 seconds, then try again
                    sleep(5)

        return False



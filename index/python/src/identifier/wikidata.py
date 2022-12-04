#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Silvio Peroni <essepuntato@gmail.com>
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
import os
from oc.index.identifier.base import IdentifierManager
    
class WikiDataIDManager(IdentifierManager):
    '''This class is used to validate WikiData Identifiers. This is done through an ASK query to the WikiData SPARQL Endpoint.'''
    def __init__(self, data = {}, use_api_service=True):
        super().__init__()
        self._api = "http://wikidata.org/entity/"
        self._data = data
        self._use_api_service = use_api_service
        self._p = "wikidata:"

    def is_valid(self, id_string):
        qid = self.normalise(id_string)

        if qid is None:
            return False
        else:
            if not qid in self._data or self._data[qid] is None:
                return self.__qid_exists(qid)
            return self._data[qid].get("valid")

    def normalise(self, id_string, include_prefix=False):
        try:
            id_string = id_string.upper()
            qid_string = sub("\0+", "", sub("\s+", "", unquote(id_string[id_string.index("Q"):])))
            return "%s%s" % (self._p if include_prefix else "", qid_string.strip())
        except Exception as e:  # Any error in processing the WikiData ID will return None
            print(e)
            return None

    def __qid_exists(self, qid_full):
        if self.use_api_service:
            qid = self.normalise(qid_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    r = get(self._api + quote(qid), headers=self._headers, timeout=30)
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        json_res = loads(r.text)
                        return json_res.get("responseCode") == 1
                except ReadTimeout:
                    pass  # Do nothing, just try again
                except ConnectionError:
                    sleep(5)  # Sleep 5 seconds, then try again

        return False
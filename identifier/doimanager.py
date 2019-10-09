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


from index.identifier.identifiermanager import IdentifierManager
from re import sub
from urllib.parse import unquote, quote
from requests import get
from json import loads
from index.storer.csvmanager import CSVManager


class DOIManager(IdentifierManager):
    def __init__(self, valid_doi=None, use_api_service=True):
        if valid_doi is None:
            valid_doi = CSVManager(store_new=False)

        self.api = "https://doi.org/api/handles/"
        self.valid_doi = valid_doi
        self.use_api_service = use_api_service
        self.p = "doi:"
        super(DOIManager, self).__init__()

    def set_valid(self, id_string):
        doi = self.normalise(id_string, include_prefix=True)

        if self.valid_doi.get_value(doi) is None:
            self.valid_doi.add_value(doi, "v")

    def is_valid(self, id_string):
        doi = self.normalise(id_string, include_prefix=True)

        if doi is None:
            return False
        else:
            if self.valid_doi.get_value(doi) is None:
                if self.__doi_exists(doi):
                    self.valid_doi.add_value(doi, "v")
                else:
                    self.valid_doi.add_value(doi, "i")

            return self.valid_doi.get_value(doi) == {"v"}

    def normalise(self, id_string, include_prefix=False):
        try:
            doi_string = sub("\s+", "", unquote(id_string[id_string.index("10."):]))
            return "%s%s" % (self.p if include_prefix else "", doi_string.lower().strip())
        except:  # Any error in processing the DOI will return None
            return None

    def __doi_exists(self, doi_full):
        doi = self.normalise(doi_full)
        if self.use_api_service:
            r = get(self.api + quote(doi), headers=self.headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                return json_res.get("responseCode") == 1

        return False

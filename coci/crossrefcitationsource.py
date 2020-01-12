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

from os import walk, sep, remove
from os.path import isdir
from json import load
from csv import DictWriter
from index.citation.citationsource import DirCitationSource
from index.identifier.doimanager import DOIManager


class CrossrefCitationSource(DirCitationSource):
    def __init__(self, src, local_name=""):
        self.last_ref = -1
        self.doi = DOIManager()
        super(CrossrefCitationSource, self).__init__(src, local_name)

    def load(self, file_path):
        result = []
        with open(file_path) as f:
            j = load(f)
            if "items" in j:
                result.extend(j["items"])
        return result, len(result)

    def select_file(self, file_path):
        return file_path.endswith(".json")

    def get_next_citation_data(self):
        row = self._get_next_in_file()
        while row is not None:
            citing = self.doi.normalise(row.get("DOI"))
            if citing is not None and "reference" in row:
                for idx, ref in enumerate(row["reference"]):
                    if idx > self.last_ref:
                        self.last_ref = idx
                        cited = self.doi.normalise(ref.get("DOI"))
                        if cited is not None:
                            self.last_row -= 1
                            self.update_status_file()
                            return citing, cited, None, None, None, None

            self.update_status_file()
            row = self._get_next_in_file()
            self.last_ref = -1

        remove(self.status_file)

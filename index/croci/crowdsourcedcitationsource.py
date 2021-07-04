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
from index.citation.citationsource import CSVFileCitationSource
from index.identifier.doimanager import DOIManager
from index.citation.oci import Citation


class CrowdsourcedCitationSource(CSVFileCitationSource):
    def __init__(self, src, local_name=""):
        self.doi = DOIManager()
        super(CrowdsourcedCitationSource, self).__init__(src, local_name)

    def get_next_citation_data(self):
        row = self._get_next_in_file()

        while row is not None:
            citing = self.doi.normalise(row.get("citing_id"))
            cited = self.doi.normalise(row.get("cited_id"))

            if citing is not None and cited is not None:
                created = row.get("citing_publication_date")
                if not created:
                    created = None

                cited_pub_date = row.get("cited_publication_date")
                if not cited_pub_date:
                    timespan = None
                else:
                    c = Citation(None, None, created, None, cited_pub_date, None, None, None, None, "", None, None, None, None, None)
                    timespan = c.duration

                self.update_status_file()
                return citing, cited, created, timespan, None, None

            self.update_status_file()
            row = self._get_next_in_file()

        remove(self.status_file)

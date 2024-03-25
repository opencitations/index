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

from csv import DictReader

from oc.index.identifier.doi import DOIManager
from oc.index.parsing.base import CitationParser


class CrowdsourcedParser(CitationParser):
    def __init__(self):
        super().__init__()
        self._rows = []
        self._doi_manager = DOIManager() # MIDManager()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".csv")

    def parse(self, filename: str):
        super().parse(filename)
        with open(filename, encoding="utf8") as fp:
            self._rows = list(DictReader(fp))
        self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop(0)
        self._current_item += 1
        citing = self._doi_manager.normalise(row.get("citing_id"))
        cited = self._doi_manager.normalise(row.get("cited_id"))

        if citing is not None and cited is not None:
            citing_date = row.get("citing_publication_date")
            if not citing_date:
                citing_date = None

            cited_date = row.get("cited_publication_date")
            if not cited_date:
                cited_date = None

            return citing, cited, citing_date, cited_date, None, None

        return self.get_next_citation_data()

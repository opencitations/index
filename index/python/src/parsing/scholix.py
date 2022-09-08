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

from json import load

from oc.index.identifier.doi import DOIManager
from oc.index.parsing.base import CitationParser


class ScholixParser(CitationParser):
    def __init__(self):
        super().__init__()
        self._rows = []
        self._doi_manager = DOIManager()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".scholix")

    def parse(self, filename: str):
        with open(filename, encoding="utf8") as fp:
            self._rows = load(fp)
        self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop()
        self._current_item += 1

        citing_item = row.get("Source")
        cited_item = row.get("Target")

        if not citing_item or not cited_item:
            return self.get_next_citation_data()

        citing = citing_item.get("ID")
        cited = cited_item.get("ID")

        if not citing or not cited:
            return self.get_next_citation_data()

        citing = self._doi_manager.normalise(citing)
        cited = self._doi_manager.normalise(cited)

        citing = self._doi_manager.normalise(row.get("citing_id"))
        cited = self._doi_manager.normalise(row.get("cited_id"))

        citing_date = citing_item.get("PublicationDate")
        if not citing_date:
            citing_date = None
        cited_date = cited_item.get("PublicationDate")
        if not cited_date:
            cited_date = None

        return citing, cited, citing_date, cited_date, None, None

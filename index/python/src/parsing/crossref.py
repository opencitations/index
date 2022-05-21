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


class CrossrefParser(CitationParser):
    def __init__(self):
        self._rows = []
        self._doi_manager = DOIManager()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".json")

    def parse(self, filename: str):
        super().parse(filename)
        json_content = None
        with open(filename, encoding="utf8") as fp:
            json_content = load(fp)

        if "items" in json_content:
            self._rows = json_content.get("items")
            self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop()
        self._current_item += 1
        citing = self._doi_manager.normalise(row.get("DOI"))
        if citing is not None and "reference" in row:
            citations = []
            for ref in row["reference"]:
                cited = self._doi_manager.normalise(ref.get("DOI"))
                if cited is not None:
                    citations.append((citing, cited, None, None, None, None))
            return citations

        return self.get_next_citation_data()
        
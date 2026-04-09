#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

from oc_index.identifier.pmid import PMIDManager
from oc_index.parsing.base import CitationParser
import csv


class NIHParser(CitationParser):
    def __init__(self):
        super().__init__()
        self._rows = []
        self._pmid_manager = PMIDManager()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".csv")

    def parse(self, filename: str):
        super().parse(filename)
        with open(filename, mode='r') as csv_file:
            csv_reader_l = list(csv.DictReader(csv_file))
            self._rows = csv_reader_l
            self._items = len(csv_reader_l)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop(0)
        self._current_item += 1
        citing = self._pmid_manager.normalise(str(row.get("citing")))
        cited = self._pmid_manager.normalise(str(row.get("referenced")))

        if citing is not None and cited is not None:
            return citing, cited, None, None, None, None

        return self.get_next_citation_data()

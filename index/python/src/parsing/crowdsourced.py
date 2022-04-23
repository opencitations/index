from csv import DictReader

from oc.index.identifier.doi import DOIManager
from oc.index.parsing.parser import CitationParser


class CrowdsourcedParser(CitationParser):
    def __init__(self):
        self._rows = []
        self._doi_manager = DOIManager()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".csv")

    def parse(self, filename: str):
        super().parse(filename)
        with open(filename, encoding="utf8") as fp:
            self._rows = DictReader(fp)
        self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop()
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

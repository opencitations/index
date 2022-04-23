from json import load

from oc.index.identifier.doi import DOIManager
from oc.index.parsing.parser import CitationParser


class ScholixParser(CitationParser):
    def __init__(self):
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

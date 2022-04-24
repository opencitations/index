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


class DataciteParser(CitationParser):
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

        if "data" in json_content:
            self._rows = json_content.get("data")
            self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        row = self._rows.pop()
        self._current_item += 1

        # from here: parse the row and return citation data

        citing = self._doi_manager.normalise(row["attributes"]["doi"])
        if citing is not None and "attributes" in row:
            citations = []

            attr = row["attributes"]
            if "relatedIdentifiers" in attr:
                relatedIdentifier = attr["relatedIdentifiers"]
                # esempio: "relatedIdentifiers" : [{"relationType":"IsCitedBy","relatedIdentifier":"10.1234/testpub","relatedIdentifierType":"DOI"},{"relationType":"Cites","relatedIdentifier":"http://testing.ts/testpub","relatedIdentifierType":"URN"}]
                if relatedIdentifier:
                    for related in relatedIdentifier:
                        relatedIdentifierType = str(related["relatedIdentifierType"])
                        relatedIdentifierType = relatedIdentifierType.lower()
                        if relatedIdentifierType:
                            if relatedIdentifierType == "doi":
                                relationType = related["relationType"]
                                if relationType:
                                    if relationType == "References":
                                        if related["relatedIdentifier"]:

                                            cited = self._doi_manager.normalise(
                                                related["relatedIdentifier"]
                                            )

                                            if cited is not None:
                                                citations.append(
                                                    (
                                                        citing,
                                                        cited,
                                                        None,
                                                        None,
                                                        None,
                                                        None,
                                                    )
                                                )

                    # tendenzialmente non ritornerà niente perché spesso "relatedIdentifiers" è una lista vuota
                    # (no elementi su cui iterare)
                    return citations
                    # controlla che sia indentato correttamente:
                    # nel cocdice ci crossref si allinea con for ref in row["reference"],
                    # quindi "per ogni citato". Così dovrebbe funzionare perché "relatedIdentifiers"
                    # è una lista che contiene un dizionario per ogni id related con l'id in questione,
                    # di cui poi, attraverso le coppie chiave-valore, specifica: il tipo di relazione (NB:
                    # COME ESISTONO SIA "isReferencedBy" che "isCitedBy", oltre a "References"
                    # esiste anche un "Cites": che senso ha?), l'identificativo stesso, e il tipo di identificativo.

        return self.get_next_citation_data()

from json import loads

from index.citation.data import CitationData
from index.citation.oci import Citation
from index.identifier.doimanager import DOIManager
from index.parser.parser import CitationParser


class ScholixParser(CitationParser):
    def __init__(self):
        self.__doi = DOIManager()
        self.__current_file = None
        self.__last_row = None

    def is_valid(self, file):
        return file.endswith(".scholix")

    def set_input_file(self, file, targz_fd):
        self.__current_file = open(file, "r", encoding="utf8")
        self.__current_file.readline()
        self.__last_row = 1

    def __get_next_scholix_item(self):
        if self.__current_file is None:
            return None

        line = self.__current_file.readline()
        self.__last_row += 1
        if line is None or line == "]":
            self.__current_file.close()
            return None

        if self.__current_file is None:
            return None

        closed = 1
        value = ""
        while closed != 0:
            value += line
            line = self.__current_file.readline()
            self.__last_row += 1
            if "{" in line:
                closed += 1
            if "}" in line:
                closed -= 1
        value += "}"
        return loads(value)

    def get_next_citation_data(self):
        item = self.__get_next_scholix_item()
        if item is not None:
            citing = item.get("Source")
            cited = item.get("Target")

            if not citing or not cited:
                return None

            citing_id = citing.get("Identifier")
            cited_id = cited.get("Identifier")
            if not citing_id or not cited_id:
                return None

            citing_doi = citing_id.get("ID")
            cited_doi = cited_id.get("ID")
            if not citing_doi or not cited_doi:
                return None

            citing_doi = self.__doi.normalise(citing_doi)
            cited_doi = self.__doi.normalise(cited_doi)

            citing_pubdate = citing.get("PublicationDate")
            cited_pubdate = cited.get("PublicationDate")

            timespan = None
            if not citing_pubdate:
                citing_pubdate = None
            else:
                if not cited_pubdate:
                    timespan = None
                else:
                    c = Citation(
                        None,
                        None,
                        citing_pubdate,
                        None,
                        cited_pubdate,
                        None,
                        None,
                        None,
                        None,
                        "",
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    timespan = c.duration

            return CitationData(
                citing_doi, cited_doi, citing_pubdate, timespan, None, None
            )
        return None

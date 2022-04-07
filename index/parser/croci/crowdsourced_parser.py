from csv import DictReader
from index.citation.data import CitationData
from index.citation.oci import Citation
from index.identifier.doimanager import DOIManager
from index.parser.parser import CitationParser


class CrowdsourcedParser(CitationParser):
    def __init__(self):
        self.__doi = DOIManager()
        self.__rows = []
        self.__boolmap = {
            "yes": True,
            "no": False,
        }

    def is_valid(self, file):
        return file.endswith(".csv")

    def set_input_file(self, file, targz_fd):
        result = []

        if targz_fd is None:
            f = open(file, encoding="utf8")
        else:
            f = targz_fd.extractfile(file)

        result.extend(DictReader(f))

        self.__rows = result

    def get_next_citation_data(self):
        if len(self.__rows) == 0:
            return None

        row = self.__rows.pop()

        citing = self.__doi.normalise(row.get("citing_id"))
        cited = self.__doi.normalise(row.get("cited_id"))

        if citing is not None and cited is not None:
            created = row.get("citing_publication_date")
            if not created:
                created = None

            cited_pub_date = row.get("cited_publication_date")
            if not cited_pub_date:
                timespan = None
            else:
                c = Citation(
                    None,
                    None,
                    created,
                    None,
                    cited_pub_date,
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

            return CitationData(citing, cited, created, timespan, None, None)
        else:
            return None

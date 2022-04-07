from index.citation.data import CitationData
from index.identifier.doimanager import DOIManager
from index.parser.parser import CitationParser
from json import load, loads


class CrossrefParser(CitationParser):
    def __init__(self):
        self.last_ref = -1
        self.__doi = DOIManager()
        self.__rows = []

    def is_valid(self, file):
        return file.endswith(".json")

    def set_input_file(self, file, targz_fd):
        result = []

        if targz_fd is None:
            with open(file, encoding="utf8") as f:
                j = load(f)
        else:
            f = targz_fd.extractfile(file)
            json_str = f.read()

            if type(json_str) is bytes:
                json_str = json_str.decode("utf-8")

            j = loads(json_str)

        if "items" in j:
            result.extend(j["items"])

        self.__rows = result

    def get_next_citation_data(self):
        if len(self.__rows) == 0:
            return None

        row = self.__rows.pop()
        citing = self.__doi.normalise(row.get("DOI"))
        if citing is not None and "reference" in row:
            for idx, ref in enumerate(row["reference"]):
                if idx > self.last_ref:
                    self.last_ref = idx
                    cited = self.__doi.normalise(ref.get("DOI"))
                    if cited is not None:
                        # self.update_status_file() # In Crossref, this should not be
                        # needed since I modify the row only when I finished to process
                        # all the references of a certain row (since here, a row is an
                        # article, not a citation)
                        return CitationData(citing, cited, None, None, None, None)

        self.last_ref = -1

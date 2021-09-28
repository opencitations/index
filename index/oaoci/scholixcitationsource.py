from index.citation.citationsource import CitationSource
from index.identifier.doimanager import DOIManager
from csv import DictWriter
from json import loads
from index.citation.oci import Citation

from os import walk, sep
from os.path import isdir, join

class ScholixCitationSource(CitationSource):
    def __init__(self, src):
        super(ScholixCitationSource, self).__init__(src)
        self.doi = DOIManager()
        self.__current_file = None
        self.__last_row = None

        if not isdir(src):
            raise ValueError("src must be a valid directory path")

        self.status_file = src + sep + ".dir_citation_source"
        self.__files = []
        for path, _, files in walk(self.src):
            for name in files:
                if name.endswith(".scholix"):
                    self.__files.append(join(path, name))
        self.__files.sort()
                    
    def __open_next_scholix_file(self):
        if not self.__current_file is None:
            self.__current_file.close()
        if len(self.__files) == 0:
            self.__current_file = None
            return
        self.__current_path = self.__files.pop()
        self.__current_file = open(self.__current_path, "r", encoding="utf8")
        self.__current_file.readline()
        self.__last_row = 1

    def __get_next_scholix_item(self):
        if self.__current_file is None:
            self.__open_next_scholix_file()
            if self.__current_file is None:
                return None

        line = self.__current_file.readline()
        self.__last_row += 1
        if line is None or line == "]":
            self.__current_file.close()
            self.__current_file = None
            return self.__get_next_scholix_item()

        if self.__current_file is None:
            return None

        closed = 1
        value = ""
        while(closed != 0):
            value += line
            line = self.__current_file.readline()
            self.__last_row += 1
            if "{" in line:
                closed += 1
            if "}" in line:
                closed -= 1
        value += "}"
        return loads(value)

    def update_status_file(self):
        with open(self.status_file, "w", encoding="utf8") as f:
            w = DictWriter(f, fieldnames=("file", "line"))
            w.writeheader()
            w.writerow({"file": self.__current_path, "line": self.__last_row})

    def get_next_citation_data(self):
        item = self.__get_next_scholix_item()
        if item is not None:
            citing = item.get("Source")
            cited = item.get("Target")

            if not citing or not cited:
                self.update_status_file()
                return None
            
            citing_id = citing.get("Identifier")
            cited_id = cited.get("Identifier")
            if not citing_id or not cited_id:
                self.update_status_file()
                return None

            citing_doi = citing_id.get("ID")
            cited_doi = cited_id.get("ID")
            if not citing_doi or not cited_doi:
                self.update_status_file()
                return None
            
            citing_doi = self.doi.normalise(citing_doi)
            cited_doi = self.doi.normalise(cited_doi)

            citing_pubdate = citing.get("PublicationDate")
            cited_pubdate = cited.get("PublicationDate")
            
            timespan = None
            if not citing_pubdate:
                citing_pubdate = None
            else:
                if not cited_pubdate:
                    timespan = None
                else:
                    c = Citation(None, None, citing_pubdate, None, cited_pubdate, None, None, None, None, "", None, None, None, None, None)
                    timespan = c.duration

            self.update_status_file()
            return citing_doi, cited_doi, citing_pubdate, timespan, None
        return None

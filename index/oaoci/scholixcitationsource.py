
from index.citation.citationsource import CitationSource
from index.identifier.doimanager import DOIManager
from csv import DictWriter
from json import loads

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
            citing = item["Source"]["Identifier"]["ID"] 
            cited = item["Target"]["Identifier"]["ID"]
            self.update_status_file()
            return citing, cited, None, None, None
        return None
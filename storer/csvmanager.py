#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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

from csv import DictReader
from io import StringIO
from os.path import exists, isdir
from os import walk, sep


class CSVManager(object):
    """This class is able to load a simple CSV composed by two fields, 'id' and
    'value', and then to index all its items in a structured form so as to be
    easily queried. In addition, it allows one to store new information in the CSV,
    if needed."""

    def __init__(self, csv_path=None, line_threshold=10000, store_new=True):
        self.csv_path = csv_path
        self.data = {}
        self.store_new = store_new

        if csv_path is not None and exists(csv_path):
            CSVManager.__load_all_csv_files([csv_path], self.__load_csv, line_threshold=line_threshold)

    @staticmethod
    def load_csv_column_as_set(file_or_dir_path, key, line_threshold=10000):
        result = set()

        if exists(file_or_dir_path):
            file_to_process = []
            if isdir(file_or_dir_path):
                for cur_dir, cur_subdir, cur_files in walk(file_or_dir_path):
                    for cur_file in cur_files:
                        if cur_file.endswith(".csv"):
                            file_to_process.append(cur_dir + sep + cur_file)
            else:
                file_to_process.append(file_or_dir_path)

            for item in CSVManager.__load_all_csv_files(file_to_process, CSVManager.__load_csv_by_key,
                                                        line_threshold=line_threshold, key=key):
                result.update(item)

        return result

    @staticmethod
    def __load_csv_by_key(csv_string, key):
        result = set()

        csv_metadata = DictReader(StringIO(csv_string), delimiter=',')
        for row in csv_metadata:
            result.add(row[key])

        return result

    @staticmethod
    def __load_all_csv_files(list_of_csv_files, fun, line_threshold, **params):
        result = []
        header = None

        for csv_path in list_of_csv_files:
            with open(csv_path, encoding="utf-8") as f:
                csv_content = ""
                for idx, line in enumerate(f.readlines()):
                    if header is None:
                        header = line
                        csv_content = header
                    else:
                        if idx % line_threshold == 0:
                            result.append(fun(csv_content, **params))
                            csv_content = header
                        csv_content += line

            result.append(fun(csv_content, **params))

        return result

    def get_value(self, id_string):
        """It returns the set of values associated to the input 'id_string',
        or None if 'id_string' is not included in the CSV."""
        if id_string in self.data:
            return set(self.data[id_string])

    def add_value(self, id_string, value):
        """It adds the value specified in the set of values associated to 'id_string'.
        If the object was created with the option of storing also the data in a CSV
        ('store_new' = True, default behaviour), then it also add new data in the CSV."""
        if id_string not in self.data:
            self.data[id_string] = set()

        if value not in self.data[id_string]:
            self.data[id_string].add(value)

            if self.csv_path is not None and self.store_new:
                if not exists(self.csv_path):
                    with open(self.csv_path, "w") as f:
                        f.write('"id","value"\n')

                with open(self.csv_path, "a") as f:
                    f.write('"%s","%s"\n' % (id_string.replace('"', '""'),
                                             value.replace('"', '""')))

    def __load_csv(self, csv_string):
        csv_metadata = DictReader(StringIO(csv_string), delimiter=',')
        for row in csv_metadata:
            cur_id = row["id"]
            if cur_id not in self.data:
                self.data[cur_id] = set()

            self.data[cur_id].add(row["value"])

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

from os import walk, sep, remove
from os.path import isdir, exists, dirname
from csv import DictReader, DictWriter
from collections import deque
from urllib.parse import parse_qs


class CitationSource(object):
    """This is the abstract class that must be implemented for any source providing
    citations to be included in an OpenCitations Index, e.g. COCI or CROCI. In
    practice, it provides basic methods that must be used to get the basic
    information about citations, in particular the citing id and the cited id."""

    def __init__(self, src):
        """The constructor allows to associate to the variable 'src' a kind of source,
        that will be used to retrieve citation data."""
        self.src = src

    def get_next_citation_data(self):
        """This method returns the next citation data available in the source specified.
        The citation data returned is a tuple of six elements: citing id (string), citing
        date (string, or None if unknown), cited id (string), cited date (string or None
        if unknown), if it is a journal self-citation (True = yes, False = no, None = do
        not know), and if it is an author self-citation (True = yes, False = no, None = do
        not know). If no more citation data are available, it returns None."""
        pass


class DirCitationSource(CitationSource):
    def __init__(self, all_src, local_name=""):
        self.last_file = None
        self.last_row = None
        self.data = None
        self.len = None
        self.status_file = None
        self.all_files = []
        self.file_idx = 0
        self.file_len = 0

        if isinstance(all_src, (list, set, tuple)):
            src_collection = all_src
        else:
            src_collection = [all_src]

        for src in sorted(src_collection):
            cur_dir = src
            if not isdir(cur_dir):
                cur_dir = dirname(cur_dir)

            if self.status_file is None:
                self.status_file = cur_dir + sep + ".dir_citation_source" + local_name

            if exists(self.status_file):
                with open(self.status_file, encoding="utf8") as f:
                    row = next(DictReader(f))
                    self.last_file = row["file"] if row["file"] else None
                    self.last_row = int(row["line"]) if row["line"] else None

            if isdir(src):
                for cur_dir, cur_subdir, cur_files in walk(src):
                    for cur_file in cur_files:
                        full_path = cur_dir + sep + cur_file
                        if self.select_file(full_path):
                            self.all_files.append(full_path)
            elif self.select_file(src):
                self.all_files.append(src)

        self.all_files.sort()
        self.file_len = len(self.all_files)

        self.all_files = deque(self.all_files)
        # If case the directory contains some files
        if self.all_files:
            # If there is no status file (i.e. '.dir_citation_source') available,
            # the we have to set the current last file as the first of the list of files
            if self.last_file is None:
                self.last_file = self.all_files.popleft()
            # In case the status file is available (i.e. '.dir_citation_source') and
            # it already specifies the last file that have been processed,
            # we need to exclude all the files in the list that are lesser than the
            # last one considered
            else:
                tmp_file = self.all_files.popleft()
                while tmp_file < self.last_file:
                    tmp_file = self.all_files.popleft()
        
        if self.last_file is not None:
            self.file_idx += 1
            print("Opening file '%s' (%s out of %s)" % (self.last_file, self.file_idx, self.file_len))
            self.data, self.len = self.load(self.last_file)
        
        if self.last_row is None:
            self.last_row = -1

        super(DirCitationSource, self).__init__(src)

    def _get_next_in_file(self):
        # A file containing citations was already open
        if self.data is not None:
            # Set the next row in the file to consider
            self.last_row += 1
            # There are citations in the open fine that have not
            # being gathered yet
            if self.last_row < self.len:
                return self.data[self.last_row]
            # All the citations of the last file parsed have been
            # gathered, thus a new file must be considered, if available
            elif self.all_files:
                self.last_file = self.all_files.popleft()
                self.file_idx += 1
                print("Opening file '%s' (%s out of %s)" % (self.last_file, self.file_idx, self.file_len))
                self.data, self.len = self.load(self.last_file)
                self.last_row = -1
                return self._get_next_in_file()
            # There are no citation data to consider, since no more
            # files are available
            else:
                self.data = None

    def load(self, file_path):
        pass  # To implement in the concrete classes

    def select_file(self, file_path):
        pass  # To implement in the concrete classes

    def update_status_file(self):
        with open(self.status_file, "w", encoding="utf8") as f:
            w = DictWriter(f, fieldnames=("file", "line"))
            w.writeheader()
            w.writerow({"file": self.last_file, "line": self.last_row})


class CSVFileCitationSource(DirCitationSource):
    def load(self, file_path):
        result = []
        with open(file_path, encoding="utf8") as f:
            result.extend(DictReader(f))
        return result, len(result)

    def select_file(self, file_path):
        return file_path.endswith(".csv")

    def get_next_citation_data(self):
        row = self._get_next_in_file()
        boolmap = {
            "yes": True,
            "no": False,
        }
        while row is not None:
            citing = row.get("citing")
            cited = row.get("cited")

            if citing is not None and cited is not None:
                created = row.get("creation") or None
                timespan = row.get("timespan") or None

                journal_sc = boolmap.get(row.get("journal_sc"))
                author_sc = boolmap.get(row.get("author_sc"))

                self.update_status_file()
                return citing, cited, created, timespan, journal_sc, author_sc

            self.update_status_file()
            row = self._get_next_in_file()

        if exists(self.status_file):
            remove(self.status_file)

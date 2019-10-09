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

from os.path import isdir, exists
from os import walk, sep
from csv import DictReader
from io import StringIO


def key_set_from_csv(csv_file_or_dir, key, line_threshold=10000):
    result = set()

    if exists(csv_file_or_dir):
        file_list = []
        if isdir(csv_file_or_dir):
            for cur_dir, cur_subdir, cur_files in walk(csv_file_or_dir):
                for cur_file in [f for f in cur_files if f.endswith(".csv")]:
                    file_list.append(cur_dir + sep + cur_file)
        elif csv_file_or_dir.endswith(".csv"):
            file_list.append(csv_file_or_dir)

        header = None
        for file in file_list:
            with open(file) as f:
                csv_content = ""
                for idx, line in enumerate(f.readlines()):
                    if header is None:
                        header = line
                        csv_content = header
                    else:
                        if idx % line_threshold == 0:
                            for row in DictReader(StringIO(csv_content), delimiter=','):
                                result.add(row[key])
                            csv_content = header
                        csv_content += line

                for row in DictReader(StringIO(csv_content), delimiter=','):
                    result.add(row[key])

    return result

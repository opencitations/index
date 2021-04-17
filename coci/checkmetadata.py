#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2020,
# Silvio Peroni <essepuntato@gmail.com>
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

from argparse import ArgumentParser
from os import sep, walk
from json import load
from collections import deque


def process(input_dir, metadata_field):
    result = None

    for cur_dir, cur_subdir, cur_files in walk(input_dir):
        for cur_file in cur_files:
            if cur_file.endswith(".json"):
                with open(cur_dir + sep + cur_file, encoding="utf8") as f:
                    for item in load(f).get("items", []):
                        to_get = deque(metadata_field)
                        value = None

                        while to_get:
                            if value is None:
                                value = item.get(to_get.popleft())
                            else:
                                value = value.get(to_get.popleft())

                        if result is None or value > result:
                            result = value

    return result


if __name__ == "__main__":
    arg_parser = ArgumentParser("Check metadata in Crossref documents",
                                description="Process Crossref JSON files and check for the biggest value "
                                            "in its metadata")
    arg_parser.add_argument("-i", "--input_dir", dest="input_dir", required=True,
                            help="The directory that contains the Crossref data dump of JSON files.")
    arg_parser.add_argument("-m", "--metadata_field", dest="metadata_field", required=True,
                            help="The name of the metadata to look for.")

    args = arg_parser.parse_args()
    print(process(args.input_dir, args.metadata_field.split("=>")))

# Example of call
# python -m index.coci.checkmetadata -i /input/dir -m "deposited=>date-time"

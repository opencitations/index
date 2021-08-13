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
from os import sep, walk, makedirs
from json import load, dump
from collections import deque
from os.path import exists


def process(input_dir, output_dir, metadata_field, metadata_value):
    result = {"items": []}
    idx = 1
    item_idx = 0

    if not exists(output_dir):
        makedirs(output_dir)

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

                        if value is None or value >= metadata_value:
                            if item_idx >= 10000:
                                with open(output_dir + sep + str(idx) + ".json", "w", 
                                          encoding="utf8") as g:
                                    dump(result, g, ensure_ascii=False)
                                result = {"items": []}
                                item_idx = 0
                                idx += 1

                            item_idx += 1
                            result.get("items").append(item)

    if result.get("items"):
        with open(output_dir + sep + str(idx) + ".json", "w", encoding="utf8") as g:
            dump(result, g, ensure_ascii=False)


if __name__ == "__main__":
    arg_parser = ArgumentParser("Trim Crossref dump",
                                description="Process Crossref JSON files and trim them according to the value "
                                            "contained in the metadata specified in input")
    arg_parser.add_argument("-i", "--input_dir", dest="input_dir", required=True,
                            help="The directory that contains the Crossref data dump of JSON files.")
    arg_parser.add_argument("-o", "--output_dir", dest="output_dir", required=True,
                            help="The directory that will contain the selected JSON files.")
    arg_parser.add_argument("-m", "--metadata_field", dest="metadata_field", required=True,
                            help="The name of the metadata to look for.")
    arg_parser.add_argument("-v", "--metadata_value", dest="metadata_value", required=True,
                            help="The minimum (exclusive) value of the metadata")

    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir, args.metadata_field.split("=>"), args.metadata_value)

# Example of call
# python -m index.coci.trimdump -i /input/dir -o /output/dir -m "deposited=>date-time" -v "2019-10-12T07:59:37Z"

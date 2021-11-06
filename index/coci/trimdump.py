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
from os.path import isdir, basename
from json import load, dump, loads
from collections import deque
from os.path import exists
import tarfile


def get_all_files(i_dir_or_targz_file):
    result = []
    targz_fd = None

    if isdir(i_dir_or_targz_file):
        for cur_dir, cur_subdir, cur_files in walk(i_dir_or_targz_file):
            for cur_file in cur_files:
                if cur_file.endswith(".json") and not basename(cur_file).startswith("."):
                    result.append(cur_dir + sep + cur_file)
    elif i_dir_or_targz_file.endswith("tar.gz"):
        targz_fd = tarfile.open(i_dir_or_targz_file, "r:gz", encoding="utf-8")
        for cur_file in targz_fd:
            if cur_file.name.endswith(".json") and not basename(cur_file.name).startswith("."):
                result.append(cur_file)
    else:
        print("It is not possible to process the input path.")
    return result, targz_fd


def load_json(file, targz_fd, file_idx, len_all_files):
    result = None

    if targz_fd is None:
        print("Open file %s of %s" % (file_idx, len_all_files))
        with open(file, encoding="utf8") as f:
            result = load(f)
    else:
        print("Open file %s of %s (in tar.gz archive)" % (file_idx, len_all_files))
        cur_tar_file = targz_fd.extractfile(file)
        json_str = cur_tar_file.read()

        # In Python 3.5 it seems that, for some reason, the extractfile method returns an 
        # object 'bytes' that cannot be managed by the function 'load' in the json package.
        # Thus, to avoid issues, in case an object having type 'bytes' is return, it is
        # transformed as a string before passing it to the function 'loads'. Please note
        # that Python 3.9 does not show this behaviour, and it works correctly without
        # any transformation.
        if type(json_str) is bytes:
            json_str = json_str.decode("utf-8")
        
        result = loads(json_str)
    
    return result


def process(input_dir_or_targz, output_dir, metadata_field, metadata_value):
    result = {"items": []}
    idx = 1
    item_idx = 0

    if not exists(output_dir):
        makedirs(output_dir)

    all_files = get_all_files(input_dir_or_targz)
    for cur_file in all_files:
        load_json = load_json(cur_file)

        for item in load_json.get("items", []):
            if "reference" in item and len(item["reference"]) > 0:

                matching = False
                all_to_get = deque(metadata_field)
                all_to_check = deque(metadata_value)
                while not matching and len(all_to_get) > 0:
                    to_get = deque(all_to_get.popleft())
                    op, to_check = all_to_check.popleft()

                    value = None
                    while to_get:
                        if value is None:
                            value = item.get(to_get.popleft())
                        else:
                            value = value.get(to_get.popleft())

                    if value is None:
                        matching = True
                    elif op == "==":
                        matching = value == to_check
                    elif op == ">=":
                        matching = value >= to_check
                    elif op == "<=":
                        matching = value <= to_check
                    elif op == "!=":
                        matching = value != to_check
                    elif op == ">":
                        matching = value > to_check
                    elif op == "<":
                        matching = value < to_check
                    else:
                        print("Error: Comparison operator not found:", op)
                        exit(-1)

                if matching:
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
                            help="The name of the metadata fields to look for.")
    arg_parser.add_argument("-v", "--metadata_value", dest="metadata_value", required=True,
                            help="The value of the metadata to consider in the comparison")

    args = arg_parser.parse_args()
    metadata_fields = args.metadata_field.split(" ")
    metadata_values = args.metadata_value.split(" ")

    if len(metadata_fields) == len(metadata_values):
        metadata_fields = [item.split("=>") for item in metadata_fields]
        metadata_values = [item.split(":", 1) for item in metadata_values]

        process(args.input_dir, args.output_dir, metadata_fields, metadata_values)
    else:
        print("Error: different number of metadata fields and values specified.")

# Example of call
# python -m index.coci.trimdump -i /input/dir -o /output/dir -m "deposited=>date-time member" -v ">=:2019-10-12T07:59:37Z ==:316"

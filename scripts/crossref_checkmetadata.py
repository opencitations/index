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
from os.path import isdir, basename
from json import load, loads
from collections import deque
import tarfile


def process(input_dir_or_targz_file, metadata_field):
    result = None
    list_of_files = []
    is_targz = False

    if isdir(input_dir_or_targz_file):
        for cur_dir, cur_subdir, cur_files in walk(input_dir_or_targz_file):
            for cur_file in cur_files:
                if cur_file.endswith(".json") and not basename(cur_file).startswith(
                    "."
                ):
                    list_of_files.append(cur_dir + sep + cur_file)
    elif input_dir_or_targz_file.endswith("tar.gz"):
        is_targz = True
        targz_fd = tarfile.open(input_dir_or_targz_file, "r:gz", encoding="utf-8")
        for cur_file in targz_fd:
            if cur_file.name.endswith(".json") and not basename(
                cur_file.name
            ).startswith("."):
                list_of_files.append(cur_file)
    else:
        print("It is not possible to process the input path.")
        return

    for file_idx, cur_file in enumerate(list_of_files):
        json_file = None
        len_all_files = len(list_of_files)

        if targz_fd is None:
            print("Open file %s of %s" % (file_idx, len_all_files))
            with open(cur_file, encoding="utf8") as f:
                json_file = load(f)
        else:
            print("Open file %s of %s (in tar.gz archive)" % (file_idx, len_all_files))
            cur_tar_file = targz_fd.extractfile(cur_file)
            json_str = cur_tar_file.read()

            # In Python 3.5 it seems that, for some reason, the extractfile method returns an
            # object 'bytes' that cannot be managed by the function 'load' in the json package.
            # Thus, to avoid issues, in case an object having type 'bytes' is return, it is
            # transformed as a string before passing it to the function 'loads'. Please note
            # that Python 3.9 does not show this behaviour, and it works correctly without
            # any transformation.
            if type(json_str) is bytes:
                json_str = json_str.decode("utf-8")

            json_file = loads(json_str)

        if "items" not in json_file and "message" in json_file:
            json_file = json_file["message"]

        for item in json_file.get("items", []):
            to_get = deque(metadata_field)
            value = None

            while to_get:
                if value is None:
                    value = item.get(to_get.popleft())
                else:
                    value = value.get(to_get.popleft())

            if value is not None and (result is None or value > result):
                result = value
                print("New better result:", result)

    if is_targz:
        targz_fd.close()

    return result


def main():
    arg_parser = ArgumentParser(
        "Check metadata in Crossref documents",
        description="Process Crossref JSON files and check for the biggest value "
        "in its metadata",
    )
    arg_parser.add_argument(
        "-i",
        "--input_dir",
        dest="input_dir",
        required=True,
        help="The directory that contains the Crossref data dump of JSON files.",
    )
    arg_parser.add_argument(
        "-m",
        "--metadata_field",
        dest="metadata_field",
        required=True,
        help="The name of the metadata to look for.",
    )

    args = arg_parser.parse_args()
    print("\nFinal result:", process(args.input_dir, args.metadata_field.split("=>")))


# Example of call
# python -m index.coci.checkmetadata -i /input/dir -m "deposited=>date-time"

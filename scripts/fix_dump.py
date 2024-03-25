#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
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

import multiprocessing
import os
import time
import csv
import redis
from zipfile import ZipFile
import json
import io
import re

from tqdm import tqdm
from argparse import ArgumentParser
from urllib.parse import quote
from datetime import datetime, timezone
from collections import defaultdict

_config = get_config()

def create_dirs(path_to_create):
    # Split the path into individual directories
    dirs = path_to_create.split(os.path.sep)
    for directory in dirs:
        current_path = os.path.join(current_path, directory)
        if not os.path.exists(current_path):
            os.mkdir(current_path)

def fix_dump(input_dir, output_dir, f_rm_cits, f_add_cits):

    oci_rm_set = set()
    with open(f_rm_cits, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        for row in reader:
            oci_rm_set.add(row[0])

    oci_add_set = set()
    with open(f_add_cits, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        for row in reader:
            oci_add_set.add(row[0])

    files_to_process = []
    for root, dirs, files in os.walk(input_dir):

        #create dir of file if does not exist
        out_f_dest = output_dir+"fix_dump/"+root.replace(input_dir,"")
        create_dirs(out_f_dest)

        for file in files:
            if file.endswith('.ttl'):

                # process input file
                new_file_lines = []
                with open(os.path.join(input_dir, file), 'r') as ttl_file:
                    lines = ttl_file.readlines()
                    for line in lines:
                        if line.strip() != "":
                            oci_pattern = r"https://w3id.org/oc/index/ci/(\d{1,}-\d{1,})"
                            oci = re.search(oci_pattern, line)
                            if oci:
                                oci = oci.group(1)
                                if oci not in oci_rm_set:
                                    new_file_lines.append(line)

                    # produce corresponding new file
                    with open(out_f_dest+"/"+file, 'w', newline='') as new_file:
                        for line in lines:
                            new_file.write(line + '\n')

def main():
    global _config

    arg_parser = ArgumentParser(description="Normalize the data of OpenCitations Index")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory contatining the original files in TTL",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        default="out",
        help="The destination directory to save outputs",
    )
    arg_parser.add_argument(
        "-rmc",
        "--rmcits",
        required=True,
        default=None,
        help="Remove citations from the RDF (TTL) dump of Index; it needs a CSV file such that each row contains an OCI to be removed from the dump",
    )
    arg_parser.add_argument(
        "-addc",
        "--addcits",
        required=True,
        default=None,
        help="Add citations to the RDF (TTL) dump of Index; it needs a CSV file such that each row contains an OCI to be added as new citation in INDEX dump",
    )

    args = arg_parser.parse_args()

    # input directory/file
    input_dir = args.input + "/" if args.input[-1] != "/" else args.input

    # output directory
    output_dir = args.output + "/" if args.output[-1] != "/" else args.output
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # call the fix_dump function
    fix_dump(input_dir, output_dir, args.rmcits, args.addcits)

    print("Done !!")

#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2016, Giuseppe Grieco <g.grieco1997@gmail.com>
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

'''
In order to run this script you need to specify two positional parameters:
    mode : a number indicating which utility you want to use
        - 0 reads a list of oci from csv and insert them in rethinkdb
        - 1 reads a list of doi date from csv and insert them in rethinkdb
        - 1 reads a list of doi ISSN from csv and insert them in rethinkdb
        - 3 reads a list of doi orcid from csv and insert them in rethinkdb
        - 4 reads a list of doi validity from csv and insert them in rethinkdb
        - 5 creates the database and related tables
    port : rethinkdb's server port

In addition if the port is between 0 and 4 you need also to indicate another
parameter:
    --input_file : this parameter refers to the file or directory from which 
        to read the eventual files to be used to populate the
        database tables.

[NOTE] this script has as a precondition to be run local to the server where 
rethinkdb is started, and performs a direct access without password.
'''

__author__ = 'giuseppegrieco'

import argparse
import sys
from os.path import isfile, isdir, join
from os import walk
from fnmatch import fnmatch
from argparse import RawTextHelpFormatter

from rethinkdb import RethinkDB

def oci(r, line):
    if len(line) > 1:
        line = line.split(",")

        r.db("oc").table("oci").insert({
            "oci": line[0]
        }).run()

def doi_date(r, line):
    line = line.replace("\"", "").split(",")
    doi = line[0]
    date = line[1]

    __doi_update(r, doi, "date", date)

def doi_issn(r, line):
    line = line.replace("\"", "").split(",")
    doi = line[0]
    issn = line[1]

    __doi_update(r, doi, "issn", [issn], True)

def doi_orcid(r, line):
    line = line.replace("\"", "").split(",")
    doi = line[0]
    date = line[1]

    __doi_update(r, doi, "orcid", [date], True)

def doi_valid(r, line):
    line = line.replace("\"", "").split(",")
    doi = line[0]
    validity = line[1].replace("\n", "") == "v"

    __doi_update(r, doi, "validity", validity)

def __doi_update(r, doi, field, value, merge=False):
    if merge:
        value[0] = value[0].replace("\n", "")
    else:
        if isinstance(value, str):
            value = value.replace("\n", "")
    doi_obj = r.db("oc").table("doi").get(doi).run()
    if doi_obj is None:
        doi_obj = __doi_obj(doi)
        doi_obj[field] = value
        r.db("oc").table("doi").insert(doi_obj).run()
    else:
        if not merge:
            r.db("oc").table("doi").get(doi).update({
                field: value
            }).run()
        else:
            r.db("oc").table("doi").get(doi).update({
                field: value + doi_obj[field]
            }).run()

def __doi_obj(doi):
    return {
        "doi": doi,
        "validity": False,
        "orcid": [],
        "date": None,
        "issn": []
    }

def __process_file(inputFile, f):
    file = open(inputFile, 'r')
    file.readline()
    while True:
        line = file.readline()
        if not line:
            break
        f(r, line)
    file.close()

parser = argparse.ArgumentParser(
    description='It insert a specific set of data contained in a file in the database',
    formatter_class=RawTextHelpFormatter
)
parser.add_argument(
    "mode",
    help='a number indicating which utility you want to use, 0 <= mode <= 5\n' +
        '  - 0 reads a list of oci from csv and insert them in rethinkdb\n' +
        '  - 1 reads a list of doi date from csv and insert them in rethinkdb\n' +
        '  - 1 reads a list of doi ISSN from csv and insert them in rethinkdb\n' +
        '  - 3 reads a list of doi orcid from csv and insert them in rethinkdb\n' +
        '  - 4 reads a list of doi validity from csv and insert them in rethinkdb\n' +
        '  - 5 creates the database and related tables',
    type=int
)
parser.add_argument(
    "port",
    help='rethinkdb\'s server port',
    type=int
)
parser.add_argument(
    '--input_file', 
    help='input file (.csv) containing data',
    default=None
)

args = parser.parse_args()
mode = args.mode
port = args.port
inputFile = args.input_file

r = RethinkDB()
r.connect("localhost", port).repl()

# Setup the database
if args.mode == 5:
    r.db_create("oc").run()
    r.db("oc").table_create("doi", primary_key="doi").run()
    r.db("oc").table_create("oci", primary_key="oci").run()
else:
    if inputFile == None:
        print("Input file needed to run mode between 0 and 4", file=sys.stderr)
        sys.exit(-1)
    functions = {
        0: oci,
        1: doi_date,
        2: doi_issn,
        3: doi_orcid,
        4: doi_valid
    }
    f = functions.get(mode)
    if isdir(inputFile):
        for path, subdirs, files in walk(inputFile):
            for name in files:
                if fnmatch(name, "*.csv"):
                    __process_file(join(path, name), f)
    else:
        if isfile(inputFile):
            __process_file(inputFile, f)
        else:
            raise FileNotFoundError(inputFile)
            
print("Operation completed")
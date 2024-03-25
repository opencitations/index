#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Silvio Peroni <essepuntato@gmail.com>, Arianna Moretti <arianna.moretti4@unibo.it>
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

from datetime import datetime
from os.path import abspath, isdir, sep
from os import walk
from argparse import ArgumentParser
from glob import glob
from re import sub
from os.path import basename, normpath
from urllib.parse import quote
from SPARQLWrapper import SPARQLWrapper
import pathlib
from rdflib import Graph, Literal, RDF, URIRef

def add(server, g_url, f_n, date_str, type_file):
    server = SPARQLWrapper(server)
    server.method = 'POST'
    my_query = 'LOAD <' + pathlib.Path(abspath(f_n)).as_uri() + '> INTO GRAPH <' + g_url + '>'
    server.setQuery(my_query)
    server.query()

    with open("updatetp_report_%s_%s.txt" % (type_file, date_str), "a", 
              encoding="utf8") as h:
        h.write("Added file '%s'\n" % f_n)

def remove(server, g_url, f_n, date_str, type_file, n):
    server = SPARQLWrapper(server)
    server.method = 'POST'
    file_path = pathlib.Path(abspath(f_n)).as_uri()

    #remove all the triples from the specified file
    g = Graph()
    g.parse(file_path, format="nt11" )

    i = 0
    triples_group = ""

    for index, (s, p, o) in enumerate(g):
        triple = "<" + str( s ) + ">" + "<" + str( p ) + ">" + "<" + str( o ) + ">" + "."
        i += 1
        if i == int(n):
            triples_group = triples_group + triple + " "
            i = 0
            my_query = 'DELETE DATA {GRAPH <' + g_url + '> {' + triples_group + '} }'
            server.setQuery(my_query)
            server.query()
            triples_group = ""

        else:
            triples_group = triples_group + triple + " "

    if triples_group != "":
        triples_group = triples_group + triple + " "
        my_query = 'DELETE DATA {GRAPH <' + g_url + '> {' + triples_group + '} }'
        server.setQuery( my_query )
        server.query()

    with open("updatetp_report_%s_%s.txt" % (type_file, date_str), "a",
              encoding="utf8") as h:
        h.write("Removed triples from file '%s'\n" % f_n)


if __name__ == "__main__":
    arg_parser = ArgumentParser("updatetp.py", description="Update a triplestore with a given "
                                                           "input .nt/.ttl file of new triples and "
                                                           "the graph enclosing them. Use .ttl file "
                                                           "if you need to preserve UTF-8 encoding.")
    arg_parser.add_argument("-s", "--sparql_endpoint",
                            dest="se_url", required=True,
                            help="The URL of the SPARQL endpoint.")
    arg_parser.add_argument("-i", "--input_file", dest="input_file", required=True,
                            help="The path to the NT file to upload on the triplestore.")
    arg_parser.add_argument("-i_r", "--input_file_r", dest="input_file_r", required=True,
                            help="The path to the NT file whose triples are to remove from the triplestore.")
    arg_parser.add_argument("-g", "--graph", dest="graph_name", required=True,
                            help="The graph URL to associate to the triples.")
    arg_parser.add_argument("-f", "--force", dest="force", default=False, action="store_true",
                            help="Force the creation of the triples associated to the input graph.")
    arg_parser.add_argument("-n", "--number", dest="number", required=True,
                            help="Number of triples after which the query to remove triples from the triplestore "
                                 "is performed")

    args = arg_parser.parse_args()

    SE_URL = args.se_url
    INPUT_FILE = args.input_file
    INPUT_FILE_R = args.input_file_r
    GRAPH_URL = args.graph_name
    TRIPLES_NUM = args.number
    date_str = datetime.now().strftime('%Y-%m-%dT%H%M%S')
    type_file = "prov" if "prov" + sep in INPUT_FILE else "data"

    if not args.force and type_file == "prov" and type_file not in GRAPH_URL:
        print("It seems that the graph specified is not the provenance graph.")
        exit(-1)

    if not args.force and type_file == "data" and "prov" in GRAPH_URL:
        print("It seems that the graph specified is not the data graph.")
        exit(-1)

    print("# Process starts")

    already_done = set()
    for file in glob("updatetp_report_%s_*.txt" % type_file):
        with open(file, encoding="utf8") as f:
            for line in f.readlines():
                already_done.add(basename(sub("^.+'([^']+)'.*$", "\\1", line)).strip())

    all_files = []
    if isdir(INPUT_FILE):
        for cur_dir, cur_subdir, cur_files in walk(INPUT_FILE):
            for cur_file in cur_files:
                cur_file_abs_path = cur_dir + sep + cur_file
                if basename(cur_file_abs_path) not in already_done and \
                        (cur_file_abs_path.endswith(".nt") or cur_file_abs_path.endswith(".ttl")):
                    all_files.append(cur_file_abs_path)
    else:
        all_files.append(INPUT_FILE)

    print("%s files to upload." % str(len(all_files)))

    for idx, cur_file in enumerate(all_files):
        print("\nUploading file '%s'" % cur_file)
        add(SE_URL, GRAPH_URL, cur_file, date_str, type_file)
        print("ADD: Done.")

#extension for removing triples
    all_files_r = []
    if isdir(INPUT_FILE_R):
        for cur_dir, cur_subdir, cur_files in walk(INPUT_FILE_R):
            for cur_file in cur_files:
                cur_file_abs_path = cur_dir + sep + cur_file
                if basename(cur_file_abs_path) not in already_done and \
                        (cur_file_abs_path.endswith(".nt") or cur_file_abs_path.endswith(".ttl")):
                    all_files_r.append(cur_file_abs_path)
    else:
        all_files_r.append(INPUT_FILE_R)

    print("%s files to remove." % str(len(all_files_r)))

    for idx, cur_file in enumerate(all_files_r):
        print("\nRemoving triples from file '%s'" % cur_file)
        remove(SE_URL, GRAPH_URL, cur_file, date_str, type_file, TRIPLES_NUM)
        print("REMOVE: Done.")

    print("# Process ends")

# sample: python -m index.storer.updatetp -s "http://localhost:3001/blazegraph/sparql" -i "index/test_data/mapping_test_output_1/triples_to_add" -i_r "index/test_data/mapping_test_output_1/triples_to_remove" -g "https://w3id.org/oc/index/noci/" -n 3

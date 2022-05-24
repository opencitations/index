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

import unittest
from os import makedirs
from os.path import join, exists
from index.python.src.preprocessing.datacite_pp import DatacitePreProcessing
#to be changed in : from oc.index.preprocessing.datacite_pp import DatacitePreProcessing
import shutil
import json

class DOCIPPTest(unittest.TestCase):
    """This class aims at testing the methods of the class DatacitePreProcessing."""
    def setUp(self):
        test_dir = join("index", "python", "test", "data")
        self.input_dir = join(test_dir, "doci_pp_dump_input")
        self.output_dir = join(test_dir, "doci_pp_dump_output")
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        makedirs(self.output_dir)
        self.assertTrue(exists(self.output_dir))
        self.num = 77
        self.DatacitePP = DatacitePreProcessing()

        #note: data concerning the dois listed below were modified for testing purposes.
        self.ent_glob_only = "10.1001/jama.289.8.989"
        self.ent_parser_only = "10.1002/2015jc010802"
        self.ent_no_data = "10.1016/j.gene.2017.10.006"



    def test_dump_filter_and_split(self):
        self.DatacitePP.dump_filter_and_split(self.input_dir, self.output_dir, self.num)
        all_files = self.DatacitePP.get_all_files(self.output_dir)
        for_parser_only = False
        for_glob_only = False
        for file_idx, file in enumerate(all_files):
            with open(file, "r", encoding="utf-8") as f:
                dict_from_json = json.load(f)
                for dict in dict_from_json["data"]:
                    # check that the entities with neither related identifiers nor other information for the glob were discarded
                    self.assertNotEqual(self.ent_no_data, dict["id"])
                    # check that the entities with at least either related identifiers or other information for the glob were kept
                    if dict["id"] == self.ent_glob_only:
                        for_glob_only = True
                    elif dict["id"] == self.ent_parser_only:
                        for_parser_only = True
        self.assertTrue(for_parser_only)
        self.assertTrue(for_glob_only)






    def test_counter_check(self):
        self.DatacitePP.dump_filter_and_split(self.input_dir, self.output_dir, self.num)
        # check that all the output files contain the number of entities specified in input, except for the last one
        all_files = self.DatacitePP.get_all_files(self.output_dir)
        for file_idx, file in enumerate(all_files):
            if "rest" not in file:
                with open(file, "r", encoding="utf-8") as f:
                    dict_from_json = json.load(f)
                    self.assertEqual(len(dict_from_json["data"]), self.num)
            else:
                with open( file, "r", encoding="utf-8" ) as f:
                    dict_from_json = json.load(f)
                    self.assertLessEqual(len(dict_from_json["data"]), self.num)







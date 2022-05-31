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
from os.path import join, exists
from index.python.src.preprocessing.nih_pp import NIHPreProcessing # TO DO: to be changed in : from oc.index.preprocessing.datacite_pp import DatacitePreProcessing
import shutil
import csv


class NOCIPPTest(unittest.TestCase):
    """This class aims at testing the methods of the class DatacitePreProcessing."""

    def setUp(self):
        test_dir = join("index", "python", "test", "data")
        self.input_dir = join(test_dir, "noci_pp_dump_input")
        self.output_dir = join(test_dir, "noci_pp_dump_output")
        self.num_0 = 356
        self.num_1 = 8
        self.num_2 = 5
        self.num_3 = 300
        self.num_4 = 4
        self.NIHPP = NIHPreProcessing()
        self.lines_sample = [[1, 14161139], [1, 14323813], [1, 4990046], [1, 4988806], [2, 4150960], [2, 4356257], [2, 4846745], [2, 4357832]]
        self.headers = ["citing", "referenced"]


    def test_dump_split(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        self.NIHPP.dump_split(self.input_dir, self.output_dir, self.num_3)

        #checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_dir))

        #checks that the input lines where stored in 2 files, one containing 300 items and the other the remaining 56
        files = self.NIHPP.get_all_files( self.output_dir)
        len_files = len(files)
        self.assertEqual(len_files, 2)
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_lines = len(list(reader))
                self.assertTrue(len_lines == 300 or len_lines == 56)


    def test_chunk_to_file(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        self.NIHPP.chunk_to_file(self.num_1, self.num_4, self.output_dir, self.headers, self.lines_sample)

        #checks that chunk_to_file recreates the output directory
        self.assertTrue(exists(self.output_dir))

        #checks that the input lines are correctly stored in a file
        files = self.NIHPP.get_all_files(self.output_dir)
        self.assertGreater(len(files), 0)

        #CSVFile_2.csv : target number (num_4) is 4 and current number (num_1) is 8 --> 8%4 == 0,  8//4  == 2
        self.assertTrue(exists(join(self.output_dir, "CSVFile_2.csv")))

        #runs again the process using a current number which is not a multiple of the target number, i.e.: 5
        shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        self.NIHPP.chunk_to_file(self.num_2, self.num_4, self.output_dir, self.headers, self.lines_sample)

        #checks that chunk_to_file recreates the output directory
        self.assertTrue(exists(self.output_dir))

        #checks that the input lines are correctly stored in a file
        files = self.NIHPP.get_all_files(self.output_dir)
        self.assertGreater(len(files), 0)

        #CSVFile_Rem.csv : target number (num_4) is 4 and current number (num_1) is 8 --> 8%4 == 0,  8//4  == 2
        self.assertTrue(exists(join(self.output_dir, "CSVFile_Rem.csv")))
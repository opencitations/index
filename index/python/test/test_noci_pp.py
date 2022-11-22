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
import os.path
from oc.index.preprocessing.nih_pp import NIHPreProcessing
import shutil
import csv
import os
import pandas as pd


class NOCIPPTest(unittest.TestCase):
    """This class aims at testing the methods of the classes NIHPreProcessing and ICiteMDPreProcessing."""

    def setUp(self):
        self.test_dir = join("index", "python", "test", "data")
        self.req_type = ".csv"
        self.num_0 = 356
        self.num_1 = 8
        self.num_2 = 5
        self.num_3 = 300
        self.num_4 = 4

        # NIH-OCC data, for NOCI parser
        self.input_type = "occ"
        self.input_dir = join(self.test_dir, "noci_pp_dump_input")
        self.output_dir = self.__get_output_directory("noci_pp_dump_output")
        self.output_dir_broken = join(self.test_dir, "noci_index_dump_output_broken")

        # iCite Metadata, for NOCI glob
        self.input_type_md = "icmd"
        self.input_md_dir = join(self.test_dir, "noci_md_pp_dump_input")
        self.output_md_dir = self.__get_output_directory("noci_md_pp_dump_output")
        self.output_md_dir_broken = join(self.test_dir, "noci_glob_dump_output_broken")

    def __get_output_directory(self, directory):
        directory = join(".", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_split_input(self):
        if exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        self.assertFalse(exists(self.output_dir))
        self.NIHPP = NIHPreProcessing(self.input_dir, self.output_dir, self.num_3, self.input_type)
        self.NIHPP.split_input()

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_dir))

        # checks that the input lines where stored in the corret number of files, containing the same number of lines as the input file
        input_files, targz_fd = self.NIHPP.get_all_files(self.input_dir, self.req_type)
        len_total_lines = 0
        for idx, file in enumerate(input_files):
            df = pd.read_csv(file, usecols=self.NIHPP._filter, low_memory=True)
            df.fillna("", inplace=True)
            df_dict_list = df.to_dict("records")
            len_total_lines += len(df_dict_list)

        expected_num_files = len_total_lines // self.num_3 if len_total_lines % self.num_3 == 0 else len_total_lines // self.num_3 + 1

        files, targz_fd = self.NIHPP.get_all_files(self.output_dir, self.req_type)
        len_files = len(files)
        self.assertEqual(len_files, expected_num_files)
        len_lines_output = 0
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_lines_output += len(list(reader))

        self.assertEqual(len_total_lines, len_lines_output)

    def test_icmd_split(self):
        if exists(self.output_md_dir):
            shutil.rmtree(self.output_md_dir)
        self.assertFalse(exists(self.output_md_dir))
        self.NIHPPmd = NIHPreProcessing(self.input_md_dir, self.output_md_dir, self.num_3, self.input_type_md)
        self.NIHPPmd.split_input()

        # checks that the output directory is generated in the process.
        self.assertTrue(exists(self.output_md_dir))

        # checks that the input lines where stored in the correct number of files, with respect to the parameters specified.
        # checks that the number of filtered lines is equal to the number of lines in input - the number of discarded lines
        input_files, targz_fd = self.NIHPPmd.get_all_files(self.input_md_dir, self.req_type)
        len_discarded_lines = 0
        len_total_lines = 0 
        for idx, file in enumerate(input_files):
            df = pd.read_csv(file, usecols=self.NIHPPmd._filter, low_memory=True)
            df.fillna("", inplace=True)
            df_dict_list = df.to_dict("records")
            len_total_lines += len(df_dict_list)
            len_discarded_lines += len([d for d in df_dict_list if not (d.get("cited_by") or d.get("references"))])

        expected_num_files = (len_total_lines - len_discarded_lines) // self.num_3 if (len_total_lines - len_discarded_lines) % self.num_3 == 0 else (len_total_lines - len_discarded_lines) // self.num_3 + 1
        files, targz_fd = self.NIHPPmd.get_all_files(self.output_md_dir, self.req_type)
        len_files = len(files)
        self.assertEqual(len_files, expected_num_files)

        len_filtered_lines = 0
        for idx, file in enumerate(files):
            with open(file, "r") as op_file:
                reader = csv.reader(op_file, delimiter=",")
                next(reader, None)
                len_filtered_lines += len(list(reader))
                
        self.assertEqual(len_filtered_lines, len_total_lines - len_discarded_lines)

    def test_continue_broken_process_glob(self):
        NIHppBroken = NIHPreProcessing(self.input_md_dir, self.output_md_dir_broken, 50, self.input_type_md)
        lines_in_input_dir = []
        files_in_input = NIHppBroken.get_all_files(self.input_md_dir, self.req_type)[0]

        for idx, file in enumerate(files_in_input):
            with open(file, 'r') as myFile:
                reader = csv.DictReader(myFile)
                myList = list(reader)
                lines_in_input_dir.extend([l for l in myList if (l.get("cited_by") or l.get("references"))])

        pre_break_files = NIHppBroken.get_all_files(self.output_md_dir_broken, self.req_type)[0]
        pre_break_lines = []
        for idx, file in enumerate(pre_break_files):
            with open(file, 'r') as myFile:
                reader = csv.DictReader(myFile)
                myList = list(reader)
                pre_break_lines.extend([l for l in myList])

        NIHppBroken.split_input()
        final_files = NIHppBroken.get_all_files(self.output_md_dir_broken, self.req_type)[0]
        added_lines = []
        for idx, file in enumerate(final_files):
            with open(file, 'r') as myFile:
                reader = csv.DictReader(myFile)
                myList = list(reader)
                added_lines.extend([l for l in myList])
        files = [file for file in final_files if file not in pre_break_files]
        for file in files:
            os.remove(file)
        self.assertEqual(len(lines_in_input_dir), len(added_lines))
        pmids_in_input = [item["pmid"] for item in lines_in_input_dir]
        pmids_in_output = [item["pmid"] for item in added_lines]
        self.assertTrue(all(item in pmids_in_input for item in pmids_in_output))

    # def test_continue_broken_process_index(self):
    #     NIHppBroken_index = NIHPreProcessing(self.input_dir, self.output_dir_broken, 50, self.input_type)
    #     lines_in_input_dir = []
    #     files_in_input = NIHppBroken_index.get_all_files(self.input_dir, self.req_type)[0]
    #
    #     for idx, file in enumerate(files_in_input):
    #         with open(file, 'r') as myFile:
    #             reader = csv.DictReader(myFile)
    #             myList = list(reader)
    #             lines_in_input_dir.extend([l for l in myList])
    #
    #     pre_break_files = NIHppBroken_index.get_all_files(self.output_dir_broken, self.req_type)[0]
    #     pre_break_lines = []
    #     for idx, file in enumerate(pre_break_files):
    #         with open(file, 'r') as myFile:
    #             reader = csv.DictReader(myFile)
    #             myList = list(reader)
    #             pre_break_lines.extend([l for l in myList])
    #
    #     NIHppBroken_index.split_input()
    #     final_files = NIHppBroken_index.get_all_files(self.output_dir_broken, self.req_type)[0]
    #     added_lines = []
    #     for idx, file in enumerate(final_files):
    #         with open(file, 'r') as myFile:
    #             reader = csv.DictReader(myFile)
    #             myList = list(reader)
    #             added_lines.extend([l for l in myList])
    #     files = [file for file in final_files if file not in pre_break_files]
    #     for file in files:
    #         os.remove(file)
    #     self.assertEqual(len(lines_in_input_dir), len(added_lines))
    #     pmids_in_input = [item for item in lines_in_input_dir]
    #     pmids_in_output = [item for item in added_lines]
    #     self.assertTrue(all(item in pmids_in_input for item in pmids_in_output))







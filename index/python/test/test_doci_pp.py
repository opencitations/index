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

import json
import unittest
from oc.index.preprocessing.datacite_pp import DatacitePreProcessing
from os.path import exists
import os.path
from os import sep, makedirs, walk
import pandas as pd
import shutil
import math

class PreprocessingTest(unittest.TestCase):
        def setUp(self):
            self._input_dir_dc = "index/python/test/data/preprocess/data_datacite"
            self._input_compr = "index/python/test/data/preprocess/dc_pp_input.json.zst"
            self._output_dir_dc_lm = "index/python/test/data/preprocess/tmp_data_datacite_lm"
            self._output_dir_dc_nlm = "index/python/test/data/preprocess/tmp_data_datacite_nlm"
            self._output_dir_compr_nlm = "index/python/test/data/preprocess/tmp_data_datacite_c_nlm"
            self._output_dir_compr_lm = "index/python/test/data/preprocess/tmp_data_datacite_c_lm"
            self._interval = 78
            self._relation_type_datacite = ["references", "isreferencedby", "cites", "iscitedby"]
            self._out_dir_broken_process_compr = "index/python/test/data/preprocess/tmp_data_datacite_c_broken"
            self._out_dir_broken_process = "index/python/test/data/preprocess/tmp_data_datacite_broken"


        def test_dc_preprocessing(self):
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval)
            if exists(self._output_dir_dc_lm):
                shutil.rmtree(self._output_dir_dc_lm)
            makedirs(self._output_dir_dc_lm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_dir_dc, self._dc_pp._req_type)[0]:
                f = open(file)
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()
            len_out_files = len([name for name in os.listdir(self._output_dir_dc_lm) if os.path.isfile(os.path.join(self._output_dir_dc_lm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_dc_lm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)
            shutil.rmtree(self._output_dir_dc_lm)

        def test_dc_preprocessing_no_low_memory(self):
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_nlm, self._interval, low_memo=False)
            if exists(self._output_dir_dc_nlm):
                shutil.rmtree(self._output_dir_dc_nlm)
            makedirs(self._output_dir_dc_nlm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_dir_dc, self._dc_pp._req_type)[0]:
                f = open(file)
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()

            len_out_files = len([name for name in os.listdir(self._output_dir_dc_nlm) if os.path.isfile(os.path.join(self._output_dir_dc_nlm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_dc_nlm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)
            shutil.rmtree(self._output_dir_dc_nlm)

        def test_dc_preprocessing_compr(self):
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._output_dir_compr_lm, self._interval)
            if exists(self._output_dir_compr_lm):
                shutil.rmtree(self._output_dir_compr_lm)
            makedirs(self._output_dir_compr_lm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_compr, self._dc_pp._req_type)[0]:
                f = open(file)
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()
            len_out_files = len([name for name in os.listdir(self._output_dir_compr_lm) if os.path.isfile(os.path.join(self._output_dir_compr_lm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_compr_lm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)
            shutil.rmtree(self._output_dir_compr_lm)

        def test_dc_preprocessing_no_low_memory_compr(self):
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._output_dir_compr_nlm, self._interval, low_memo=False)
            if exists(self._output_dir_compr_nlm):
                shutil.rmtree(self._output_dir_compr_nlm)
            makedirs(self._output_dir_compr_nlm)
            self._dc_pp.split_input()
            len_lines_input = 0
            for file in self._dc_pp.get_all_files(self._input_compr, self._dc_pp._req_type)[0]:
                f = open(file)
                lines_with_relids = [json.loads(line) for line in f if json.loads(line).get("attributes").get("relatedIdentifiers")]
                lines_with_citations = []
                if lines_with_relids:
                    lines_with_needed_fields = [line for line in lines_with_relids if [i for i in line["attributes"]["relatedIdentifiers"] if (i.get("relatedIdentifierType") and i.get("relationType") and i.get("relatedIdentifier"))]]
                    if lines_with_needed_fields:
                        lines_with_citations = [line for line in lines_with_needed_fields if [i for i in line["attributes"]["relatedIdentifiers"] if (i["relatedIdentifierType"].lower()=="doi" and i["relationType"].lower() in self._relation_type_datacite)]]
                len_lines_input = len(lines_with_citations)
                f.close()

            len_out_files = len([name for name in os.listdir(self._output_dir_compr_nlm) if os.path.isfile(os.path.join(self._output_dir_compr_nlm, name))])

            self.assertTrue(len(self._dc_pp.get_all_files(self._output_dir_compr_nlm, self._dc_pp._req_type)[0]) > 0)
            self.assertEqual(math.ceil(len_lines_input/self._interval), len_out_files)
            shutil.rmtree(self._output_dir_compr_nlm)

        def test_dc_broken_preprocessing_compr(self):
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._out_dir_broken_process_compr, self._interval)
            already_processed_files = []
            for file in os.listdir(self._out_dir_broken_process_compr):
                already_processed_files.append(file)
            self._dc_pp.split_input()
            new_files = [f for f in os.listdir(self._out_dir_broken_process_compr) if f not in already_processed_files]
            nfpath = os.path.join(self._out_dir_broken_process_compr, new_files[0])
            f = open(nfpath)
            new_file_data = json.load(f)
            f.close()
            self.assertTrue(new_file_data["data"][0]["id"] == '10.1017/s0031182017001779')

            for nf in new_files:
                os.remove(os.path.join(self._out_dir_broken_process_compr, nf))

        def test_dc_broken_preprocessing(self):
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._out_dir_broken_process, self._interval)
            already_processed_files = []
            for file in os.listdir(self._out_dir_broken_process):
                already_processed_files.append(file)
            self._dc_pp.split_input()
            new_files = [f for f in os.listdir(self._out_dir_broken_process) if f not in already_processed_files]
            nfpath = os.path.join(self._out_dir_broken_process, new_files[0])
            f = open(nfpath)
            new_file_data = json.load(f)
            f.close()
            self.assertTrue(new_file_data["data"][0]["id"] == '10.1017/s0031182017001779')

            for nf in new_files:
                os.remove(os.path.join(self._out_dir_broken_process, nf))


if __name__ == '__main__':
    unittest.main()

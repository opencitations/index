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
from oc.index.preprocessing.datacite import DatacitePreProcessing
from os.path import exists, join
import os.path
from os import listdir
import shutil
import math
import glob
import csv


class PreprocessingTest(unittest.TestCase):
        def setUp(self):
            self.test_dir = join("index", "python","test", "data", "preprocess")
            self._input_dir_dc = join(self.test_dir, "data_datacite")
            self._input_dir_cit = join(self.test_dir, "data_datacite_sample")
            self._input_compr = join(self.test_dir, "dc_pp_input.json.zst")
            self._output_dir_dc_lm = join(self.test_dir, "tmp_data_datacite_lm")
            self._output_dir_dc_nlm = join(self.test_dir, "tmp_data_datacite_nlm")
            self._output_dir_compr_nlm = join(self.test_dir, "tmp_data_datacite_c_nlm")
            self._output_dir_compr_lm = join(self.test_dir, "tmp_data_datacite_c_lm")
            self._output_dir_cit = join(self.test_dir, "tmp_data_datacite_cit")
            self._output_dir_cit2 = join(self.test_dir, "tmp_data_datacite_cit2")
            self._interval = 78
            self._relation_type_datacite = ["references", "isreferencedby", "cites", "iscitedby"]
            self._out_dir_broken_process_compr = join(self.test_dir, "tmp_data_datacite_c_broken")
            self._out_dir_broken_process = join(self.test_dir, "tmp_data_datacite_broken")
            self._out_dir_csv_1 = join(self.test_dir, "datacite_csv_cit_1")
            self._out_dir_csv_2 = join(self.test_dir, "datacite_csv_cit_2")
            self._out_dir_dupl_check = self._output_dir_cit2 + "_citations"
            self._process_meta = "meta"
            self._process_index = "index"

        def test_dc_preprocessing(self):
            if exists(self._output_dir_dc_lm):
                shutil.rmtree(self._output_dir_dc_lm)
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_lm, self._interval, self._process_meta)
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
            if exists(self._output_dir_dc_nlm):
                shutil.rmtree(self._output_dir_dc_nlm)
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._output_dir_dc_nlm, self._interval, self._process_meta, low_memo=False)
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
            if exists(self._output_dir_compr_lm):
                shutil.rmtree(self._output_dir_compr_lm)
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._output_dir_compr_lm, self._interval, self._process_meta)
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
            if exists(self._output_dir_compr_nlm):
                shutil.rmtree(self._output_dir_compr_nlm)
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._output_dir_compr_nlm, self._interval, self._process_meta, low_memo=False)
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
            self._dc_pp = DatacitePreProcessing(self._input_compr, self._out_dir_broken_process_compr, self._interval, self._process_meta)
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
            self._dc_pp = DatacitePreProcessing(self._input_dir_dc, self._out_dir_broken_process, self._interval, self._process_meta)
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

        def test_citations_preprocessing(self):
            if exists(self._output_dir_cit):
                shutil.rmtree(self._output_dir_cit)
            out_dir_csv_2 = self._output_dir_cit + "_citations"

            self._dc_pp_cit = DatacitePreProcessing(self._input_dir_cit, self._output_dir_cit, self._interval, self._process_index)
            self._dc_pp_cit.split_input()
            expected_citations_set = {("10.1002/2013jc009302","10.1002/2014gb004975"),
                                      ("10.1016/0304-4203(74)90015-2","10.1002/2014gb004975"),
                                      ("10.1002/2014gl061020", "10.1029/2000jc000355"),
                                      ("10.1002/2014gl061020","10.1029/2011gl050078")}
            processed_citations = set()
            out_dir_p = listdir(out_dir_csv_2)
            if len(out_dir_p) != 0:
                list_of_csv = glob.glob(join(out_dir_csv_2, '*.csv'))
                for file in list_of_csv:
                    with open(file, 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        next(csv_reader)
                        citations = [tuple(x) for x in csv_reader]
                        processed_citations.update(citations)
            self.assertEqual(processed_citations, expected_citations_set)
            shutil.rmtree(out_dir_csv_2)
            shutil.rmtree(self._output_dir_cit)

        def test_citations_preprocessing_duplicates(self):
            if exists(self._output_dir_cit2):
                shutil.rmtree(self._output_dir_cit2)
            out_dir_p = listdir(self._out_dir_dupl_check)
            self._dc_pp_cit = DatacitePreProcessing(self._input_dir_cit, self._output_dir_cit2, self._interval, self._process_index)
            out_dir_p = listdir(self._out_dir_dupl_check)
            citations_before_process = set()
            list_of_csv_before_process = glob.glob(join(self._out_dir_dupl_check, '*.csv'))
            if len(out_dir_p) != 0:
                for file in list_of_csv_before_process:
                    with open(file, 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        next(csv_reader)
                        citations = [tuple(x) for x in csv_reader]
                        citations_before_process.update(citations)
            expected_citations_before_process = {("10.1002/2013jc009302","10.1002/2014gb004975"),
                                                 ("10.1016/0304-4203(74)90015-2","10.1002/2014gb004975"),
                                                 ("10.1002/2014gl061020", "10.1029/2000jc000355")}
            self.assertEqual(citations_before_process, expected_citations_before_process)

            self._dc_pp_cit.split_input()

            expected_citations_set = {("10.1002/2013jc009302","10.1002/2014gb004975"),
                                      ("10.1016/0304-4203(74)90015-2","10.1002/2014gb004975"),
                                      ("10.1002/2014gl061020", "10.1029/2000jc000355"),
                                      ("10.1002/2014gl061020","10.1029/2011gl050078")}
            processed_citations = set()
            out_dir_p_post = listdir(self._out_dir_dupl_check)
            if len(out_dir_p_post) != 0:
                list_of_csv_post = glob.glob(join(self._out_dir_dupl_check, '*.csv'))
                for file in list_of_csv_post:
                    with open(file, 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        next(csv_reader)
                        citations = [tuple(x) for x in csv_reader]
                        processed_citations.update(citations)
            self.assertEqual(processed_citations, expected_citations_set)
            shutil.rmtree(self._output_dir_cit2)

            list_of_csv_post_process = glob.glob(join(self._out_dir_dupl_check, '*.csv'))
            new_files = [f for f in list_of_csv_post_process if f not in list_of_csv_before_process]
            for nf in new_files:
                os.remove(nf)


if __name__ == '__main__':
    unittest.main()

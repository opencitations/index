import unittest
from os import sep, remove, makedirs
import os
from os.path import exists, join
import shutil
from shutil import rmtree
import pandas as pd
import json

from oc.index.glob.csv import CSVDataSource
from oc.index.glob.redis import RedisDataSource
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.pmid import PMIDManager
from oc.index.utils.config import get_config

from oc.index.scripts.glob_doci import (
    DataCiteResourceFinder,
    load_json_doci,
    process_doci,
)

from oc.index.scripts.glob_noci import (
    issn_data_recover_noci,
    issn_data_to_cache_noci,
    build_pubdate_noci,
    process_noci,
)


from oc.index.scripts.glob_crossref import (
    build_pubdate_coci,
    get_all_files_coci,
    load_json_coci,
    process_coci,
)


class GlobTest(unittest.TestCase):
    def setUp(self):
        config = get_config()
        self.test_dir = join("index", "python", "test", "data")
        self.doi_manager = DOIManager()
        self.pmid_manager = PMIDManager()
        self.issn_manager = ISSNManager()
        self.orcid_manager = ORCIDManager()

        # Initialize datasource
        self.noci_datasource = None
        self.doci_datasource = None
        self.coci_datasource = None

        # COCI
        self.inp_coci = join(self.test_dir, "crossref_glob_dump_input")
        self.out_coci = self.__get_output_directory("crossref_glob_dump_output")
        self.dir_get_all_files_coci = join(self.test_dir, "crossref_glob_dump_input")
        self.sample_doi_coci = self.doi_manager.normalise("10.7717/peerj.4375", True)
        self.sample_reference_coci = self.doi_manager.normalise(
            "10.1016/j.joi.2016.08.002", True
        )
        self.sample_doi_coci_2 = self.doi_manager.normalise(
            "10.1016/j.websem.2017.06.001", True
        )
        self.obj_for_date = {"issued": {"date-parts": [[2017, 5]]}}
        self.obj_for_date_2 = {"issued": {"date-parts": [[2018, 2, 13]]}}
        self.obj_for_date_3 = {"issued": {"date-parts": [[2015, 3, 9]]}}
        self.load_json_c_inp = join(self.inp_coci, "crossref_dump.json")
        self.coci_id_valid = config.get("COCI_T", "valid_id")
        self.coci_id_date = config.get("COCI_T", "id_date")
        self.coci_id_orcid = config.get("COCI_T", "id_orcid")
        self.coci_id_issn = config.get("COCI_T", "id_issn")

        # DOCI
        self.inp_doci = join(self.test_dir, "doci_glob_dump_input")
        self.issn_journal_doci = {
            "european journal of organic chemistry": ["1434193X"],
            "drug delivery and translational research": ["2190-3948"],
            "the social science journal": ["1873-5355"],
        }
        self.n_doci = 3
        self.dir_issn_map_doci = join(self.test_dir, "recover_w_mapping_doci")
        self.dir_no_issn_map_doci = join(self.test_dir, "recover_wo_mapping_doci")
        self.dir_data_to_cache_doci = join(self.test_dir, "issn_data_to_cache_doci")
        self.dir_get_all_files_doci = self.__get_output_directory("doci_pp_dump_output")
        self.sample_reference_doci = self.doi_manager.normalise(
            "10.1002/anie.200504236", True
        )
        self.load_json_d_inp = join(self.inp_doci, "doci_dump.json")
        self.doci_id_valid = config.get("DOCI_T", "valid_id")
        self.doci_id_date = config.get("DOCI_T", "id_date")
        self.doci_id_orcid = config.get("DOCI_T", "id_orcid")
        self.doci_id_issn = config.get("DOCI_T", "id_issn")
        # NOCI
        self.inp_noci = join(self.test_dir, "noci_glob_dump_input")
        self.inp_noci_map = join(self.test_dir, "noci_glob_input_doi_orcid_map")
        self.out_noci = self.__get_output_directory("noci_glob_dump_output")
        self.id_orcid_map = join(
            self.test_dir, "noci_id_orcid_map_zip", "doi_orcid_mapping.zip"
        )
        self.n_noci = 3
        self.issn_journal_noci = {
            "N Biotechnol": ["1871-6784"],
            "Biochem Med": ["0006-2944"],
            "Magn Reson Chem": ["0749-1581"],
        }
        self.dir_issn_map_noci = join(self.test_dir, "recover_w_mapping_noci")
        self.dir_no_issn_map_noci = join(self.test_dir, "recover_wo_mapping_noci")
        self.dir_data_to_cache_noci = join(self.test_dir, "issn_data_to_cache_noci")
        self.dir_get_all_files_noci = self.__get_output_directory(
            "noci_md_pp_dump_output"
        )
        self.csv_sample = join(self.inp_noci, "CSVFile_1.csv")
        self.sample_reference_noci = self.pmid_manager.normalise("4150960", True)
        self.noci_id_valid = config.get("NOCI_T", "valid_id")
        self.noci_id_date = config.get("NOCI_T", "id_date")
        self.noci_id_orcid = config.get("NOCI_T", "id_orcid")
        self.noci_id_issn = config.get("NOCI_T", "id_issn")

    def __get_output_directory(self, directory):
        directory = join(".", "tmp", directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def test_build_pubdate_coci(self):
        self.assertEqual(build_pubdate_coci(self.obj_for_date), "2017-05")
        self.assertEqual(build_pubdate_coci(self.obj_for_date_2), "2018-02-13")
        self.assertEqual(build_pubdate_coci(self.obj_for_date_3), "2015-03-09")

    def test_load_json_coci(self):
        self.assertTrue(
            isinstance(load_json_coci(self.load_json_c_inp, None, 1, 1), dict)
        )

    def test_process_coci(self):
        if exists(self.coci_id_date):
            remove(self.coci_id_date)
        if exists(self.coci_id_issn):
            remove(self.coci_id_issn)
        if exists(self.coci_id_valid):
            remove(self.coci_id_valid)
        if exists(self.coci_id_orcid):
            remove(self.coci_id_orcid)
        process_coci(self.inp_coci, self.out_coci, process_type="test")
        self.coci_datasource = CSVDataSource("COCI_T")

        citing_doi = self.doi_manager.normalise(self.sample_doi_coci, True)
        citing_doi_2 = self.doi_manager.normalise(self.sample_doi_coci_2, True)
        self.assertEqual(
            self.coci_datasource.get(citing_doi_2)["orcid"],
            {"0000-0003-0530-4305", "0000-0002-7562-5203"},
        )
        self.assertEqual(self.coci_datasource.get(citing_doi)["valid"], {"v"})
        self.assertEqual(
            self.coci_datasource.get(self.sample_reference_coci)["valid"], {"v"}
        )
        self.assertEqual(self.coci_datasource.get(citing_doi)["date"], {"2018-02-13"})
        self.assertEqual(self.coci_datasource.get(citing_doi)["issn"], {"2167-8359"})
        if exists(self.coci_id_date):
            remove(self.coci_id_date)
        if exists(self.coci_id_issn):
            remove(self.coci_id_issn)
        if exists(self.coci_id_valid):
            remove(self.coci_id_valid)
        if exists(self.coci_id_orcid):
            remove(self.coci_id_orcid)


    # TEST DOCI GLOB
    def test_valid_date_doci(self):
        dcrf = DataCiteResourceFinder()
        self.assertTrue(isinstance(dcrf.Date_Validator(str(2018)), str))
        self.assertEqual(dcrf.Date_Validator("2018-11-25"), "2018-11-25")
        self.assertEqual(dcrf.Date_Validator("2015-07-14"), "2015-07-14")
        self.assertEqual(dcrf.Date_Validator("2015-2016"), "2015")
        self.assertEqual(dcrf.Date_Validator("2015-May"), "2015")
        self.assertIsNone(dcrf.Date_Validator("May 2015"))
        self.assertIsNone(dcrf.Date_Validator("14 2015"))
        self.assertIsNone(dcrf.Date_Validator("11-25-2018"))
        self.assertIsNone(dcrf.Date_Validator("25-11-2018"))

    def test_load_json_doci(self):
        self.assertTrue(
            isinstance(load_json_doci(self.load_json_d_inp, None, 1, 1), dict)
        )


    def test_process_doci(self):
        if exists(self.doci_id_date):
            remove(self.doci_id_date)
        if exists(self.doci_id_issn):
            remove(self.doci_id_issn)
        if exists(self.doci_id_valid):
            remove(self.doci_id_valid)
        if exists(self.doci_id_orcid):
            remove(self.doci_id_orcid)
        process_doci(self.inp_doci, process_type="test")
        self.doci_datasource = CSVDataSource("DOCI_T")

        citing_doi = "doi:10.1002/ejoc.201800947"
        self.assertEqual(
            self.doci_datasource.get(citing_doi)["orcid"], {"0000-0002-2397-9093"}
        )
        self.assertEqual(self.doci_datasource.get(citing_doi)["valid"], {"v"})
        self.assertEqual(
            self.doci_datasource.get(self.sample_reference_doci)["valid"], {"v"}
        )
        self.assertEqual(self.doci_datasource.get(citing_doi)["date"], {"2018-11-25"})
        self.assertEqual(self.doci_datasource.get(citing_doi)["issn"], {"1434-193X"})

        if exists(self.doci_id_date):
            remove(self.doci_id_date)
        if exists(self.doci_id_issn):
            remove(self.doci_id_issn)
        if exists(self.doci_id_valid):
            remove(self.doci_id_valid)
        if exists(self.doci_id_orcid):
            remove(self.doci_id_orcid)


    # TEST NOCI GLOB
    def test_issn_data_recover_noci(self):
        if exists(self.dir_no_issn_map_noci):
            rmtree(self.dir_no_issn_map_noci)
        makedirs(self.dir_no_issn_map_noci)
        if exists(self.dir_issn_map_noci):
            rmtree(self.dir_issn_map_noci)
        makedirs(self.dir_issn_map_noci)
        with open(
            join(self.dir_no_issn_map_noci, "journal_issn.json"), "w", encoding="utf-8"
        ) as g:
            json.dump({}, g, ensure_ascii=False, indent=4)
        with open(
            join(self.dir_issn_map_noci, "journal_issn.json"), "w", encoding="utf-8"
        ) as f:
            json.dump(self.issn_journal_noci, f, ensure_ascii=False, indent=4)

        # Test the case in which there is no mapping file for journals - issn
        self.assertEqual(issn_data_recover_noci(self.dir_no_issn_map_noci), {})
        # Test the case in which there is a mapping file for journals - issn
        self.assertNotEqual(issn_data_recover_noci(self.dir_issn_map_noci), {})

        rmtree(self.dir_no_issn_map_noci)
        rmtree(self.dir_issn_map_noci)


    def test_issn_data_to_cache_noci(self):
        filename = join(self.dir_data_to_cache_noci, "journal_issn.json")
        if not exists(self.dir_data_to_cache_noci):
            makedirs(self.dir_data_to_cache_noci)
        if exists(filename):
            remove(filename)
        self.assertFalse(exists(filename))
        issn_data_to_cache_noci(self.issn_journal_noci, self.dir_data_to_cache_noci)
        self.assertTrue(exists(filename))
        rmtree(self.dir_data_to_cache_noci)


    def test_build_pubdate_noci(self):
        df = pd.DataFrame()
        for chunk in pd.read_csv(self.csv_sample, chunksize=1000):
            f = pd.concat([df, chunk], ignore_index=True)
            f.fillna("", inplace=True)
            for index, row in f.iterrows():
                pub_date = build_pubdate_noci(row)
                self.assertTrue(isinstance(pub_date, str))
                self.assertTrue(isinstance(int(pub_date), int))
                self.assertEqual(len(pub_date), 4)


    def test_process_noci(self):
        if exists(self.noci_id_date):
            remove(self.noci_id_date)
        if exists(self.noci_id_issn):
            remove(self.noci_id_issn)
        if exists(self.noci_id_valid):
            remove(self.noci_id_valid)
        if exists(self.noci_id_orcid):
            remove(self.noci_id_orcid)
        for files in os.listdir(self.out_noci):
            path = os.path.join(self.out_noci, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

        process_noci(self.inp_noci, self.out_noci, self.n_noci, process_type="test")
        self.noci_datasource = CSVDataSource("NOCI_T")

        citing_pmid = "pmid:2"
        citing_pmid5 = "pmid:5"
        self.assertEqual(self.noci_datasource.get(citing_pmid)["orcid"], None)
        # self.assertEqual(self.noci_datasource.get(citing_pmid5)["orcid"], {"0000-0002-4762-5345"})
        # run the glob process with credetials to make this test assertion pass
        self.assertEqual(self.noci_datasource.get(citing_pmid)["valid"], {"v"})
        self.assertEqual(self.noci_datasource.get(citing_pmid)["date"], {"1975"})
        self.assertEqual(self.noci_datasource.get(citing_pmid)["issn"], {"0006-291X"})

        for files in os.listdir(self.out_noci):
            path = os.path.join(self.out_noci, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)
        if exists(self.noci_id_date):
            remove(self.noci_id_date)
        if exists(self.noci_id_issn):
            remove(self.noci_id_issn)
        if exists(self.noci_id_valid):
            remove(self.noci_id_valid)
        if exists(self.noci_id_orcid):
            remove(self.noci_id_orcid)

    def test_noci_process_doi_orcid_map(self):
        if exists(self.noci_id_date):
            remove(self.noci_id_date)
        if exists(self.noci_id_issn):
            remove(self.noci_id_issn)
        if exists(self.noci_id_valid):
            remove(self.noci_id_valid)
        if exists(self.noci_id_orcid):
            remove(self.noci_id_orcid)
        doi_orcid_decompr_dir = join(
            self.test_dir, "noci_id_orcid_map_zip", "doi_orcid_mapping_decompr_zip_dir"
        )

        for files in os.listdir(self.out_noci):
            path = os.path.join(self.out_noci, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

        # try with doi_orcid mapping folder
        process_noci(self.inp_noci_map, self.out_noci, 2, process_type="test", id_orcid_dir=self.id_orcid_map)
        self.assertTrue(len(os.listdir(doi_orcid_decompr_dir)) == 3)
        noci_datasource_m = CSVDataSource("NOCI_T")

        self.assertEqual({'0000-0001-8665-095X', '0000-0003-0530-4305', '0000-0001-5486-7070'}, noci_datasource_m.get("pmid:1000000001")["orcid"])
        self.assertEqual({'0000-0001-5366-5194', '0000-0001-5439-4576'}, noci_datasource_m.get("pmid:1000000002")["orcid"])
        self.assertEqual({'0000-0002-9812-4065', '0000-0001-7363-6737', '0000-0002-8420-0696'}, noci_datasource_m.get("pmid:1000000003")["orcid"])
        self.assertEqual({'0000-0001-5506-523X', '0000-0002-6279-3830'}, noci_datasource_m.get("pmid:1000000004")["orcid"])

        for files in os.listdir(self.out_noci):
            path = os.path.join(self.out_noci, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

        try:
            rmtree(doi_orcid_decompr_dir)
        except:
            os.remove(doi_orcid_decompr_dir)
        if exists(self.noci_id_date):
            remove(self.noci_id_date)
        if exists(self.noci_id_issn):
            remove(self.noci_id_issn)
        if exists(self.noci_id_valid):
            remove(self.noci_id_valid)
        if exists(self.noci_id_orcid):
            remove(self.noci_id_orcid)


if __name__ == "__main__":
    unittest.main()

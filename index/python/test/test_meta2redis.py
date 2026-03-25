#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import csv
import io
import json
import os
import tarfile
import tempfile
import unittest
from typing import Dict, Set, cast
from zipfile import ZipFile

import fakeredis

from oc.index.scripts.meta2redis import (
    _get_csv_files,
    _p_csvfile,
    get_att_ids,
    get_id_val,
    get_key_ids,
)


class TestGetKeyIds(unittest.TestCase):
    def test_single_id(self):
        result = get_key_ids("omid:br/0601")
        assert result == ["omid:br/0601"]

    def test_multiple_ids(self):
        result = get_key_ids("omid:br/0601 doi:10.1234/test openalex:W123")
        assert result == ["omid:br/0601", "doi:10.1234/test", "openalex:W123"]

    def test_empty_string(self):
        result = get_key_ids("")
        assert result == [""]


class TestGetAttIds(unittest.TestCase):
    def test_single_bracket(self):
        result = get_att_ids("[omid:ra/0601 orcid:0000-0001-1234-5678]")
        assert result == [["omid:ra/0601", "orcid:0000-0001-1234-5678"]]

    def test_multiple_brackets(self):
        result = get_att_ids("[omid:ra/0601 orcid:0000-0001-1234-5678]; [omid:ra/0602]")
        assert result == [["omid:ra/0601", "orcid:0000-0001-1234-5678"], ["omid:ra/0602"]]

    def test_no_brackets(self):
        result = get_att_ids("no brackets here")
        assert result == []

    def test_empty_brackets(self):
        result = get_att_ids("[]")
        assert result == [[]]


class TestGetIdVal(unittest.TestCase):
    def test_filter_omid(self):
        ids = ["omid:br/0601", "doi:10.1234/test", "omid:br/0602"]
        result = get_id_val(ids, ["omid"])
        assert result == ["omid:br/0601", "omid:br/0602"]

    def test_filter_multiple_types(self):
        ids = ["omid:br/0601", "doi:10.1234/test", "orcid:0000-0001-1234-5678"]
        result = get_id_val(ids, ["doi", "orcid"])
        assert result == ["doi:10.1234/test", "orcid:0000-0001-1234-5678"]

    def test_no_match(self):
        ids = ["omid:br/0601", "doi:10.1234/test"]
        result = get_id_val(ids, ["issn"])
        assert result == []

    def test_empty_list(self):
        result = get_id_val([], ["omid"])
        assert result == []


class FakeRedisDB:
    def __init__(self, fake_server: fakeredis.FakeServer, db: int):
        self.rconn = fakeredis.FakeRedis(server=fake_server, db=db, decode_responses=True)

    def flush_index(self, data: Dict[str, Set[str]]) -> None:
        pipe = self.rconn.pipeline()
        for _k, _v in data.items():
            if _v:
                pipe.sadd(_k, *_v)
        pipe.execute()

    def flush_metadata(self, data: Dict[str, str]) -> None:
        pipe = self.rconn.pipeline()
        for _k, _v in data.items():
            pipe.set(_k, _v)
        pipe.execute()

    def key_count(self) -> int:
        return cast(int, self.rconn.dbsize())

    def export_to_csv(self, filepath: str) -> None:
        with open(filepath, "w", newline="") as f:
            writer = csv.writer(f)
            for key in self.rconn.scan_iter():
                members = cast(Set[str], self.rconn.smembers(key))
                writer.writerow([key, "; ".join(members)])


class TestRedisDB(unittest.TestCase):
    def setUp(self):
        self.fake_server = fakeredis.FakeServer()
        self.rconn_br = FakeRedisDB(self.fake_server, 0)
        self.rconn_ra = FakeRedisDB(self.fake_server, 1)
        self.rconn_metadata = FakeRedisDB(self.fake_server, 2)

    def tearDown(self):
        self.rconn_br.rconn.flushdb()
        self.rconn_ra.rconn.flushdb()
        self.rconn_metadata.rconn.flushdb()

    def test_flush_index_single_value(self):
        data = {"doi:10.1234/test": {"omid:br/0601"}}
        self.rconn_br.flush_index(data)
        members = self.rconn_br.rconn.smembers("doi:10.1234/test")
        assert members == {"omid:br/0601"}

    def test_flush_index_multiple_values(self):
        data = {"doi:10.1234/test": {"omid:br/0601", "omid:br/0602"}}
        self.rconn_br.flush_index(data)
        members = self.rconn_br.rconn.smembers("doi:10.1234/test")
        assert members == {"omid:br/0601", "omid:br/0602"}

    def test_flush_index_multiple_keys(self):
        data = {
            "doi:10.1234/test1": {"omid:br/0601"},
            "doi:10.1234/test2": {"omid:br/0602"},
        }
        self.rconn_br.flush_index(data)
        assert self.rconn_br.rconn.smembers("doi:10.1234/test1") == {"omid:br/0601"}
        assert self.rconn_br.rconn.smembers("doi:10.1234/test2") == {"omid:br/0602"}

    def test_flush_index_empty_set(self):
        data = {
            "doi:10.1234/test1": {"omid:br/0601"},
            "doi:10.1234/empty": set(),
            "doi:10.1234/test2": {"omid:br/0602"},
        }
        self.rconn_br.flush_index(data)
        assert self.rconn_br.rconn.smembers("doi:10.1234/test1") == {"omid:br/0601"}
        assert self.rconn_br.rconn.smembers("doi:10.1234/test2") == {"omid:br/0602"}
        assert self.rconn_br.rconn.exists("doi:10.1234/empty") == 0

    def test_flush_metadata(self):
        metadata = {
            "omid:br/0601": '{"date": "2023-01-15", "valid": true, "orcid": [], "issn": []}'
        }
        self.rconn_metadata.flush_metadata(metadata)
        result = self.rconn_metadata.rconn.get("omid:br/0601")
        assert result == '{"date": "2023-01-15", "valid": true, "orcid": [], "issn": []}'

    def test_key_count(self):
        data = {
            "key1": {"value1"},
            "key2": {"value2"},
            "key3": {"value3"},
        }
        self.rconn_br.flush_index(data)
        assert self.rconn_br.key_count() == 3

    def test_export_to_csv(self):
        data = {
            "doi:10.1234/test": {"omid:br/0601", "omid:br/0602"},
        }
        self.rconn_br.flush_index(data)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            filepath = f.name
        self.rconn_br.export_to_csv(filepath)
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            rows = list(reader)
        os.unlink(filepath)
        assert len(rows) == 1
        assert rows[0][0] == "doi:10.1234/test"
        exported_values = set(rows[0][1].split("; "))
        assert exported_values == {"omid:br/0601", "omid:br/0602"}


class TestProcessCsvFile(unittest.TestCase):
    def setUp(self):
        self.fake_server = fakeredis.FakeServer()
        self.rconn_br = FakeRedisDB(self.fake_server, 0)
        self.rconn_ra = FakeRedisDB(self.fake_server, 1)
        self.rconn_metadata = FakeRedisDB(self.fake_server, 2)

    def tearDown(self):
        self.rconn_br.rconn.flushdb()
        self.rconn_ra.rconn.flushdb()
        self.rconn_metadata.rconn.flushdb()

    def test_process_csv_br_index(self):
        csv_content = (
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n"
            "omid:br/0601 doi:10.1234/test,Title,[omid:ra/0601],2023-01-15,[omid:br/0610 issn:1234-5678],,,,,,"
        )
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)
        assert self.rconn_br.rconn.smembers("doi:10.1234/test") == {"omid:br/0601"}

    def test_process_csv_ra_index(self):
        csv_content = (
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n"
            "omid:br/0601,Title,[omid:ra/0601 orcid:0000-0001-1234-5678],2023-01-15,[],,,,,,"
        )
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)
        assert self.rconn_ra.rconn.smembers("orcid:0000-0001-1234-5678") == {"omid:ra/0601"}

    def test_process_csv_metadata(self):
        csv_content = (
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n"
            "omid:br/0601,Title,[omid:ra/0601 orcid:0000-0001-1234-5678],2023-01-15,[omid:br/0610 issn:1234-5678],,,,,,"
        )
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)
        result = json.loads(cast(str, self.rconn_metadata.rconn.get("omid:br/0601")))
        assert result == {
            "date": "2023-01-15",
            "valid": True,
            "orcid": ["0000-0001-1234-5678"],
            "issn": ["1234-5678"],
        }

    def test_process_csv_multiple_authors(self):
        csv_content = (
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n"
            "omid:br/0601,Title,[omid:ra/0601 orcid:0000-0001-1111-1111]; [omid:ra/0602 orcid:0000-0002-2222-2222],2023-01-15,[],,,,,,"
        )
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)
        assert self.rconn_ra.rconn.smembers("orcid:0000-0001-1111-1111") == {"omid:ra/0601"}
        assert self.rconn_ra.rconn.smembers("orcid:0000-0002-2222-2222") == {"omid:ra/0602"}
        result = json.loads(cast(str, self.rconn_metadata.rconn.get("omid:br/0601")))
        assert set(result["orcid"]) == {"0000-0001-1111-1111", "0000-0002-2222-2222"}

    def test_process_csv_multiple_issns(self):
        csv_content = (
            "id,title,author,pub_date,venue,volume,issue,page,type,publisher,editor\n"
            "omid:br/0601,Title,[omid:ra/0601],2023-01-15,[omid:br/0610 issn:1234-5678 issn:8765-4321],,,,,,"
        )
        csv_file = io.BytesIO(csv_content.encode("utf-8"))
        _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)
        result = json.loads(cast(str, self.rconn_metadata.rconn.get("omid:br/0601")))
        assert set(result["issn"]) == {"1234-5678", "8765-4321"}


class TestGetCsvFiles(unittest.TestCase):
    def setUp(self):
        self.test_dir = os.path.join("index", "python", "test", "data", "meta2redis")

    def test_csv_files_from_directory(self):
        result = _get_csv_files(self.test_dir)
        csv_names = [name for _, _, name in result]
        assert "sample_meta.csv" in csv_names
        assert "sample_meta_2.csv" in csv_names

    def test_csv_files_from_single_file(self):
        csv_path = os.path.join(self.test_dir, "sample_meta.csv")
        result = _get_csv_files(csv_path)
        assert len(result) == 1
        assert result[0] == ("file", csv_path, "sample_meta.csv")

    def test_csv_files_from_zip(self):
        zip_path = os.path.join(self.test_dir, "sample_meta.zip")
        result = _get_csv_files(zip_path)
        assert len(result) == 1
        assert result[0][0] == "zip"
        assert result[0][2] == "sample_meta.csv"

    def test_csv_files_from_tar_gz(self):
        tar_path = os.path.join(self.test_dir, "sample_meta.tar.gz")
        result = _get_csv_files(tar_path)
        assert len(result) == 1
        assert result[0][0] == "tar"
        assert result[0][2] == "sample_meta.csv"

    def test_nonexistent_path(self):
        result = _get_csv_files("/nonexistent/path")
        assert result == []


class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.fake_server = fakeredis.FakeServer()
        self.rconn_br = FakeRedisDB(self.fake_server, 0)
        self.rconn_ra = FakeRedisDB(self.fake_server, 1)
        self.rconn_metadata = FakeRedisDB(self.fake_server, 2)
        self.test_dir = os.path.join("index", "python", "test", "data", "meta2redis")

    def tearDown(self):
        self.rconn_br.rconn.flushdb()
        self.rconn_ra.rconn.flushdb()
        self.rconn_metadata.rconn.flushdb()

    def test_full_csv_processing(self):
        csv_path = os.path.join(self.test_dir, "sample_meta.csv")
        with open(csv_path, "rb") as f:
            _p_csvfile(f, self.rconn_br, self.rconn_ra, self.rconn_metadata)

        assert self.rconn_br.rconn.smembers("doi:10.1234/test1") == {"omid:br/0601"}
        assert self.rconn_br.rconn.smembers("doi:10.1234/test2") == {"omid:br/0602"}
        assert self.rconn_br.rconn.smembers("openalex:W123") == {"omid:br/0601"}
        assert self.rconn_br.rconn.smembers("pmid:12345678") == {"omid:br/0602"}
        assert self.rconn_br.rconn.smembers("isbn:978-3-16-148410-0") == {"omid:br/0603"}

        assert self.rconn_ra.rconn.smembers("orcid:0000-0001-1234-5678") == {"omid:ra/0601"}
        assert self.rconn_ra.rconn.smembers("orcid:0000-0002-1234-5678") == {"omid:ra/0602"}

        metadata_1 = json.loads(cast(str, self.rconn_metadata.rconn.get("omid:br/0601")))
        assert metadata_1["date"] == "2023-01-15"
        assert metadata_1["valid"] is True
        assert set(metadata_1["orcid"]) == {"0000-0001-1234-5678", "0000-0002-1234-5678"}
        assert metadata_1["issn"] == ["1234-5678"]

        metadata_2 = json.loads(cast(str, self.rconn_metadata.rconn.get("omid:br/0602")))
        assert metadata_2["date"] == "2023-02-20"
        assert set(metadata_2["issn"]) == {"8765-4321", "1111-2222"}

    def test_zip_processing(self):
        zip_path = os.path.join(self.test_dir, "sample_meta.zip")
        with ZipFile(zip_path) as archive:
            with archive.open("sample_meta.csv") as csv_file:
                _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)

        assert self.rconn_br.rconn.smembers("doi:10.1234/test1") == {"omid:br/0601"}

    def test_tar_gz_processing(self):
        tar_path = os.path.join(self.test_dir, "sample_meta.tar.gz")
        with tarfile.open(tar_path, "r:gz") as archive:
            csv_file = archive.extractfile("sample_meta.csv")
            _p_csvfile(csv_file, self.rconn_br, self.rconn_ra, self.rconn_metadata)

        assert self.rconn_br.rconn.smembers("doi:10.1234/test1") == {"omid:br/0601"}


if __name__ == "__main__":
    unittest.main()

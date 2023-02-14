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

from json import load
from oc.index.identifier.doi import DOIManager
from oc.index.parsing.base import CitationParser
from os.path import exists, basename
import gzip
import pandas as pd
from os import sep, makedirs, walk
from zipfile import ZipFile
from tarfile import TarFile
import json


class OpenaireParser(CitationParser):
    def __init__(self, id_to_metaid_file: str = None):
        super().__init__()
        self._rows = []
        self._doi_manager = DOIManager()
        self._accepted_schemas = {"pmid", "pmc", "doi"}
        if id_to_metaid_file and exists(id_to_metaid_file):
            self._id_to_metaid = self.dict_from_zip(id_to_metaid_file)
        else:
            self._id_to_metaid = dict()

    def is_valid(self, filename: str):
        super().is_valid(filename)
        return filename.endswith(".gz")

    def parse(self, filename: str):
        super().parse(filename)
        f = gzip.open(filename, 'rb')
        self._rows = f.readlines()  # list
        self._items = len(self._rows)

    def get_next_citation_data(self):
        if len(self._rows) == 0:
            return None

        item_bt = self._rows.pop()
        row = json.loads(item_bt.decode('utf-8'))
        self._current_item += 1
        if row:
            relation = row.get("relationship")
            if relation:
                if row.get("source") and row.get("target") and (relation.get("name") == "Cites" or relation.get("name") == "IsCitedBy"):
                    citations = []

                    # split process according to citation type: addressed / received

                    # ADDRESSED CITATION
                    if relation.get("name") == "Cites":
                        # citing
                        if row.get("source"):
                            source = row["source"]
                            if source.get("identifier"):
                                # TODO: SUBST string manipulation with id normalization
                                citing_ids = [x.get("schema")+":"+x.get("identifier") for x in source.get("identifier") if
                                             x.get("schema") in self._accepted_schemas]
                                metaid_citing = None
                                for id_citing in citing_ids:
                                    metaid_citing = self._id_to_metaid.get(id_citing)
                                    if metaid_citing:
                                        break
                                if metaid_citing:
                                    citing = metaid_citing

                                    # cited
                                    if row.get("target"):
                                        target = row["target"]
                                        if target.get("identifier"):
                                            # TODO: SUBST string manipulation with id normalization
                                            cited_ids = [x.get("schema")+":"+x.get("identifier") for x in target.get("identifier") if
                                                          x.get("schema") in self._accepted_schemas]
                                            metaid_cited = None
                                            for id_cited in cited_ids:
                                                metaid_cited = self._id_to_metaid.get(id_cited)
                                                if metaid_cited:
                                                    break
                                            if metaid_cited:
                                                cited = metaid_cited
                                                citations.append((citing, cited, None, None, None, None))
                    # RECEIVED CITATION
                    elif relation.get("name") == "IsCitedBy":
                        # citing
                        if row.get("target"):
                            target = row["target"]
                            if target.get("identifier"):
                                # TODO: SUBST string manipulation with id normalization
                                citing_ids = [x.get("schema")+":"+x.get("identifier") for x in target.get("identifier") if x.get("schema") in self._accepted_schemas]
                                metaid_citing = None
                                for id_citing in citing_ids:
                                    metaid_citing = self._id_to_metaid.get(id_citing)
                                    if metaid_citing:
                                        break
                                if metaid_citing:
                                    citing = metaid_citing

                                    # cited
                                    if row.get("source"):
                                        source = row["source"]
                                        if source.get("identifier"):
                                            # TODO: SUBST string manipulation with id normalization
                                            cited_ids = [x.get("schema")+":"+x.get("identifier") for x in source.get("identifier") if
                                                          x.get("schema") in self._accepted_schemas]
                                            metaid_cited = None
                                            for id_cited in cited_ids:
                                                metaid_cited = self._id_to_metaid.get(id_cited)
                                                if metaid_cited:
                                                    break
                                            if metaid_cited:
                                                cited = metaid_cited
                                                citations.append((citing, cited, None, None, None, None))

                    return citations

        return self.get_next_citation_data()

    def dict_from_zip(self, zip_dir):
        doi_orcid_index = dict()
        if exists(zip_dir):
            orcid_id_files = self.get_all_files(zip_dir)[0]
            len_orcid_id_files = len(orcid_id_files)
            if len_orcid_id_files > 0:
                for f_idx, f in enumerate(orcid_id_files, 1):
                    df = pd.read_csv(f, encoding='utf8')
                    df.fillna("", inplace=True)
                    df_dict_list = df.to_dict("records")
                    for row in df_dict_list:
                        if row.get("id") and row.get("id") != "None":
                            if row["id"] not in doi_orcid_index.keys():
                                doi_orcid_index[row["id"]] = row["meta"].strip()
                            else:
                                if row["meta"].strip() != doi_orcid_index[row["id"]]:
                                    raise Exception("Multiple metaid for id:", row["id"], ":", doi_orcid_index[row["id"]], "and", row["meta"].strip())
        return doi_orcid_index

    @staticmethod
    def get_all_files(i_dir):
        result = []
        opener = None
        if i_dir.endswith(".zip"):
            with ZipFile(i_dir, 'r') as zip_ref:
                dest_dir = i_dir.split(".")[0] + "_decompr_zip_dir"
                if not exists(dest_dir):
                    makedirs(dest_dir)
                zip_ref.extractall(dest_dir)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(".csv") and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)
        elif i_dir.endswith(".tar.gz"):
            tf = TarFile.open(i_dir)
            for name in tf.getnames():
                if name.lower().endswith(".csv"):
                    result.append(name)
            opener = tf.extractfile

        else:
            for cur_dir, cur_subdir, cur_files in walk(i_dir):
                for file in cur_files:
                    if file.lower().endswith(".csv"):
                        result.append(cur_dir + sep + file)
            opener = open
        return result, opener


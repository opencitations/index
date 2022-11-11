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

from argparse import ArgumentParser
import os
from os import sep, makedirs, walk
from os.path import exists, basename, isdir
from json import load, loads
import datetime
from timeit import default_timer as timer
from tqdm import tqdm
import json
import tarfile

# from oc.index.legacy.csv import CSVManager
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.glob.csv import CSVDataSource
from oc.index.finder.datacite import DataCiteResourceFinder


def get_all_files(i_dir_or_compr, req_type):
    result = []
    targz_fd = None

    if isdir(i_dir_or_compr):

        for cur_dir, cur_subdir, cur_files in walk(i_dir_or_compr):
            for cur_file in cur_files:
                if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                    result.append(os.path.join(cur_dir, cur_file))
    elif i_dir_or_compr.endswith("tar.gz"):
        targz_fd = tarfile.open(i_dir_or_compr, "r:gz", encoding="utf-8")
        for cur_file in targz_fd:
            if cur_file.name.endswith(req_type) and not basename(cur_file.name).startswith("."):
                result.append(cur_file)
    elif i_dir_or_compr.endswith("zip"):
        with zipfile.ZipFile(i_dir_or_compr, 'r') as zip_ref:
            dest_dir = i_dir_or_compr + "decompr_zip_dir"
            if not exists(dest_dir):
                makedirs(dest_dir)
            zip_ref.extractall(dest_dir)
        for cur_dir, cur_subdir, cur_files in walk(dest_dir):
            for cur_file in cur_files:
                if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                    result.append(cur_dir + sep + cur_file)

    elif i_dir_or_compr.endswith("zst"):
        input_file = pathlib.Path(i_dir_or_compr)
        dest_dir = i_dir_or_compr.split(".")[0] + "decompr_zst_dir"
        with open(input_file, 'rb') as compressed:
            decomp = zstd.ZstdDecompressor()
            if not exists(dest_dir):
                makedirs(dest_dir)
            output_path = pathlib.Path(dest_dir) / input_file.stem
            if not exists(output_path):
                with open(output_path, 'wb') as destination:
                    decomp.copy_stream(compressed, destination)
        for cur_dir, cur_subdir, cur_files in walk(dest_dir):
            for cur_file in cur_files:
                if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                    result.append(cur_dir + sep + cur_file)
    else:
        print("It is not possible to process the input path:", i_dir_or_compr)
    return result, targz_fd


def load_json_doci(file, targz_fd, file_idx, len_all_files):
    result = None

    if targz_fd is None:
        # print("Open file %s of %s" % (file_idx, len_all_files))
        with open(file, encoding="utf8") as f:
            result = load(f)
    else:
        print("Open file %s of %s (in tar.gz archive)" % (file_idx, len_all_files))
        cur_tar_file = targz_fd.extractfile(file)
        json_str = cur_tar_file.read()
        # In Python 3.5 it seems that, for some reason, the extractfile method returns an
        # object 'bytes' that cannot be managed by the function 'load' in the json package.
        # Thus, to avoid issues, in case an object having type 'bytes' is return, it is
        # transformed as a string before passing it to the function 'loads'. Please note
        # that Python 3.9 does not show this behaviour, and it works correctly without
        # any transformation.
        if type(json_str) is bytes:
            json_str = json_str.decode("utf-8")

        result = loads(json_str)

    return result


def process_doci(input_dir, output_dir, n):
    start = timer()
    if not exists(output_dir):
        makedirs(output_dir)

    doi_manager = DOIManager()
    csv_datasource = CSVDataSource("DOCI")
    dcrf = DataCiteResourceFinder()

    all_files, targz_fd = get_all_files(input_dir, ".json")
    len_all_files = len(all_files)
    relevant_relations = ["references", "isreferencedby", "cites", "iscitedby"]

    count = 0
    # Read all the JSON files in the DataCite dump to create the main information of all the indexes
    print("\n\n# Add valid DOIs from DataCite metadata")
    for file_idx, file in enumerate(tqdm(all_files), 1):
        data = load_json_doci(file, targz_fd, file_idx, len_all_files)
        data_list = data["data"]
        for item in data_list:
            count += 1
            attributes = item["attributes"]
            citing_doi = attributes["doi"]
            citing_doi = doi_manager.normalise(citing_doi, True)
            # valid_doi.add_value(citing_doi, "v" if doi_manager.is_valid(citing_doi) else "i")
            if citing_doi is not None:
                entity = csv_datasource.get(citing_doi)
                if entity is None:
                    entity = dict()
                    entity["valid"] = True

                # collect the date of issue if it is possible,  the year of publcation otherwise
                citing_date = []

                dates = attributes.get("dates")
                date_not_found = True
                if dates:
                    for date in dates:
                        if date.get("dateType") == "Issued":
                            cur_date = dcrf.Date_Validator(date.get("date"))
                            if cur_date:
                                citing_date.append(cur_date)
                                date_not_found = False
                                break
                if date_not_found:
                    cur_date = json_obj.get("publicationYear")
                    if cur_date:
                        cur_date = dcrf.Date_Validator(str(cur_date))
                        if cur_date:
                            citing_date.append(cur_date)

                if len(citing_date) > 0:
                    entity["date"] = citing_date

                # collect the orcid of the contributors
                orcid_list = list(dcrf._get_orcid(attributes))
                if len(orcid_list) > 0:
                    entity["orcid"] = orcid_list

                # collect the issn of the resource or its container
                valid_issn_list = list(dcrf._get_issn(attributes))
                if len(valid_issn_list) > 0:
                    entity["issn"] = valid_issn_list

                csv_datasource.set(citing_doi, entity)

    middle = timer()
    print("citing entities process duration: :", (middle - start))

    cited_dois = 0
    count = 0
    needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
    relevant_relations = ["references", "cites", "isreferencedby", "iscitedby"]
    for file_idx, file in enumerate(tqdm(all_files), 1):
        data = load_json_doci(file, targz_fd, file_idx, len_all_files)
        data_list = data["data"]
        for item in data_list:
            count += 1
            # print("processing entity n.", count, "for cited dois")
            attributes = item["attributes"]
            for ref in attributes["relatedIdentifiers"]:
                if all(elem in ref for elem in needed_info):
                    relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower().strip()
                    if relatedIdentifierType == "doi":
                        rel_id = doi_manager.normalise(ref["relatedIdentifier"])
                        relationType = str(ref["relationType"]).lower().strip()
                        if relationType in relevant_relations:
                            relatedDOI = doi_manager.normalise(rel_id, True)
                            if relatedDOI:
                                relatedDOI_entity = csv_datasource.get(relatedDOI)
                                if not relatedDOI_entity:
                                    relatedDOI_entity = dict()
                                    relatedDOI_entity["valid"] = (True if doi_manager.is_valid(relatedDOI) else False)
                                    cited_dois += 1
                                    csv_datasource.set(relatedDOI, relatedDOI_entity)

    end = timer()
    print("cited entities process duration: ", end-middle)
    print("full process duration: ", end-start)


def main():
    arg_parser = ArgumentParser(
        "Global files creator for DOCI",
        description="Process DataCite JSON files and create global indexes to enable "
                    "the creation of DOCI.",
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        dest="input",
        required=True,
        help="Either the directory or the zip file that contains the DataCite data dump "
             "of JSON files.",
    )
    arg_parser.add_argument(
        "-o",
        "--output",
        dest="output",
        required=True,
        help="The directory where the indexes are stored.",
    )

    arg_parser.add_argument(
        "-n",
        "--num_entities",
        dest="num_entities",
        required=True,
        help="Interval of processed entities after which the issn data are saved to cache files.",
    )

    args = arg_parser.parse_args()
    process_doci(args.input, args.output, args.num_entities)

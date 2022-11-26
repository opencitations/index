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
import csv
from argparse import ArgumentParser
from os import sep, makedirs, walk
import os
import os.path
import json
from zipfile import ZipFile
from tarfile import TarFile
import re
from os.path import exists, basename, isdir, join
from timeit import default_timer as timer
from re import sub
import pandas as pd
import codecs
import requests
from tqdm import tqdm

from oc.index.oci.citation import Citation
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.pmid import PMIDManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.finder.orcid import ORCIDResourceFinder, ORCIDResourceFinderPMID
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.glob.csv import CSVDataSource


def dict_from_zip(zip_dir):
    doi_orcid_index = dict()
    if exists(zip_dir):
        orcid_id_files = get_all_files_noci(zip_dir)[0]
        len_orcid_id_files = len(orcid_id_files)
        if len_orcid_id_files > 0:
            for f_idx, f in enumerate(orcid_id_files, 1):
                df = pd.read_csv(f, encoding='utf8')
                df.fillna("", inplace=True)
                df_dict_list = df.to_dict("records")
                for row in df_dict_list:
                    if row.get("id") != "None":
                        if row["id"] not in doi_orcid_index.keys():
                            doi_orcid_index[row["id"]] = [row["value"]]
                        else:
                            if row["value"] not in doi_orcid_index[row["id"]]:
                                doi_orcid_index[row["id"]].append(row["value"])
    return doi_orcid_index


def check_author_identity_api(
    uri, authors_dicts_list, orcid_client_id, orcid_client_secret
):
    data = {
        "client_id": orcid_client_id,
        "client_secret": orcid_client_secret,
        "grant_type": "client_credentials",
        "scope": "/read-public",
    }
    response = requests.post(uri, headers={"Accept": "application/json"}, data=data)
    identity_verified = False
    if response.status_code == 200:
        json_response = response.json()
        if json_response:
            person = json_response["person"]
            if person:
                if "name" in person.keys():
                    given_names = [
                        v
                        for k, v in person["name"].items()
                        if k == "given-names" and v
                    ]
                    if given_names:
                        if "value" in person["name"]["given-names"]:
                            g_names = person["name"]["given-names"]["value"]
                            family_name = [
                                v
                                for k, v in person["name"].items()
                                if k == "family-name" and v
                            ]
                            if family_name:
                                if "value" in person["name"]["family-name"]:
                                    f_name = person["name"]["family-name"]["value"]
                                    orcid_author_surnames_list = re.findall(
                                        "[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}",
                                        f_name,
                                    )
                                    orcid_author_surnames_list_l = [
                                        x.lower() for x in orcid_author_surnames_list
                                    ]
                                    orcid_author_names_list = re.findall(
                                        "[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}",
                                        g_names,
                                    )
                                    orcid_author_names_list_l = [
                                        x.lower() for x in orcid_author_names_list
                                    ]
                                    matches = [
                                        dict
                                        for dict in authors_dicts_list
                                        if [
                                            sn
                                            for sn in dict["surnames"]
                                            if sn in orcid_author_surnames_list_l
                                        ]
                                        and [
                                            l
                                            for l in dict["names"]
                                            if any(
                                                element.startswith(l)
                                                and element not in dict["surnames"]
                                                for element in orcid_author_names_list_l
                                            )
                                        ]
                                    ]
                                    if matches:
                                        identity_verified = True
    return identity_verified


def issn_data_recover_noci(directory):
    journal_issn_dict = dict()
    filename = join(directory, "journal_issn.json")
    if not os.path.exists(filename):
        return journal_issn_dict
    else:
        with open(filename, "r", encoding="utf8") as fd:
            journal_issn_dict = json.load(fd)
            return journal_issn_dict


def issn_data_to_cache_noci(name_issn_dict, directory):
    filename = join(directory, "journal_issn.json")
    with open(filename, "w", encoding="utf-8") as fd:
        json.dump(name_issn_dict, fd, ensure_ascii=False, indent=4)


# takes in input a data structure representing a bibliographic entity
def build_pubdate_noci(row):
    year = str(row["year"])
    str_year = sub("[^\d]", "", year)[:4]
    if str_year:
        return str_year
    else:
        return None


# get_all_files extracts all the needed files from the input directory
def get_all_files_noci(i_dir):
    result = []
    opener = None
    if i_dir.endswith(".zip"):
        with ZipFile(i_dir, 'r') as zip_ref:
            dest_dir = i_dir + "decompr_zip_dir"
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


def process_noci(
    input_dir,
    output_dir,
    n,
    id_orcid_dir=None,
    orcid_client_id=None,
    orcid_client_secret=None,
):

    start = timer()
    if not exists(output_dir):
        makedirs(output_dir)

    journal_issn_dict = issn_data_recover_noci(output_dir)
    crossref_resource_finder = CrossrefResourceFinder()
    orcid_resource_finder = ORCIDResourceFinder()
    pmid_orcid_resource_finder = ORCIDResourceFinderPMID()

    doi_manager = DOIManager()
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()
    pmid_manager = PMIDManager()
    csv_datasource = CSVDataSource("NOCI")

    all_files, opener = get_all_files_noci(input_dir)
    len_all_files = len(all_files)
    pmid_doi_map = dict()

    doi_orcid_index = dict()
    if id_orcid_dir and exists(id_orcid_dir):
        doi_orcid_index = dict_from_zip(id_orcid_dir)

    # Read all the CSV file in the NIH dump to create the main information of all the indexes
    print("\n\n# Add valid PMIDs from NIH metadata")
    for file_idx, file in enumerate(tqdm(all_files), 1):
        df = pd.read_csv(file, encoding='utf8', low_memory=True)
        df.fillna("", inplace=True)
        df_dict_list = df.to_dict("records")
        for index, row in enumerate(df_dict_list):
            if int(index) != 0 and int(index) % int(n) == 0:
                # print( "Group nr.", int(index)//int(n), "processed. Data from", int(index), "rows saved to journal_issn.json mapping file")
                issn_data_to_cache_noci(journal_issn_dict, output_dir)
            citing_pmid = pmid_manager.normalise(row["pmid"], True)
            if citing_pmid :
                authors_dicts_list = []
                authors_split_list = row["authors"].split(",")
                for author in authors_split_list:
                    names = re.findall(
                        "([A-Z]|[ÄŐŰÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽÑ]){1}\s",
                        author,
                    )
                    strp_names = [(x.strip()).lower() for x in names]
                    surnames = re.findall(
                        "[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}",
                        author,
                    )
                    surnames_l = [s.lower() for s in surnames]
                    author_dict = {"names": strp_names, "surnames": surnames_l}
                    authors_dicts_list.append(author_dict)

                if not csv_datasource.get(citing_pmid):
                    entity = dict()

                    entity["valid"] = True

                    citing_doi = doi_manager.normalise(row["doi"], False)
                    if citing_doi:
                        pmid_doi_map[citing_pmid] = {
                            "doi": citing_doi,
                            "has_orcid": False,
                            "all_authors_names": authors_dicts_list,
                        }

                    citing_date = Citation.check_date(build_pubdate_noci(row))
                    if citing_date:
                        entity["date"] = [citing_date]

                    if orcid_client_id and orcid_client_secret:
                        json_res_pmid = pmid_orcid_resource_finder._call_api(
                            citing_pmid
                        )
                        if json_res_pmid and len(json_res_pmid) > 0:
                            pmid_certified_orcid = []
                            for orcid_dict_pmid in json_res_pmid:
                                pmid_json_uri = orcid_dict_pmid["orcid-identifier"][
                                    "uri"
                                ]
                                pmid_chek_passed = check_author_identity_api(
                                    pmid_json_uri,
                                    authors_dicts_list,
                                    orcid_client_id,
                                    orcid_client_secret,
                                )
                                pmid_norm_orc = orcid_manager.normalise(
                                    pmid_json_uri
                                )
                                if pmid_chek_passed and pmid_norm_orc:
                                    pmid_certified_orcid.append(pmid_norm_orc)

                            if len(pmid_certified_orcid) > 0:
                                entity["orcid"] = pmid_certified_orcid
                                if citing_pmid in pmid_doi_map.keys():
                                    pmid_doi_map[citing_pmid]["has_orcid"] = True

                    issn_list = []
                    journal_name = row["journal"]
                    if journal_name:
                        if journal_name in journal_issn_dict.keys():
                            for issn in journal_issn_dict[journal_name]:
                                issn_list.append(issn)

                        else:
                            if citing_doi:
                                json_res = crossref_resource_finder._call_api(
                                    citing_doi
                                )
                                if json_res:
                                    issn_set = crossref_resource_finder._get_issn(
                                        json_res
                                    )
                                    if len(issn_set) > 0:
                                        journal_issn_dict[journal_name] = []
                                    for issn in issn_set:
                                        issn_norm = issn_manager.normalise(
                                            str(issn)
                                        )
                                        issn_list.append(issn_norm)
                                        journal_issn_dict[journal_name].append(
                                            issn_norm
                                        )
                    else:
                        if citing_doi:
                            json_res = crossref_resource_finder._call_api(
                                citing_doi
                            )
                            if json_res:
                                issn_set = crossref_resource_finder._get_issn(
                                    json_res
                                )
                                for issn in issn_set:
                                    issn_norm = issn_manager.normalise(str(issn))
                                    issn_list.append(issn_norm)
                    if issn_list:
                        entity["issn"] = issn_list

                    csv_datasource.set(citing_pmid, entity)

        if doi_orcid_index:
            pmid_doi_map = {
                k: v for k, v in pmid_doi_map.items() if v["has_orcid"] is False
            }
            if pmid_doi_map:
                for k,v in pmid_doi_map.items():
                    if v["doi"] in doi_orcid_index.keys():
                        values_list = doi_orcid_index[v["doi"]]
                        for author in values_list:
                            orcid = re.search("([0-9]{4}-){3}[0-9]{3}[0-9X]", author, re.IGNORECASE).group(0)
                            author_name_parts = re.findall(
                                    "[a-zA-Z'\-áéíóúäëïöüÄłŁőŐűŰZàáâäãåąčćęèéêëėįìíîïłńòóôöõøùúûüųūÿýżźñçčšžÀÁÂÄÃÅĄĆČĖĘÈÉÊËÌÍÎÏĮŁŃÒÓÔÖÕØÙÚÛÜŲŪŸÝŻŹÑßÇŒÆČŠŽñÑâê]{2,}",
                                    author,
                                )
                            author_name_parts_l = [np.lower() for np in author_name_parts]
                            authors_dicts_list = pmid_doi_map[k]["all_authors_names"]
                            matches = [dict for dict in authors_dicts_list if [
                                        sn for sn in dict["surnames"] if sn in author_name_parts_l
                                    ]
                                    and [l for l in dict["names"] if any(
                                            element.startswith(l) and element not in dict["surnames"] for element in author_name_parts_l
                                        )
                                    ]
                                ]
                            if matches and orcid:
                                nor_orcid = orcid_manager.normalise(orcid)
                                if nor_orcid:
                                    c_pmid_entity = csv_datasource.get(k)
                                    if not c_pmid_entity["orcid"]:
                                        c_pmid_entity["orcid"] = []
                                        c_pmid_entity["orcid"].append(nor_orcid)
                                    else:
                                        c_pmid_entity["orcid"] = list(
                                            c_pmid_entity["orcid"]
                                        )
                                        c_pmid_entity["orcid"].append(nor_orcid)

                                    csv_datasource.set(k, c_pmid_entity)
                                    if (
                                        pmid_doi_map[k]["has_orcid"]
                                        == False
                                    ):
                                        pmid_doi_map[k]["has_orcid"] = True

            if orcid_client_id and orcid_client_secret:
                for citing_pmid, d in pmid_doi_map.items():
                    if d["has_orcid"] == False:
                        json_res = orcid_resource_finder._call_api(d["doi"])
                        if json_res and len(json_res) > 0:
                            # To do: check if absence of result with orcid resource finder
                            # implies absence of result also with access token
                            certified_orcid = []
                            for orcid_dict in json_res:
                                json_uri = orcid_dict["orcid-identifier"]["uri"]
                                chek_passed = check_author_identity_api(
                                    json_uri,
                                    d["all_authors_names"],
                                    orcid_client_id,
                                    orcid_client_secret,
                                )
                                norm_orc = orcid_manager.normalise(json_uri)
                                if chek_passed and norm_orc:
                                    certified_orcid.append(norm_orc)

                            citing_pmid_dict = csv_datasource.get(citing_pmid)

                            if len(certified_orcid) > 0:
                                d["has_orcid"] = True
                                if not citing_pmid_dict["orcid"]:
                                    citing_pmid_dict["orcid"] = certified_orcid
                                else:
                                    citing_pmid_dict["orcid"].extend(
                                        certified_orcid
                                    )
                                csv_datasource.set(citing_pmid, citing_pmid_dict)

        pmid_doi_map = dict()
        issn_data_to_cache_noci(journal_issn_dict, output_dir)

    middle = timer()

    print("first process duration: :", (middle - start))
    print("\n\n# Checking the referenced pmids validity")
    for file_idx, file in enumerate(tqdm(all_files), 1):
        df = pd.read_csv(file, encoding='utf8', low_memory=True)
        df.fillna("", inplace=True)
        df_dict_list = df.to_dict("records")
        for index, row in enumerate(df_dict_list):
            if row.get("references"):
                ref_string = row["references"].strip()
                ref_string_norm = re.sub("\s+", " ", ref_string)
                cited_pmids = set(ref_string_norm.split(" "))
                for cited_pmid in cited_pmids:
                    cited_pmid = pmid_manager.normalise(cited_pmid, True)

                    if cited_pmid:
                        cited_pmid_entity = csv_datasource.get(cited_pmid)
                        if not cited_pmid_entity:
                            cited_pmid_entity = dict()
                            cited_pmid_entity["valid"] = (
                                True if pmid_manager.is_valid(cited_pmid) else False
                            )
                            csv_datasource.set(cited_pmid, cited_pmid_entity)

            if row.get("cited_by"):
                citing_string = row["cited_by"].strip()
                citing_string_norm = re.sub("\s+", " ", citing_string)
                citing_pmids = set(citing_string_norm.split(" "))
                for citing_p in citing_pmids:
                    citing_p = pmid_manager.normalise(citing_p, True)
                    if not citing_p:
                        citing_p_entity = csv_datasource.get(citing_p)
                        if not citing_p_entity:
                            citing_p_entity = dict()
                            citing_p_entity["valid"] = (
                                True if pmid_manager.is_valid(citing_p) else False
                            )
                            csv_datasource.set(citing_p, citing_p_entity)

    end = timer()
    print("second process duration: ", end-middle)
    print("full process duration: ", end-start)


def main():
    arg_parser = ArgumentParser(
        "Global files creator for NOCI",
        description="Process iCiteMetadata CSV files and create global indexes to enable the creation of NOCI.",
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        dest="input",
        required=True,
        help="Either the directory or the zip file that contains the iCiteMetadata data dump of CSV files.",
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
        "--entities",
        dest="entities",
        required=True,
        help="Interval of processed entities after which the issn data are saved to the cache file.",
    )

    arg_parser.add_argument(
        "-iod",
        "--orcid",
        dest="orcid",
        required=False,
        help="Either the directory or the zip file that contains the id-orcid mapping data.",
    )

    arg_parser.add_argument(
        "-oci",
        "--orcid_client_id",
        dest="orcid_client_id",
        required=False,
        help="ORCID credentials: orcid client ID, to double check that the identity of the authors declared in NIH "
        "dump corresponds to the ORCID registry data. The credentials are required to obtain the token, which is"
        "required in order to access ORCID public API. Client ID and Client secret can be required in the "
        "Developer tools section of the ORCID platform.",
    )

    arg_parser.add_argument(
        "-ocs",
        "--orcid_client_secret",
        dest="orcid_client_secret",
        required=False,
        help="ORCID credentials: orcid password, to double check that the identity of the authors declared in NIH "
        "dump corresponds to the ORCID registry data. The credentials are required to obtain the token, which is"
        "required in order to access ORCID public API. Client ID and Client secret can be required in the "
        "Developer tools section of the ORCID platform.",
    )

    args = arg_parser.parse_args()
    process_noci(
        args.input,
        args.output,
        args.entities,
        args.orcid,
        args.orcid_client_id,
        args.orcid_client_secret,
    )
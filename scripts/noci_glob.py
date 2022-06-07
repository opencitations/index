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
from os import sep, makedirs, walk
import os
import json
from zipfile import ZipFile
from tarfile import TarFile
import re
from timeit import default_timer as timer
from os.path import exists, basename, isdir
from json import load, loads
from collections import Counter
from datetime import date
from timeit import default_timer as timer
from re import sub
import tarfile
import pandas as pd



from oc.index.oci.citation import Citation
from oc.index.legacy.csv import CSVManager
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.pmid import PMIDManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.finder.orcid import ORCIDResourceFinder
from oc.index.finder.crossref import CrossrefResourceFinder


def issn_data_recover(directory):
    journal_issn_dict = dict()
    filename = directory + sep + 'journal_issn.json'
    if not os.path.exists(filename):
        return journal_issn_dict
    else:
        with open(filename, 'r', encoding='utf8') as fd:
            journal_issn_dict = json.load(fd)
            types = type(journal_issn_dict)
            return journal_issn_dict

def issn_data_to_cache(name_issn_dict, directory):
    filename = directory + sep + 'journal_issn.json'
    with open(filename, 'w', encoding='utf-8' ) as fd:
            json.dump(name_issn_dict, fd, ensure_ascii=False, indent=4)

#PUB DATE EXTRACTION : takes in input a data structure representing a bibliographic entity
def build_pubdate(row):
    year = str(row["year"])
    str_year = sub( "[^\d]", "", year)[:4]
    if str_year:
        return str_year
    else:
        return None


# get_all_files extracts all the needed files from the input directory
def get_all_files(i_dir):
    result = []
    opener = None
    if i_dir.endswith(".zip"):
        zf = ZipFile(i_dir)
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                result.append(name)
        opener = zf.open
    elif i_dir.endswith(".tar.gz"):
        tf = TarFile.open(i_dir)
        for name in tf.getnames():
            if name.lower().endswith(".csv") and "citations" not in name.lower() and "source" not in name.lower():
                result.append(name)
        opener = tf.extractfile

    else:
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in cur_files:
                if file.lower().endswith(".csv") and "citations" not in file.lower() and "source" not in file.lower():
                    result.append(cur_dir + sep + file)
        opener = open
    return result, opener

def process(input_dir, output_dir, n, id_orcid_dir):
    start = timer()
    if not exists(output_dir):
        makedirs(output_dir)

    citing_pmid_with_no_date = set()
    valid_pmid = CSVManager(output_dir + sep + "valid_pmid.csv")
    valid_doi = CSVManager(output_dir + sep + "valid_doi.csv")
    id_date = CSVManager(output_dir + sep + "id_date_pmid.csv")
    id_issn = CSVManager(output_dir + sep + "id_issn_pmid.csv")
    id_orcid = CSVManager(output_dir + sep + "id_orcid_pmid.csv")

    journal_issn_dict = issn_data_recover(output_dir)

    crossref_resource_finder = CrossrefResourceFinder()
    orcid_resource_finder = ORCIDResourceFinder()

    doi_manager = DOIManager(valid_doi)
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()
    pmid_manager = PMIDManager()


    all_files, opener = get_all_files(input_dir)
    len_all_files = len(all_files)

    # Read all the CSV file in the NIH dump to create the main information of all the indexes
    print("\n\n# Add valid PMIDs from NIH metadata")
    for file_idx, file in enumerate(all_files, 1):
        df = pd.DataFrame()

        for chunk in pd.read_csv(file, chunksize=1000 ):
            f = pd.concat( [df, chunk], ignore_index=True )
            f.fillna("", inplace=True)

            print("Open file %s of %s" % (file_idx, len_all_files))
            for index, row in f.iterrows():
                if int(index) !=0 and int(index) % int(n) == 0:
                    print( "Group nr.", int(index)//int(n), "processed. Data from", int(index), "rows saved to journal_issn.json mapping file")
                    issn_data_to_cache(journal_issn_dict, output_dir)

                citing_pmid = pmid_manager.normalise(row['pmid'], True)
                valid_pmid.add_value(citing_pmid,"v")
                citing_doi = doi_manager.normalise(row['doi'], True)

                if id_date.get_value(citing_pmid) is None:
                    citing_date = Citation.check_date(build_pubdate(row))
                    if citing_date is not None:
                        id_date.add_value(citing_pmid, citing_date)
                        if citing_pmid in citing_pmid_with_no_date:
                            citing_pmid_with_no_date.remove(citing_pmid)
                    else:
                        citing_pmid_with_no_date.add( citing_pmid )

                if id_issn.get_value( citing_pmid ) is None:
                    journal_name = row["journal"]
                    if journal_name: #check that the string is not empty
                        if journal_name in journal_issn_dict.keys():
                            for issn in journal_issn_dict[journal_name]:
                                id_issn.add_value(citing_pmid, issn)
                        else:
                            if citing_doi is not None:
                                json_res = crossref_resource_finder._call_api(citing_doi)
                                if json_res is not None:
                                    issn_set = crossref_resource_finder._get_issn(json_res)
                                    if len(issn_set)>0:
                                        journal_issn_dict[journal_name] = []
                                    for issn in issn_set:
                                        issn_norm = issn_manager.normalise(str(issn))
                                        id_issn.add_value( citing_pmid, issn_norm )
                                        journal_issn_dict[journal_name].append(issn_norm)


                if id_orcid.get_value(citing_pmid) is None:
                    if citing_doi is not None:
                        json_res = orcid_resource_finder._call_api(citing_doi)
                        if json_res is not None:
                            orcid_set = orcid_resource_finder._get_orcid(json_res)
                            for orcid in orcid_set:
                                orcid_norm = orcid_manager.normalise(orcid)
                                id_orcid.add_value(citing_pmid, orcid_norm)

            issn_data_to_cache(journal_issn_dict, output_dir)

    middle = timer()

    print("first process duration: :", (middle - start))
    # Iterate once again for all the rows of all the csv files, so to check the validity of the referenced pmids.
    print("\n\n# Checking the referenced pmids validity")
    for file_idx, file in enumerate(all_files, 1):
        df = pd.DataFrame()

        for chunk in pd.read_csv(file, chunksize=1000):
            f = pd.concat([df, chunk], ignore_index=True)
            f.fillna("", inplace=True)
            print("Open file %s of %s" % (file_idx, len_all_files))
            for index, row in f.iterrows():
                if row["references"] != "":
                    ref_string = row["references"].strip()
                    ref_string_norm = re.sub("\s+", " ", ref_string)
                    cited_pmids = set(ref_string_norm.split(" "))
                    for cited_pmid in cited_pmids:
                        cited_pmid = pmid_manager.normalise(cited_pmid, True)
                        if valid_pmid.get_value(cited_pmid) is None:
                            valid_pmid.add_value(cited_pmid, "v" if pmid_manager.is_valid(cited_pmid) else "i")
                            print("valid cited pmid added:", cited_pmid)
                        else:
                            print("invalid cited pmid discarded:", cited_pmid)
                else:
                    print("the type of row reference is", (row["references"]), type(row["references"]))
                    print(index, row)

    for pmid in citing_pmid_with_no_date:
        id_date.add_value(pmid, "")

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
        "--input_dir",
        dest="input_dir",
        required=True,
        help="Either the directory or the zip file that contains the iCiteMetadata data dump of CSV files.",
    )
    arg_parser.add_argument(
        "-o",
        "--output_dir",
        dest="output_dir",
        required=True,
        help="The directory where the indexes are stored.",
    )

    arg_parser.add_argument(
        "-n",
        "--num_entities",
        dest="num_entities",
        required=True,
        help="Interval of processed entities after which the issn data are saved to the cache file.",
    )

    arg_parser.add_argument(
        "-iod",
        "--id_orcid_dir",
        dest="id_orcid_dir",
        required=False,
        help="Either the directory or the zip file that contains the id-orcid mapping data.",
    )

    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir, args.num_entities, args.id_orcid_dir)

# For testing purposes
if __name__ == '__main__':
    main()

# GitHub\index>python "scripts/noci_glob.py" -i ./index/python/test/data/noci_glob_dump_input -o ./index/python/test/data/noci_glob_dump_output -n 25 -iod ./index/python/test/data/noci_id_orcid_mapping
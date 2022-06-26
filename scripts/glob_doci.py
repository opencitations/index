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

from oc.index.legacy.csv import CSVManager
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager


def issn_data_recover_doci(directory):
    journal_issn_dict = dict()
    filename = directory + sep + "journal_issn.json"
    if not os.path.exists(filename):
        return journal_issn_dict
    else:
        with open(filename, "r", encoding="utf8") as fd:
            journal_issn_dict = json.load(fd)
            types = type(journal_issn_dict)
            return journal_issn_dict


def issn_data_to_cache_doci(name_issn_dict, directory):
    filename = directory + sep + "journal_issn.json"
    with open(filename, "w", encoding="utf-8") as fd:
        json.dump(name_issn_dict, fd, ensure_ascii=False, indent=4)


def get_all_files_doci(i_dir_or_targz_file):
    result = []
    targz_fd = None

    if isdir(i_dir_or_targz_file):
        for cur_dir, cur_subdir, cur_files in walk(i_dir_or_targz_file):
            for cur_file in cur_files:
                if cur_file.endswith(".json") and not basename(cur_file).startswith(
                    "."
                ):
                    result.append(cur_dir + sep + cur_file)
    elif i_dir_or_targz_file.endswith("tar.gz"):
        targz_fd = tarfile.open(i_dir_or_targz_file, "r:gz", encoding="utf-8")
        for cur_file in targz_fd:
            if cur_file.name.endswith(".json") and not basename(
                cur_file.name
            ).startswith("."):
                result.append(cur_file)
    else:
        print("It is not possible to process the input path.")
    return result, targz_fd


def valid_date_doci(date_text):
    date_text = str(date_text)
    try:
        return datetime.datetime.strptime(date_text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        try:
            return datetime.datetime.strptime(date_text, "%Y-%m").strftime("%Y-%m")
        except ValueError:
            try:
                return datetime.datetime.strptime(date_text, "%Y").strftime("%Y")
            except ValueError:
                if "-" in date_text:
                    possibiliDate = date_text.split("-")
                    while possibiliDate:
                        possibiliDate.pop()
                        seperator = "-"
                        data = seperator.join(possibiliDate)
                        try:
                            return datetime.datetime.strptime(
                                data, "%Y-%m-%d"
                            ).strftime("%Y-%m-%d")
                        except ValueError:
                            try:
                                return datetime.datetime.strptime(
                                    data, "%Y-%m"
                                ).strftime("%Y-%m")
                            except ValueError:
                                try:
                                    return datetime.datetime.strptime(
                                        data, "%Y"
                                    ).strftime("%Y")
                                except ValueError:
                                    pass


def load_json_doci(file, targz_fd, file_idx, len_all_files):
    result = None

    if targz_fd is None:
        # print("Open file %s of %s" % (file_idx, len_all_files))
        with open(file, encoding="utf8") as f:
            result = load(f)
    else:
        # print("Open file %s of %s (in tar.gz archive)" % (file_idx, len_all_files))
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

    citing_doi_with_no_date = set()
    valid_doi = CSVManager(output_dir + sep + "valid_doi.csv")
    id_date = CSVManager(output_dir + sep + "id_date.csv")
    id_issn = CSVManager(output_dir + sep + "id_issn.csv")
    id_orcid = CSVManager(output_dir + sep + "id_orcid.csv")

    journal_issn_dict = issn_data_recover_doci(output_dir)

    doi_manager = DOIManager()
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()

    all_files, targz_fd = get_all_files_doci(input_dir)
    len_all_files = len(all_files)
    issnDict = {}
    relevant_relations = ["references", "isreferencedby", "cites", "iscitedby"]

    count = 0
    # Read all the JSON files in the DataCite dump to create the main information of all the indexes
    # print("\n\n# Add valid DOIs from DataCite metadata")
    for file_idx, file in enumerate(all_files, 1):
        data = load_json_doci(file, targz_fd, file_idx, len_all_files)
        if "data" in data:
            data_list = data["data"]
            for item in tqdm(data_list):
                count += 1
                attributes = item["attributes"]
                citing_doi = attributes["doi"]
                relatedIdentifiers = attributes["relatedIdentifiers"]
                citing_doi = doi_manager.normalise(citing_doi, True)
                # valid_doi.add_value(citing_doi, "v" if doi_manager.is_valid(citing_doi) else "i")
                valid_doi.add_value(
                    citing_doi, "v"
                )  # NB:  add value aggiunge l'id se non c'era e aggiunge il valore al set di valori. Cosa succede se il valore c'è giù ed è invalid?

                # collect the date of issue if there is, otherwise the year of publciation
                if id_date.get_value(citing_doi) is None:
                    listDates = attributes["dates"]
                    publicationYear = attributes["publicationYear"]

                    if listDates != []:
                        if [
                            data
                            for data in listDates
                            if str(data["dateType"]).lower() == "issued"
                        ]:
                            issue_dates = [
                                data
                                for data in listDates
                                if str(data["dateType"]).lower() == "issued"
                            ]
                            if [
                                iss_date
                                for iss_date in issue_dates
                                if valid_date_doci(str(iss_date["date"]))
                            ]:
                                flt_issue_dates = [
                                    iss_date
                                    for iss_date in issue_dates
                                    if valid_date_doci(str(iss_date["date"]))
                                ]
                                for flt_iss_date in flt_issue_dates:
                                    id_date.add_value(
                                        citing_doi,
                                        valid_date_doci(str(flt_iss_date["date"])),
                                    )
                                    if citing_doi in citing_doi_with_no_date:
                                        citing_doi_with_no_date.remove(citing_doi)
                                    break

                            # c'è listDates, almeno uno degli elementi ha "issued" come valore del campo "dateType"
                            # ma nessuna delle date in listDates ha una data valida nel campo "date"
                            elif publicationYear:
                                publicationYear = valid_date_doci(str(publicationYear))
                                if publicationYear:
                                    id_date.add_value(citing_doi, publicationYear)
                                    if citing_doi in citing_doi_with_no_date:
                                        citing_doi_with_no_date.remove(citing_doi)
                                else:
                                    citing_doi_with_no_date.add(citing_doi)
                            else:
                                citing_doi_with_no_date.add(citing_doi)

                        # c'è listDates ma nessuno degli elementi ha come dateType "issued"
                        elif publicationYear:
                            publicationYear = valid_date_doci(str(publicationYear))
                            if publicationYear:
                                id_date.add_value(citing_doi, publicationYear)
                                if citing_doi in citing_doi_with_no_date:
                                    citing_doi_with_no_date.remove(citing_doi)
                            else:
                                citing_doi_with_no_date.add(citing_doi)
                        else:
                            citing_doi_with_no_date.add(citing_doi)

                    # Non c'è listDates, non ci sono elementi in listDates
                    elif publicationYear:
                        publicationYear = valid_date_doci(str(publicationYear))
                        if publicationYear:
                            id_date.add_value(citing_doi, publicationYear)
                            if citing_doi in citing_doi_with_no_date:
                                citing_doi_with_no_date.remove(citing_doi)
                        else:
                            citing_doi_with_no_date.add(citing_doi)
                    else:
                        citing_doi_with_no_date.add(citing_doi)

                # collect the orcid of the contributors
                if id_orcid.get_value(citing_doi) is None:
                    contributorList = attributes["creators"]
                    if contributorList != []:
                        for author in contributorList:  # da qui!!
                            if "nameIdentifiers" in author.keys():
                                infoAuthor = author["nameIdentifiers"]
                                for element in infoAuthor:
                                    if (
                                        "nameIdentifier" in element.keys()
                                        and "nameIdentifierScheme" in element.keys()
                                    ):
                                        if (
                                            element["nameIdentifierScheme"]
                                        ).lower() == "orcid":
                                            orcid = element["nameIdentifier"]
                                            if orcid is not None and orcid != "":
                                                orcid = orcid_manager.normalise(orcid)
                                                if orcid_manager.is_valid(orcid):
                                                    id_orcid.add_value(
                                                        citing_doi, orcid
                                                    )

                if id_issn.get_value(citing_doi) is None:
                    issn_set = set()

                    if relatedIdentifiers != []:
                        for related in relatedIdentifiers:
                            if "relationType" in related.keys():
                                relationType = related["relationType"]
                                if relationType.lower() == "ispartof":
                                    if "relatedIdentifierType" in related.keys():
                                        relatedIdentifierType = (
                                            str(related["relatedIdentifierType"])
                                        ).lower()
                                        if relatedIdentifierType == "issn":
                                            if "relatedIdentifier" in related.keys():
                                                relatedISSN = str(
                                                    related["relatedIdentifier"]
                                                )
                                                if relatedISSN:
                                                    issn_set.add(relatedISSN)

                    container = attributes["container"]
                    if (
                        "identifier" in container.keys()
                        and "identifierType" in container.keys()
                    ):
                        if (
                            container["identifier"] != ""
                            and (container["identifierType"]).lower() == "issn"
                        ):
                            cont_issn = container["identifier"]
                            issn_set.add(cont_issn)
                            if "title" in container.keys():
                                journal_title = (container["title"]).lower()
                                if journal_title in issnDict.keys():
                                    issnList = issnDict[journal_title]
                                    if issnList != []:
                                        if [
                                            el for el in issnList if el not in issn_set
                                        ]:
                                            issn_set.update(set(issnList))
                                            issnDict[journal_title] = list(issn_set)
                                    else:
                                        issnDict[journal_title] = list(issn_set)
                                else:
                                    issnDict[journal_title] = list(issn_set)

                    normalised_issn_set = set()
                    for issn in issn_set:
                        norm_issn = issn_manager.normalise(issn)
                        normalised_issn_set.add(norm_issn)
                    for issn in normalised_issn_set:
                        if issn_manager.is_valid(issn):
                            id_issn.add_value(citing_doi, issn)

                if int(count) != 0 and int(count) % int(n) == 0:
                    # print("data to cache. processed entities:", count, ". transcription to cache n.", (int(count)//int(n)))
                    issn_data_to_cache_doci(issnDict, output_dir)

    issn_data_to_cache_doci(issnDict, output_dir)
    middle = timer()
    # print("first process duration: :", (middle - start))

    cited_dois = 0
    count = 0
    for file_idx, file in enumerate(all_files, 1):
        data = load_json_doci(file, targz_fd, file_idx, len_all_files)
        if "data" in data:
            data_list = data["data"]
            for item in tqdm(data_list):
                count += 1
                # print("processing entity n.", count, "for cited dois")
                attributes = item["attributes"]
                relatedIdentifiers = attributes["relatedIdentifiers"]
                if relatedIdentifiers != []:
                    for related in relatedIdentifiers:
                        relationType = related["relationType"]
                        if relationType:
                            if relationType.lower() in relevant_relations:
                                if "relatedIdentifierType" in related.keys():
                                    relatedIdentifierType = (
                                        str(related["relatedIdentifierType"])
                                    ).lower()
                                    if relatedIdentifierType == "doi":
                                        if "relatedIdentifier" in related.keys():
                                            relatedDOI = doi_manager.normalise(
                                                related["relatedIdentifier"], True
                                            )
                                            if valid_doi.get_value(relatedDOI) is None:
                                                valid_doi.add_value(
                                                    relatedDOI,
                                                    "v"
                                                    if doi_manager.is_valid(relatedDOI)
                                                    else "i",
                                                )
                                                cited_dois += 1
                                            if valid_doi.get_value(relatedDOI) == {"v"} and id_date.get_value(relatedDOI) is None:
                                                citing_doi_with_no_date.add(relatedDOI)

    for doi in citing_doi_with_no_date:
        id_date.add_value(doi, "")

    end = timer()
    # print("second process duration: ", end-middle)
    # print("full process duration: ", end-start)


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


if __name__ == "__main__":
    main()

# python "scripts/glob_doci.py" -i ./index/python/test/data/doci_glob_dump_input -o ./index/python/test/data/doci_glob_dump_output -n 3

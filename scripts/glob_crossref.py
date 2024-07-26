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
from os.path import exists, basename, isdir
from json import load, loads
from collections import Counter
from datetime import date
from timeit import default_timer as timer
from re import sub
import tarfile


from oc.index.oci.citation import Citation
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.glob.csv import CSVDataSource
from oc.index.glob.redis import RedisDataSource
from oc.index.utils.config import get_config


def build_pubdate_coci(obj):
    if "issued" in obj:  # Main citing object
        if "date-parts" in obj["issued"]:
            # is an array of parts of dates
            try:
                obj_date = obj["issued"]["date-parts"][0]

                # lisdate[year,month,day]
                listdate = [1, 1, 1]
                dateparts = []
                for i in range(0, len(obj_date)):
                    try:
                        dateparts.append(obj_date[i])
                        intvalue = int(obj_date[i])
                        listdate[i] = intvalue
                    except:
                        pass

                # there is a date, so generate it
                if (
                    (1 < listdate[0] < 3000)
                    and (0 < listdate[1] <= 12)
                    and (0 < listdate[2] <= 31)
                ):
                    date_val = date(listdate[0], listdate[1], listdate[2])
                    dformat = "%Y"

                    # only month is specified
                    if len(dateparts) == 2:
                        dformat = "%Y-%m"
                    elif len(dateparts) == 3 and (
                        dateparts[1] != 1 or (dateparts[1] == 1 and dateparts[2] != 1)
                    ):
                        dformat = "%Y-%m-%d"

                    date_in_str = date_val.strftime(dformat)
                    return date_in_str
            except:
                pass
    elif "year" in obj:  # Reference object
        ref_year = sub("[^\d]", "", obj["year"])[:4]
        if ref_year:
            return ref_year

    return None


def get_all_files_coci(i_dir_or_targz_file):
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


def load_json_coci(file, targz_fd, file_idx, len_all_files):
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


def process_coci(input_dir, output_dir, process_type):
    start = timer()
    if not exists(output_dir):
        makedirs(output_dir)

    doi_manager = DOIManager()
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()

    config = get_config()
    if process_type == "process":
        service_ds = config.get("COCI", "datasource")
    else:
        service_ds = config.get("COCI_T", "datasource")
    svc_datasource = None
    if service_ds == "redis":
        if process_type == "process":
            svc_datasource = RedisDataSource("COCI")
        else:
            svc_datasource = RedisDataSource("COCI_T")
    elif service_ds == "csv":
        if process_type == "process":
            svc_datasource = CSVDataSource("COCI")
        else:
            svc_datasource = CSVDataSource("COCI_T")
    else:
        raise Exception(service_ds + " is not a valid data source")

    all_files, targz_fd = get_all_files_coci(input_dir)
    len_all_files = len(all_files)

    # Read all the JSON file in the Crossref dump to create the main information of all the indexes
    # print("\n\n# Add valid DOIs from Crossref metadata")
    for file_idx, file in enumerate(all_files, 1):
        data = load_json_coci(file, targz_fd, file_idx, len_all_files)

        if "items" in data:
            for obj in data["items"]:
                if "DOI" in obj:
                    citing_doi = doi_manager.normalise(obj["DOI"], True)
                    if citing_doi is not None:
                        entity = svc_datasource.get(citing_doi)
                        if entity is None:
                            entity = dict()
                            entity["valid"] = True

                            citing_date = Citation.check_date(build_pubdate_coci(obj))
                            if citing_date is not None:
                                entity["date"] = [citing_date]

                            valid_issn_list = []
                            if "type" in obj:
                                cur_type = obj["type"]
                                if (
                                    cur_type is not None
                                    and "journal" in cur_type
                                    and "ISSN" in obj
                                ):
                                    cur_issn = obj["ISSN"]
                                    if cur_issn is not None:
                                        for issn in [
                                            issn_manager.normalise(issn)
                                            for issn in cur_issn
                                        ]:
                                            if issn is not None:
                                                valid_issn_list.append(issn)

                            if len(valid_issn_list) > 0:
                                entity["issn"] = valid_issn_list

                            orcid_list = []
                            if "author" in obj:
                                cur_author = obj["author"]
                                if cur_author is not None:
                                    for author in cur_author:
                                        if "ORCID" in author:
                                            orcid = orcid_manager.normalise(
                                                author["ORCID"]
                                            )
                                            if orcid is not None:
                                                orcid_list.append(orcid)
                            if len(orcid_list) > 0:
                                entity["orcid"] = orcid_list
                            svc_datasource.set(citing_doi, entity)

    middle = timer()
    # print("first process duration: :", (middle - start))
    # Do it again for updating the dates of the cited DOIs, if these are valid
    # print("\n\n# Check cited DOIs from Crossref reference field")
    doi_date = {}
    entity_with_date_to_updete = {}
    for file_idx, file in enumerate(all_files, 1):
        data = load_json_coci(file, targz_fd, file_idx, len_all_files)

        if "items" in data:
            for obj in data["items"]:
                if "DOI" in obj and "reference" in obj:
                    for ref in obj["reference"]:
                        if "DOI" in ref:
                            cited_doi = str(doi_manager.normalise(ref["DOI"], True))
                            if cited_doi is not None:
                                cited_doi_entity = svc_datasource.get(cited_doi)
                                if cited_doi_entity is None:
                                    cited_doi_entity = dict()
                                    cited_doi_entity["valid"] = (
                                        True
                                        if doi_manager.is_valid(cited_doi)
                                        else False
                                    )

                                if (
                                    cited_doi_entity["valid"] is True
                                    and "date" not in cited_doi_entity.keys()
                                ):
                                    if cited_doi not in doi_date:
                                        doi_date[cited_doi] = []
                                    cited_date = Citation.check_date(
                                        build_pubdate_coci(ref)
                                    )
                                    if cited_date is not None:
                                        doi_date[cited_doi].append(cited_date)

                                if cited_doi in doi_date:
                                    if len(doi_date[cited_doi]) > 0:
                                        entity_with_date_to_updete[
                                            cited_doi
                                        ] = cited_doi_entity
                                    else:
                                        svc_datasource.set(cited_doi, cited_doi_entity)
                                else:
                                    svc_datasource.set(cited_doi, cited_doi_entity)

    # Add the date to the DOI if such date is the most adopted one in the various references.
    # In case two distinct dates are used the most, select the older one.
    for doi in doi_date:
        if doi in entity_with_date_to_updete.keys():
            count = Counter(doi_date[doi])
            if len(count):
                top_value = count.most_common(1)[0][1]
                selected_dates = []
                for date in count:
                    if count[date] == top_value:
                        selected_dates.append(date)
                best_date = sorted(selected_dates)[0]
                doi_entity_for_date_update = entity_with_date_to_updete[doi]
                doi_entity_for_date_update["date"] = [best_date]
                svc_datasource.set(doi, doi_entity_for_date_update)
            else:
                doi_entity_for_date_update = entity_with_date_to_updete[doi]
                svc_datasource.set(doi, doi_entity_for_date_update)

    # Close the file descriptor of the tar.gz archive if it was used
    if targz_fd is not None:
        targz_fd.close()

    end = timer()
    # print("second process duration: ", end-middle)
    # print("full process duration: ", end-start)


def main():
    arg_parser = ArgumentParser(
        "Global files creator for COCI",
        description="Process Crossref JSON files and create global indexes to enable "
        "the creation of COCI.",
    )
    arg_parser.add_argument(
        "-i",
        "--input",
        dest="input",
        required=True,
        help="Either the directory or the zip file that contains the Crossref data dump "
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
        "-p",
        "--process_type",
        dest="process_type",
        required=True,
        choices=['process', 'test'],
        help="scope of the process to be run, either 'process' or 'test'. Choose 'test' in case the script is run for"
             "testing purposes and 'process' if the script is run for processing the full glob.",
    )

    args = arg_parser.parse_args()
    process_coci(args.input, args.output, args.process_type)

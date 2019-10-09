#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019,
# Ivan Heibi <ivanhb.ita@gmail.com>
# Silvio Peroni <essepuntato@gmail.com>
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
from index.storer.csvmanager import CSVManager
from index.identifier.doimanager import DOIManager
from index.identifier.issnmanager import ISSNManager
from index.identifier.orcidmanager import ORCIDManager
from os import sep, makedirs, walk
from os.path import exists
from json import load
from collections import Counter
from datetime import date
from re import sub


def build_pubdate(obj):
    if 'issued' in obj:  # Main citing object
        if 'date-parts' in obj['issued']:
            # is an array of parts of dates
            try:
                obj_date = obj['issued']['date-parts'][0]

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

                # I have a date, so generate it
                if (1 < listdate[0] < 3000) and (0 < listdate[1] <= 12) and (0 < listdate[2] <= 31):
                    date_val = date(listdate[0], listdate[1], listdate[2])
                    dformat = '%Y'

                    # only month is specified
                    if len(dateparts) == 2:
                        dformat = '%Y-%m'
                    elif len(dateparts) == 3 and (dateparts[1] != 1 or (dateparts[1] == 1 and dateparts[2] != 1)):
                        dformat = '%Y-%m-%d'

                    date_in_str = date_val.strftime(dformat)
                    return date_in_str
            except:
                pass
    elif 'year' in obj:  # Reference object
        ref_year = sub("[^\d]", "", obj["year"])[:4]
        if ref_year:
            return ref_year

    return None


def get_all_files(i_dir):
    result = []

    for cur_dir, cur_subdir, cur_files in walk(i_dir):
        for file in cur_files:
            if file.lower().endswith('.json'):
                result.append(cur_dir + sep + file)

    return result


def process(input_dir, output_dir):
    if not exists(output_dir):
        makedirs(output_dir)

    citing_doi_with_no_date = set()
    valid_doi = CSVManager(output_dir + sep + "valid_doi.csv")
    id_date = CSVManager(output_dir + sep + "id_date.csv")
    id_issn = CSVManager(output_dir + sep + "id_issn.csv")
    id_orcid = CSVManager(output_dir + sep + "id_orcid.csv")

    doi_manager = DOIManager(valid_doi)
    issn_manager = ISSNManager()
    orcid_manager = ORCIDManager()

    all_files = get_all_files(input_dir)

    # Read all the JSON file in the Crossref dump to create the main information of all the indexes
    for file in all_files:
        with open(file) as f:
            data = load(f)
            if "items" in data:
                for obj in data['items']:
                    if "DOI" in obj:
                        citing_doi = doi_manager.normalise(obj["DOI"], True)
                        doi_manager.set_valid(citing_doi)

                        if id_date.get_value(citing_doi) is None:
                            citing_date = build_pubdate(obj)
                            if citing_date is not None:
                                id_date.add_value(citing_doi, citing_date)
                                if citing_doi in citing_doi_with_no_date:
                                    citing_doi_with_no_date.remove(citing_doi)
                            else:
                                citing_doi_with_no_date.add(citing_doi)

                        if id_issn.get_value(citing_doi) is None:
                            if "type" in obj:
                                cur_type = obj["type"]
                                if cur_type is not None and "journal" in cur_type and "ISSN" in obj:
                                    cur_issn = obj["ISSN"]
                                    if cur_issn is not None:
                                        for issn in [issn_manager.normalise(issn) for issn in cur_issn]:
                                            if issn is not None:
                                                id_issn.add_value(citing_doi, issn)

                        if id_orcid.get_value(citing_doi) is None:
                            if "author" in obj:
                                cur_author = obj['author']
                                if cur_author is not None:
                                    for author in cur_author:
                                        if "ORCID" in author:
                                            orcid = orcid_manager.normalise(author["ORCID"])
                                            if orcid is not None:
                                                id_orcid.add_value(citing_doi, orcid)

    # Do it again for updating the dates of the cited DOIs, if these are valid
    doi_date = {}
    for file in all_files:
        with open(file) as f:
            data = load(f)
            if "items" in data:
                for obj in data['items']:
                    if "DOI" in obj and "reference" in obj:
                        for ref in obj['reference']:
                            if "DOI" in ref:
                                cited_doi = doi_manager.normalise(ref["DOI"], True)
                                if doi_manager.is_valid(cited_doi) and id_date.get_value(cited_doi) is None:
                                    if cited_doi not in doi_date:
                                        doi_date[cited_doi] = []
                                    cited_date = build_pubdate(ref)
                                    if cited_date is not None:
                                        doi_date[cited_doi].append(cited_date)
                                        if cited_doi in citing_doi_with_no_date:
                                            citing_doi_with_no_date.remove(cited_doi)

    # Add the date to the DOI if such date is the most adopted one in the various references.
    # In case two distinct dates are used the most, select the older one.
    for doi in doi_date:
        count = Counter(doi_date[doi])
        if len(count):
            top_value = count.most_common(1)[0][1]
            selected_dates = []
            for date in count:
                if count[date] == top_value:
                    selected_dates.append(date)
            best_date = sorted(selected_dates)[0]
            id_date.add_value(doi, best_date)
        else:
            id_date.add_value(doi, "")

    # Add emtpy dates for the remaining DOIs
    for doi in citing_doi_with_no_date:
        id_date.add_value(doi, "")


if __name__ == "__main__":
    arg_parser = ArgumentParser("Global files creator for COCI",
                                description="Process Crossref JSON files and create global indexes to enable "
                                            "the creation of COCI.")
    arg_parser.add_argument("-i", "--input_dir", dest="input_dir", required=True,
                            help="The directory that contains the Crossref data dump of JSON files.")
    arg_parser.add_argument("-o", "--output_dir", dest="output_dir", required=True,
                            help="The directory where the indexes are stored.")

    args = arg_parser.parse_args()
    process(args.input_dir, args.output_dir)

#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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
from urllib.parse import quote
from datetime import datetime
from os.path import abspath, dirname, basename
from os import sep
from sys import path
from importlib import import_module
from index.storer.csvmanager import CSVManager
from index.identifier.doimanager import DOIManager
from index.finder.orcidresourcefinder import ORCIDResourceFinder
from index.finder.dataciteresourcefinder import DataCiteResourceFinder
from index.finder.crossrefresourcefinder import CrossrefResourceFinder
from index.finder.resourcefinder import ResourceFinderHandler
from index.citation.oci import OCIManager, Citation
from index.storer.citationstorer import CitationStorer
from index.citation.citationsource import CSVFileCitationSource
from index.coci.crossrefcitationsource import CrossrefCitationSource
from index.croci.crowdsourcedcitationsource import CrowdsourcedCitationSource

import ray


def create_csv(doi_file, date_file, orcid_file, issn_file):
    valid_doi = CSVManager(csv_path=doi_file)
    id_date = CSVManager(csv_path=date_file)
    id_orcid = CSVManager(csv_path=orcid_file)
    id_issn = CSVManager(csv_path=issn_file)

    return valid_doi, id_date, id_orcid, id_issn


def import_citation_source(python, pclass, input):
    addon_abspath = abspath(python)
    path.append(dirname(addon_abspath))
    addon = import_module(basename(addon_abspath).replace(".py", ""))
    return getattr(addon, pclass)(input)


class DataHandler(object):
    _source_classes = {
        "csv": CSVFileCitationSource,
        "crossref": CrossrefCitationSource,
        "croci": CrowdsourcedCitationSource
    }

    def __init__(self, lookup, data, pclass, inp, 
                 id_manager, rf_handler):
        self.oci_manager = OCIManager(lookup_file=lookup)
        self.exi_ocis = CSVManager.load_csv_column_as_set(
            data + sep + "data", "oci")
        self.cs = self._source_classes[pclass](inp)
        self.id_manager = id_manager
        self.rf_handler = rf_handler
        self.new_citations_added = 0
        self.citations_already_present = 0
        self.error_in_ids_existence = 0

    def get_values(self):
        return self.new_citations_added, self.citations_already_present, self.error_in_ids_existence

    def get_oci(self, citing, cited, prefix):
        return self.oci_manager.get_oci(citing, cited, prefix)

    def get_next_citation_data(self):
        return self.cs.get_next_citation_data()

    def are_valid(self, citing, cited):
        result = self.id_manager.is_valid(citing) and \
                 self.id_manager.is_valid(cited)
        if result:
            self.new_citations_added += 1
        else:
            self.error_in_ids_existence += 1

        return result
    
    def share_orcid(self, citing, cited):
        return self.rf_handler.share_orcid(citing, cited)
    
    def share_issn(self, citing, cited):
        return self.rf_handler.share_issn(citing, cited)
    
    def get_date(self, id_string):
        return self.rf_handler.get_date(id_string)

    def oci_exists(self, oci):
        result = oci in self.exi_ocis
        if result:
            self.citations_already_present += 1
        else:
            self.exi_ocis.add(oci)
        return result


@ray.remote
class ParallelDataHandler(DataHandler):
    pass


def execute_workflow(idbaseurl, baseurl, pclass, input, doi_file, date_file,
                     orcid_file, issn_file, orcid, lookup, data, prefix, agent, source, service, verbose, no_api, process_number):
    # Create the support file for handling information about bibliographic resources
    valid_doi, id_date, id_orcid, id_issn = create_csv(doi_file, date_file, orcid_file, issn_file)

    doi_manager = DOIManager(valid_doi, use_api_service=not no_api)
    crossref_rf = CrossrefResourceFinder(
        date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi, use_api_service=not no_api)
    datacite_rf = DataCiteResourceFinder(
        date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi, use_api_service=not no_api)
    orcid_rf = ORCIDResourceFinder(
        date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi,
        use_api_service=True if orcid is not None and not no_api else False, key=orcid)

    rf_handler = ResourceFinderHandler([crossref_rf, datacite_rf, orcid_rf])
    
    if process_number > 1:  # Run process in parallel via RAY
        ray.init(num_cpus=process_number)
        p_handler = ParallelDataHandler.remote(lookup, data, pclass, input, doi_manager, rf_handler)
        futures = [_parallel_extract_citations.remote(data, idbaseurl, baseurl, prefix, agent, source, service, verbose, p_handler, str(i)) 
                   for i in range(process_number - 1)]
        ray.get(futures)
        return ray.get(p_handler.get_values.remote())
    else:
        p_handler = DataHandler(lookup, data, pclass, input, doi_manager, rf_handler)
        _extract_citations(data, idbaseurl, baseurl, prefix, agent, source, service, verbose, p_handler)
        return p_handler.get_values()

@ray.remote
def _parallel_extract_citations(data, idbaseurl, baseurl, prefix, agent, 
                               source, service, verbose, p_handler, suffix):
    return _extract_citations(data, idbaseurl, baseurl, prefix, agent, 
                              source, service, verbose, p_handler, suffix, True)

def _extract_citations(data, idbaseurl, baseurl, prefix, agent, 
                      source, service, verbose, p_handler, suffix="", parallel=False):
    h_get_next_citation_data = lambda h: \
        ray.get(h.get_next_citation_data.remote()) if parallel \
        else h.get_next_citation_data()
    h_get_oci = lambda h, citing, cited, prefix: \
        ray.get(h.get_oci.remote(citing, cited, prefix)) if parallel \
        else h.get_oci(citing, cited, prefix)
    h_oci_exists = lambda h, oci: \
        ray.get(h.oci_exists.remote(oci)) if parallel \
        else h.oci_exists(oci)
    h_are_valid = lambda h, citing, cited: \
        ray.get(h.are_valid.remote(citing, cited)) if parallel \
        else h.are_valid(citing, cited)
    h_get_date = lambda h, id_string: \
        ray.get(h.get_date.remote(id_string)) if parallel \
        else h.get_date(id_string)
    h_share_issn = lambda h, citing, cited: \
        ray.get(h.share_issn.remote(citing, cited)) if parallel \
        else h.share_issn(citing, cited)
    h_share_orcid = lambda h, citing, cited: \
        ray.get(h.share_orcid.remote(citing, cited)) if parallel \
        else h.share_orcid(citing, cited)
    
    BASE_URL = idbaseurl
    DATASET_URL = baseurl + "/" if not baseurl.endswith("/") else baseurl

    cit_storer = CitationStorer(data, DATASET_URL, suffix=suffix)
    next_citation = h_get_next_citation_data(p_handler)

    while next_citation is not None:
        citing, cited, created, timespan, journal_sc, author_sc = next_citation
        oci = h_get_oci(p_handler, citing, cited, prefix)
        oci_noprefix = oci.replace("oci:", "")
        
        if not h_oci_exists(p_handler, oci_noprefix):
            if h_are_valid(p_handler, citing, cited):
                if created is None:
                    citing_date = h_get_date(p_handler, citing)
                else:
                    citing_date = created
                cited_date = h_get_date(p_handler, cited)
                if journal_sc is None or type(journal_sc) is not bool:
                    journal_sc = h_share_issn(p_handler, citing, cited)
                if author_sc is None or type(author_sc) is not bool:
                    author_sc = h_share_orcid(p_handler, citing, cited)

                if created is not None and timespan is not None:
                    cit = Citation(oci,
                                   BASE_URL + quote(citing), None,
                                   BASE_URL + quote(cited), None,
                                   created, timespan,
                                   1, agent, source, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                                   service, "doi", BASE_URL + "([[XXX__decode]])", "reference",
                                   journal_sc, author_sc,
                                   None, "Creation of the citation", None)
                else:
                    cit = Citation(oci,
                                   BASE_URL + quote(citing), citing_date,
                                   BASE_URL + quote(cited), cited_date,
                                   None, None,
                                   1, agent, source, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                                   service, "doi", BASE_URL + "([[XXX__decode]])", "reference",
                                   journal_sc, author_sc,
                                   None, "Creation of the citation", None)

                cit_storer.store_citation(cit)

                if verbose:
                    print("Create citation data for '%s' between DOI '%s' and DOI '%s'" % (oci, citing, cited))
            else:
                if verbose:
                    print("WARNING: some DOIs, among '%s' and '%s', do not exist" % (citing, cited))
        else:
            if verbose:
                print("WARNING: the citation between DOI '%s' and DOI '%s' has been already processed" %
                      (citing, cited))

        next_citation = h_get_next_citation_data(p_handler)


if __name__ == "__main__":
    arg_parser = ArgumentParser("cnc.py (Create New Citations",
                                description="This tool allows one to take a series of entity-to-entity"
                                            "citation data, and to store it according to CSV used by"
                                            "the OpenCitations Indexes so as to be added to an Index. It uses"
                                            "several online services to check several things to create the"
                                            "final CSV/TTL/Scholix files.")

    arg_parser.add_argument("-c", "--pclass", required=True,
                            help="The name of the class of data source to use to process citatation data.", 
                            choices=['csv', 'crossref', 'croci'])
    arg_parser.add_argument("-i", "--input", required=True,
                            help="The input file/directory to provide as input of the specified input "
                                 "Python file (using -p).")
    arg_parser.add_argument("-d", "--data", required=True,
                            help="The directory containing all the CSV files already added in the Index, "
                                 "including data and provenance files.")
    arg_parser.add_argument("-o", "--orcid", default=None,
                            help="ORCID API key to be used to query the ORCID API.")
    arg_parser.add_argument("-l", "--lookup", required=True,
                            help="The lookup table that must be used to produce OCIs.")
    arg_parser.add_argument("-b", "--baseurl", required=True, default="",
                            help="The base URL of the dataset")
    arg_parser.add_argument("-ib", "--idbaseurl", required=True, default="",
                            help="The base URL of the identifier of citing and cited entities, if any")
    arg_parser.add_argument("-doi", "--doi_file", default=None,
                            help="The file where the valid and invalid DOIs are stored.")
    arg_parser.add_argument("-date", "--date_file", default=None,
                            help="The file that maps id of bibliographic resources with their publication date.")
    arg_parser.add_argument("-orcid", "--orcid_file", default=None,
                            help="The file that maps id of bibliographic resources with the ORCID of its authors.")
    arg_parser.add_argument("-issn", "--issn_file", default=None,
                            help="The file that maps id of bibliographic resources with the ISSN of the journal "
                                 "they have been published in.")
    arg_parser.add_argument("-px", "--prefix", default="",
                            help="The '0xxx0' prefix to use for creating the OCIs.")
    arg_parser.add_argument("-a", "--agent", required=True, default="https://w3id.org/oc/index/prov/pa/1",
                            help="The URL of the agent providing or processing the citation data.")
    arg_parser.add_argument("-s", "--source", required=True,
                            help="The URL of the source from where the citation data have been extracted.")
    arg_parser.add_argument("-sv", "--service", required=True,
                            help="The name of the service that will made available the citation data.")
    arg_parser.add_argument("-v", "--verbose", action="store_true", default=False,
                            help="Print the messages on screen.")
    arg_parser.add_argument("-na", "--no_api", action="store_true", default=False,
                            help="Tell the tool explicitly not to use the APIs of the various finders.")
    arg_parser.add_argument("-pn", "--process_number", default=1, type=int,
                            help="The number of parallel process to run for working on the creation of citations.")

    args = arg_parser.parse_args()
    
    new_citations_added, citations_already_present, error_in_dois_existence = \
        execute_workflow(args.idbaseurl, args.baseurl, args.pclass, 
                         args.input, args.doi_file, args.date_file, args.orcid_file, 
                         args.issn_file, args.orcid, args.lookup, args.data, 
                         args.prefix, args.agent, args.source, args.service, 
                         args.verbose, args.no_api, args.process_number)
    
    print("\n# Summary\n"
          "Number of new citations added to the OpenCitations Index: %s\n"
          "Number of citations already present in the OpenCitations Index: %s\n"
          "Number of citations with invalid DOIs: %s" %
          (new_citations_added, citations_already_present, error_in_dois_existence))

# How to call the service (e.g. for COCI)
# python -m index.cnc -ib "http://dx.doi.org/" -b "https://w3id.org/oc/index/coci/" -p "index/citation/citationsource.py" -c "CSVFileCitationSource" -i "index/test_data/citations_partial.csv" -doi "index/coci_test/doi.csv" -orcid "index/coci_test/orcid.csv" -date "index/coci_test/date.csv" -issn "index/coci_test/issn.csv" -l "index/test_data/lookup_full.csv" -d "index/coci_test" -px "020" -a "https://w3id.org/oc/index/prov/pa/1" -s "https://api.crossref.org/works/[[citing]]" -sv "OpenCitations Index: COCI" -v

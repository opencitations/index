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
from os import sep
from index.citation.oci import Citation
from index.storer.citationstorer import CitationStorer
from index.storer.datahandler import FileDataHandler

import ray


@ray.remote
class ParallelFileDataHandler(FileDataHandler):
    """This class makes the FileDataHandler class as a remote object"""
    pass

def execute_workflow(idbaseurl, baseurl, pclass, inp, doi_file, date_file,
                     orcid_file, issn_file, orcid, lookup, data, prefix, agent, 
                     source, service, verbose, no_api, process_number):
    if process_number > 1:  # Run process in parallel via RAY
        ray.init(num_cpus=process_number)
        p_handler = ParallelFileDataHandler.remote(pclass, inp, lookup)
        id_remote_init = p_handler.init.remote(data, doi_file, date_file, orcid_file, issn_file, orcid, no_api)
        ray.wait([id_remote_init])  # wait until the handler is not ready
        futures = [_parallel_extract_citations.remote(data, idbaseurl, baseurl, prefix, agent, source, 
                                                      service, verbose, p_handler, str(i)) 
                   for i in range(process_number - 1)]
        ray.get(futures)
        return ray.get(p_handler.get_values.remote())
    else:
        p_handler = FileDataHandler(pclass, inp, lookup)
        p_handler.init(data, doi_file, date_file, orcid_file, issn_file, orcid, no_api)
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
# python cnc.py -ib "http://dx.doi.org/" -b "https://w3id.org/oc/index/coci/" -c "csv" -i "index/test_data/citations_partial.csv" -doi "index/coci_test/doi.csv" -orcid "index/coci_test/orcid.csv" -date "index/coci_test/date.csv" -issn "index/coci_test/issn.csv" -l "index/test_data/lookup_full.csv" -d "index/coci_test" -px "020" -a "https://w3id.org/oc/index/prov/pa/1" -s "https://api.crossref.org/works/[[citing]]" -sv "OpenCitations Index: COCI" -v

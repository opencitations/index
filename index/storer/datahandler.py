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

from abc import abstractmethod
from index.citation.citationsource import CSVFileCitationSource
from index.coci.crossrefcitationsource import CrossrefCitationSource
from index.croci.crowdsourcedcitationsource import CrowdsourcedCitationSource
from index.storer.csvmanager import CSVManager
from index.identifier.doimanager import DOIManager
from index.finder.orcidresourcefinder import ORCIDResourceFinder
from index.finder.dataciteresourcefinder import DataCiteResourceFinder
from index.finder.crossrefresourcefinder import CrossrefResourceFinder
from index.finder.resourcefinder import ResourceFinderHandler
from index.citation.oci import OCIManager
from os import sep
from re import match
from collections import OrderedDict

from rethinkdb import RethinkDB


class DataHandler(object):
    """A class acting as a proxy for accessing specific data useful to create citations."""
    _source_classes = {
        "csv": CSVFileCitationSource,
        "crossref": CrossrefCitationSource,
        "croci": CrowdsourcedCitationSource
    }

    def __init__(self, pclass, inp, lookup):
        self.cs = self._source_classes[pclass](inp)
        self.oci_manager = OCIManager(lookup_file=lookup)
        
        self.new_citations_added = 0
        self.citations_already_present = 0
        self.error_in_ids_existence = 0

    def get_values(self):
        """It returns the number of citations added, already present, and with id errors."""
        return self.new_citations_added, self.citations_already_present, self.error_in_ids_existence

    def get_oci(self, citing, cited, prefix):
        """It returns an OCI computed considering the identifiers of the citing and cited
        entities and a prefix."""
        return self.oci_manager.get_oci(citing, cited, prefix)

    def get_next_citation_data(self):
        """It returns the next available citation data in the citation data source specified."""
        return self.cs.get_next_citation_data()

    @abstractmethod
    def init(self, *params):
        """This method allows one to initialise all internal variable to make
        the data handler works corretly. It must be implemented in each particular
        subclass."""
        pass

    @abstractmethod
    def are_valid(self, citing, cited):
        """This method checks if the identifiers of the citing and cited entities
        are both valid (it returns True in this case, otherwise it returns False)."""
        pass
    
    @abstractmethod
    def share_orcid(self, citing, cited):
        """This method checks if the citing and cited entities share at least
        one ORCID (it returns True in this case, otherwise it returns False)."""
        pass
    
    @abstractmethod
    def share_issn(self, citing, cited):
        """This method checks if the citing and cited entities share at least
        one ISSN (it returns True in this case, otherwise it returns False)."""
        pass
    
    @abstractmethod
    def get_date(self, id_string):
        """This method retrives the date of publication of the entity indetified by
        the input string."""
        pass

    @abstractmethod
    def oci_exists(self, oci):
        """This method checks if the OCI in input has been already added to a
        database (it returns True in this case, otherwise it returns False)."""
        pass

class FileDataHandler(DataHandler):
    @staticmethod
    def _create_csv(doi_file, date_file, orcid_file, issn_file):
        valid_doi = CSVManager(csv_path=doi_file)
        id_date = CSVManager(csv_path=date_file)
        id_orcid = CSVManager(csv_path=orcid_file)
        id_issn = CSVManager(csv_path=issn_file)

        return valid_doi, id_date, id_orcid, id_issn

    def init(self, data, doi_file, date_file, orcid_file, issn_file, orcid, no_api):
        valid_doi, id_date, id_orcid, id_issn = \
            FileDataHandler._create_csv(doi_file, date_file, orcid_file, issn_file)

        self.id_manager = DOIManager(valid_doi, use_api_service=not no_api)
        crossref_rf = CrossrefResourceFinder(
            date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi, use_api_service=not no_api)
        datacite_rf = DataCiteResourceFinder(
            date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi, use_api_service=not no_api)
        orcid_rf = ORCIDResourceFinder(
            date=id_date, orcid=id_orcid, issn=id_issn, doi=valid_doi,
            use_api_service=True if orcid is not None and not no_api else False, key=orcid)

        self.rf_handler = ResourceFinderHandler([crossref_rf, datacite_rf, orcid_rf])

        self.exi_ocis = CSVManager.load_csv_column_as_set(
            data + sep + "data", "oci")
        
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

class RethinkDBCache:
    def __init__(self, size):
        self.__size = size
        self.__mem = OrderedDict()
        self.__n = 0 
    
    def add(self, key, value):
        if(self.__n >= self.__size):
            self.__mem.popitem(last=False)
        self.__mem[key] = value
        self.__n = len(self.__mem.keys())
    
    def get(self, key):
        return self.__mem.get(key)

class RethinkDBDataHandler(DataHandler): 
    def __init__(self, pclass, inp, lookup, address, port, cache_size):
        #super().__init__(pclass, inp, lookup)
        self.__r = RethinkDB()
        self.__r.connect(address, port).repl()
        self.__cache_doi = RethinkDBCache(cache_size)
        self.__doi_manager = DOIManager()
    
    def share_orcid(self, citing, cited):
        return self.__check_intersection(citing, cited, "orcid")
    
    def share_issn(self, doi, issn):
        return self.__check_intersection(doi, issn, "issn")

    def __get_doi(self, doi):
        doi = self.__doi_manager.normalise(doi, include_prefix=True)
        if doi is None or match("^doi:10\\..+/.+$", doi) is None:
            return None

        cached_value = self.__cache_doi.get(doi)
        if cached_value is None:
            return self.__r.db("oc").table("doi").get(doi).run()
        else:
            return cached_value

    def __check_intersection(self, doi_1, doi_2, field):
        doi_1 = self.__get_doi(doi_1)
        doi_2 = self.__get_doi(doi_2)
        if doi_1 is None or doi_2 is None:
            return False
        intersection = set(doi_1[field]) & set(doi_2[field])
        return len(intersection) > 0
    
    def get_date(self, doi):
        doi = self.__get_doi(doi)
        if(doi != None):
            return doi["date"]
        else:
            return None

    def oci_exists(self, oci):
        insert_obj = self.__r.db("oc").table("oci").insert({
            "oci": oci
        }).run()
        if insert_obj["errors"] > 0:
            #self.citations_already_present += 1
            return True
        return False

    def are_valid(self, citing, cited):
        doi_citing = self.__get_doi(citing)
        doi_cited = self.__get_doi(cited)
        result = (not doi_citing is None) and doi_citing["validity"] and (not doi_cited is None) and doi_cited["validity"]
        
        if result:
            #self.new_citations_added += 1
            pass
        else:
            #self.error_in_ids_existence += 1
            pass
        
        return result
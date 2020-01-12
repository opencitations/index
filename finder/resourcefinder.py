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

from index.storer.csvmanager import CSVManager
from index.identifier.orcidmanager import ORCIDManager
from index.identifier.doimanager import DOIManager
from index.identifier.issnmanager import ISSNManager
from collections import deque
# TODO: For multiprocessing purposes
# from multiprocessing.managers import BaseManager


class ResourceFinder(object):
    """This is the abstract class that must be implemented by any resource finder
    for a particular service (Crossref, DataCite, ORCiD, etc.). It provides
    the signatures of the methods that should be implemented, and a basic
    constructor."""

    def __init__(self, date=None, orcid=None, issn=None, doi=None, **params):
        if date is None:
            date = CSVManager(store_new=False)
        if orcid is None:
            orcid = CSVManager(store_new=False)
        if issn is None:
            issn = CSVManager(store_new=False)
        if doi is None:
            doi = CSVManager(store_new=False)

        for key in params:
            setattr(self, key, params[key])

        self.issn = issn
        self.date = date
        self.orcid = orcid
        if hasattr(self, 'use_api_service'):
            self.dm = DOIManager(doi, self.use_api_service)
        else:
            self.dm = DOIManager(doi)
        self.im = ISSNManager()
        self.om = ORCIDManager()

        self.headers = {
            "User-Agent": "ResourceFinder / OpenCitations Indexes "
                          "(http://opencitations.net; mailto:contact@opencitations.net)"
        }

        # TODO: For multiprocessing purposes
        # c_type = type(self)
        # BaseManager.register(c_type.__name__, c_type)

    def get_orcid(self, id_string):
        pass

    def get_pub_date(self, id_string):
        pass

    def get_container_issn(self, id_string):
        pass

    def is_valid(self, id_string):
        pass

    def normalise(self, id_string):
        pass


class ApiDOIResourceFinder(ResourceFinder):
    """This is the abstract class that must be implemented by any resource finder
        for a particular service which is based on DOI retrieving via HTTP REST APIs
        (Crossref, DataCite). It provides basic methods that are be used for
        implementing the main methods of the ResourceFinder abstract class."""

    # The following four methods are those ones that should be implemented in
    # the concrete subclasses of this abstract class.
    def _get_date(self, json_obj):
        pass

    def _get_issn(self, json_obj):
        return set()

    def _get_orcid(self, json_obj):
        return set()

    def _call_api(self, doi_full):
        pass

    # The implementation of the following methods is strictly dependent on the actual
    # implementation of the previous three methods, since they strictly reuse them
    # for returning the result.
    def get_orcid(self, id_string):
        return self._get_item(id_string, self.orcid)

    def get_pub_date(self, id_string):
        return self._get_item(id_string, self.date)

    def get_container_issn(self, id_string):
        return self._get_item(id_string, self.issn)

    def is_valid(self, id_string):
        return self.dm.is_valid(id_string)

    def normalise(self, id_string):
        return self.dm.normalise(id_string, include_prefix=True)

    def _get_item(self, doi_entity, csv_manager):
        if self.is_valid(doi_entity):
            doi = self.normalise(doi_entity)

            if csv_manager.get_value(doi) is None:
                json_obj = self._call_api(doi)

                if json_obj is not None:
                    for issn in self._get_issn(json_obj):
                        self.issn.add_value(doi, issn)

                    if self.date.get_value(doi) is None:
                        pub_date = self._get_date(json_obj)
                        if pub_date is not None:
                            self.date.add_value(doi, pub_date)

                    for orcid in self._get_orcid(json_obj):
                        self.orcid.add_value(doi, orcid)

            return csv_manager.get_value(doi)


class ResourceFinderHandler(object):
    """This class allows one to use multiple resource finders at the same time
    so as to find the information needed for the creation of the citations to
    include in the index."""

    def __init__(self, resource_finders):
        self.resource_finders = resource_finders

    def get_date(self, id_string):
        result = None
        finders = deque(self.resource_finders)

        while result is None and finders:
            finder = finders.popleft()
            result_set = finder.get_pub_date(id_string)
            if result_set:
                result = result_set.pop()

        if result is None:  # Add the empty value in all the finders
            for finder in self.resource_finders:
                if finder.is_valid(id_string):
                    finder.date.add_value(finder.normalise(id_string), "")

        return result

    def share_issn(self, id_string_1, id_string_2):
        return self.__share_data(id_string_1, id_string_2, "get_container_issn")

    def share_orcid(self, id_string_1, id_string_2):
        return self.__share_data(id_string_1, id_string_2, "get_orcid")

    def __share_data(self, id_string_1, id_string_2, method):
        result = False
        finders = deque(self.resource_finders)
        set_1 = set()
        set_2 = set()

        while not result and finders:
            finder = finders.popleft()

            result_set_1 = getattr(finder, method)(id_string_1)
            if result_set_1:
                set_1.update(result_set_1)

            result_set_2 = getattr(finder, method)(id_string_2)
            if result_set_2:
                set_2.update(result_set_2)

            result = len(set_1.intersection(set_2)) > 0

        return result

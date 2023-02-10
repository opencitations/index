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

import importlib
from abc import ABCMeta, abstractmethod
from collections import deque

from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.utils.config import get_config


class ResourceFinder(metaclass=ABCMeta):
    """This is the abstract class that must be implemented by any resource finder
    for a particular service (Crossref, DataCite, ORCiD, etc.). It provides
    the signatures of the methods that should be implemented, and a basic
    constructor."""

    def __init__(self, data={}, use_api_service=True, id_type="doi"):
        """Resource finder constructor.

        Args:
            data (dict): support data to use prior to api.
            use_api_service (bool): true whenever you want make use of api, false otherwise.
        """
        self._data = data

        config = get_config()
        module, classname = config.get("identifier", id_type).split(":")
        self.__id_type_manager_class = getattr(
            importlib.import_module(module), classname
        )

        self._dm = self.__id_type_manager_class(data, use_api_service)
        self._im = ISSNManager()
        self._om = ORCIDManager()

        self._headers = {
            "User-Agent": "ResourceFinder / OpenCitations Indexes "
            "(http://opencitations.net; mailto:contact@opencitations.net)"
        }
        self._use_api_service = use_api_service

    @abstractmethod
    def get_orcid(self, id_string):
        """Returns the orcid associated to a specific id.

        Args:
            id_string (str): id
        """
        pass

    @abstractmethod
    def get_pub_date(self, id_string):
        """Returns the pub date associated to a specific id.

        Args:
            id_string (str): id
        """
        pass

    @abstractmethod
    def get_unified_id(self, id_string):
        """Returns the omid associated to a specific id.

        Args:
            id_string (str): id
        """
        pass

    @abstractmethod
    def get_container_issn(self, id_string):
        """It returns the container issn.

        Args:
            id_string (_type_): id
        """
        pass

    @abstractmethod
    def is_valid(self, id_string):
        """It checks if the id is valid.

        Args:
            id_string (str): id
        Returns:
            bool: True if the id is valid, false otherwise.
        """
        pass

    @abstractmethod
    def normalise(self, id_string):
        """Normalize a specific id.

        Args:
            id_string (_type_): the id to normalize
        Returns:
            str: the id normalized
        """
        pass


class ApiDOIResourceFinder(ResourceFinder, metaclass=ABCMeta):
    """This is the abstract class that must be implemented by any resource finder
    for a particular service which is based on DOI retrieving via HTTP REST APIs
    (Crossref, DataCite). It provides basic methods that are be used for
    implementing the main methods of the ResourceFinder abstract class."""

    def _get_date(self, json_obj):
        """_summary_

        Args:
            json_obj (_type_): _description_
        """
        pass

    def _get_issn(self, json_obj):
        """_summary_

        Args:
            json_obj (_type_): _description_

        Returns:
            _type_: _description_
        """
        return []

    def _get_orcid(self, json_obj):
        """_summary_

        Args:
            json_obj (_type_): _description_
        """
        return []

    def _call_api(self, doi_full):
        """_summary_

        Args:
            doi_full (_type_): _description_
        """
        pass

    # The implementation of the following methods is strictly dependent on the actual
    # implementation of the previous three methods, since they strictly reuse them
    # for returning the result.
    def get_orcid(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "orcid")
        else:
            return self._data[id_string]["orcid"]

    def get_pub_date(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "date")
        else:
            return self._data[id_string]["date"]

    def get_unified_id(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return None
        else:
            return self._data[id_string]["omid"]

    def get_container_issn(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "issn")
        else:
            return self._data[id_string]["issn"]

    def is_valid(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._dm.is_valid(id_string)
        else:
            return self._data[id_string]["valid"]

    def normalise(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        return self._dm.normalise(id_string, include_prefix=True)

    def _get_item(self, doi_entity, column):
        if self.is_valid(doi_entity):
            doi = self.normalise(doi_entity)

            if not doi in self._data:
                json_obj = self._call_api(doi)

                if json_obj is not None:
                    if column == "issn":
                        return self._get_issn(json_obj)
                    elif column == "date":
                        return self._get_date(json_obj)
                    elif column == "orcid":
                        return self._get_orcid(json_obj)
                return None

            return self._data[doi][column]


class ResourceFinderHandler(object):
    """This class allows one to use multiple resource finders at the same time
    so as to find the information needed for the creation of the citations to
    include in the index."""

    def __init__(self, resource_finders):
        """ResourceFinderHandler constructor.

        Args:
            resource_finders (iterable): resource finders to use
        """
        self.resource_finders = resource_finders

    def get_date(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        result = None
        finders = deque(self.resource_finders)

        while result is None and finders:
            finder = finders.popleft()
            result_set = finder.get_pub_date(id_string)
            if result_set:
                if isinstance(result_set, list):
                    result = result_set.pop()
                else:
                    result = result_set

        # Why?
        # if result is None:  # Add the empty value in all the finders
        #     for finder in self.resource_finders:
        #         if finder.is_valid(id_string):
        #             finder.date.add_value(finder.normalise(id_string), "")

        return result

    def get_omid(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        result = None
        finders = deque(self.resource_finders)

        while result is None and finders:
            finder = finders.popleft()
            result_set = finder.get_unified_id(id_string)
            if result_set:
                if isinstance(result_set, list):
                    result = result_set.pop()
                else:
                    result = result_set

        return result

    def share_issn(self, id_string_1, id_string_2):
        """_summary_

        Args:
            id_string_1 (_type_): _description_
            id_string_2 (_type_): _description_

        Returns:
            _type_: _description_
        """
        return self.__share_data(id_string_1, id_string_2, "get_container_issn")

    def share_orcid(self, id_string_1, id_string_2):
        """_summary_

        Args:
            id_string_1 (_type_): _description_
            id_string_2 (_type_): _description_

        Returns:
            _type_: _description_
        """
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

        return result, set_1, set_2

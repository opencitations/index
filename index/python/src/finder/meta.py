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

from oc.index.finder.base import ResourceFinder
import importlib
from abc import ABCMeta, abstractmethod
import requests

from oc.index.identifier.issn import ISSNManager
from oc.index.identifier.orcid import ORCIDManager
from oc.index.utils.config import get_config


class MetaFinder(ResourceFinder):
    """This class is used to query OC Meta's API."""

    def __init__(self, data={}, use_api_service=True, id_type="metaid"):
        """
        Args:
            data (dict): support data to use prior to api.
            use_api_service (bool): true whenever you want make use of api, false otherwise.
        """
        super().__init__(data, use_api_service)

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
        self._api = "https://127.0.0.1:5000/api/v1/metadata/"

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
        """Normalize a specific id.

        Args:
            id_string (_type_): the id to normalize
        Returns:
            str: the id normalized

        """
        return self._dm.normalise(id_string, include_prefix=True)

    def _get_item(self, meta_entity, column):
        if self.is_valid(meta_entity):
            metaid = self.normalise(meta_entity)

            if not metaid in self._data:
                json_obj = self._call_api(metaid)

                if json_obj is not None:
                    if column == "issn":
                        return self._get_issn(json_obj)
                    elif column == "date":
                        return self._get_date(json_obj)
                    elif column == "orcid":
                        return self._get_orcid(json_obj)
                return None

            return self._data[metaid][column]

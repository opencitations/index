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

import json
from os.path import exists, basename, isdir, join, isfile
from oc.index.utils.config import get_config
from oc.index.glob.datasource import DataSource
from oc.index.legacy.csv import CSVManager

import json


class CSVDataSource(DataSource):
    def __init__(self, service):
        super(CSVDataSource, self).__init__()

        self._valid_id = CSVManager(csv_path=get_config().get(service, "valid_id"))
        self._id_date = CSVManager(csv_path=get_config().get(service, "id_date"))
        self._id_orcid = CSVManager(csv_path=get_config().get(service, "id_orcid"))
        self._id_issn = CSVManager(csv_path=get_config().get(service, "id_issn"))

    def get(self, resource_id):
        entry = self.new()
        entry["valid"] = self._valid_id.get_value(resource_id)
        entry["date"] = self._id_date.get_value(resource_id)
        entry["issn"] = self._id_issn.get_value(resource_id)
        entry["orcid"] = self._id_orcid.get_value(resource_id)
        if (
            entry["valid"] == None
            and entry["date"] == None
            and entry["issn"] == None
            and entry["orcid"] == None
        ):
            return None
        return entry

    def mget(self, resources_id):
        return {key: self.get(key) for key in resources_id}

    def set(self, resource_id, value):
        # if the value dict was compiled for the first time, the value will be True/False
        # while if the value dict is being updated and the validity information was retrieved
        # from csv files, the value retrieved for the validity will be a set with a unique
        # value, either "i" or "v"
        if "valid" in value.keys():
            if value["valid"] is False or value["valid"] == {"i"}:
                self._valid_id.add_value(resource_id, "i")
            elif value["valid"] is True or value["valid"] == {"v"}:
                self._valid_id.add_value(resource_id, "v")
                # so that all the operations and transcriptions are performed only for valid ids

                if "date" in value.keys():
                    if value["date"] is not None and len(value["date"])>0:
                        # for multiple values and to avoid self.data[id_string].add(value) TypeError: unhashable type: 'set'
                        for date in value["date"]:
                            self._id_date.add_value(resource_id, date)
                    else:
                        self._id_date.add_value(resource_id, "")
                else:
                    self._id_date.add_value(resource_id, "")

                if "issn" in value.keys():
                    if value["issn"] is not None and len(value["issn"])>0:
                        # for multiple values and to avoid self.data[id_string].add(value) TypeError: unhashable type: 'set'
                        for issn in value["issn"]:
                            self._id_issn.add_value(resource_id, issn)

                if "orcid" in value.keys():
                    if value["orcid"] is not None and len(value["orcid"])>0:
                        # for multiple values and to avoid self.data[id_string].add(value) TypeError: unhashable type: 'set'
                        for orcid in value["orcid"]:
                            self._id_orcid.add_value(resource_id, orcid)

    def mset(self, resources):
        for key in resources.keys():
            self.set(key, resources[key])
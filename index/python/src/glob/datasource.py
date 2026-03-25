#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import redis
import json

from oc.index.utils.config import get_config

from abc import ABCMeta, abstractmethod


class DataSource(metaclass=ABCMeta):
    def __init__(self, service):
        self._service = service

    def new(self):
        return {"date": None, "valid": False, "issn": [], "orcid": []}

    @abstractmethod
    def get(self, resource_id):
        pass

    @abstractmethod
    def mget(self, resources_id):
        pass

    @abstractmethod
    def set(self, resource_id, value):
        pass

    @abstractmethod
    def mset(self, resources):
        pass

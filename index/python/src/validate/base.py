#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import os
import importlib

from abc import ABCMeta, abstractmethod

from oc.index.oci.citation import OCIManager
from oc.index.utils.config import get_config

from os.path import join
from csv import DictReader


class CitationValidator(metaclass=ABCMeta):
    def __init__(self, service):
        self._config = get_config()
        self._oci_manager = OCIManager(
            lookup_file=os.path.expanduser(self._config.get("cnc", "lookup"))
        )
        self._service = service
        self._prefix = self._config.get(self._service, "prefix")

    @abstractmethod
    def build_oci_query(self, input_file, result_map, disable_tqdm=False):
        pass

    @abstractmethod
    def validate_citations(self, input_directory, result_map, output_directory):
        pass

    @staticmethod
    def get_validator(service):
        # Initialize the parser
        config = get_config()
        module, classname = config.get(service, "validator").split(":")
        return getattr(importlib.import_module(module), classname)(service)
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
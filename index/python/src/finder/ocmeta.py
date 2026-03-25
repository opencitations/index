#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

from requests import get, ConnectionError
import requests
from datetime import datetime
import time

from urllib.parse import quote

import oc.index.utils.dictionary as dict_utils
from oc.index.finder.base import IndexResourceFinder


class OCMetaResourceFinder(IndexResourceFinder):
    """This class implements an api doi resource finder for crossref"""

    def __init__(self, data={}, use_api_service=False, identifier="omid"):
        """Crossref resource finder constructor."""
        super().__init__(data, use_api_service, identifier)

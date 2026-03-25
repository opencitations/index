#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import configparser
import threading

from os.path import expanduser, join

_config = None
_config_lock = threading.Lock()


def _load_config():
    global _config
    _config = configparser.ConfigParser()
    _config.read(expanduser(join("~", ".opencitations", "index", "config.ini")))


def get_config():
    """It returns ocindex configuration."""
    global _config

    if not _config:
        _config_lock.acquire()
        _load_config()
        _config_lock.release()

    return _config

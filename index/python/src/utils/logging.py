#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021-2022 Giuseppe Grieco <g.grieco1997@gmail.com>
#
# SPDX-License-Identifier: ISC

import logging

from datetime import datetime
from os.path import expanduser, join
import multiprocessing
import threading

from oc.index.utils.config import get_config

_logger = None
_logger_lock = threading.Lock()


def _setup_logger():
    global _logger
    _logger = logging.getLogger("opencitations.cnc")
    _logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(processName)s:%(threadName)s] %(asctime)s | %(levelname)s | oc.index : %(message)s"
    )
    fileHandler = logging.FileHandler(
        expanduser(
            join(
                "~",
                ".opencitations",
                "index",
                "logs",
                datetime.now().strftime("%m-%d-%Y_%H-%M-%S") + ".log",
            )
        )
    )
    fileHandler.setFormatter(formatter)
    _logger.addHandler(fileHandler)

    if get_config().get("logging", "verbose"):
        streamHandler = logging.StreamHandler()
        streamHandler.setFormatter(formatter)
        _logger.addHandler(streamHandler)


def get_logger():
    """It returns ocindex logger instance."""
    global _logger
    global _logger_lock

    if not _logger:
        _logger_lock.acquire()
        _setup_logger()
        _logger_lock.release()

    return _logger

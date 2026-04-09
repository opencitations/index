#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021, 2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021, 2022 Giuseppe Grieco <g.grieco1997@gmail.com>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import logging
import os

from datetime import datetime

from oc.index.utils.config import get_config

_state: dict[str, logging.Logger] = {}


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("opencitations.cnc")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "[%(processName)s:%(threadName)s] %(asctime)s | %(levelname)s | oc.index : %(message)s"
    )
    config = get_config()
    logdir = os.path.expanduser(config.get("logging", "logdir"))
    os.makedirs(logdir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(logdir, datetime.now().strftime("%m-%d-%Y_%H-%M-%S") + ".log")
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if config.get("logging", "verbose"):
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


def reset_logger() -> None:
    _state.clear()


def get_logger() -> logging.Logger:
    if "logger" not in _state:
        _state["logger"] = _setup_logger()
    return _state["logger"]

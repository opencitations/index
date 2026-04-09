#!python

# SPDX-FileCopyrightText: 2019-2022 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021, 2022 Arianna Moretti <arianna.moretti2@studio.unibo.it>
# SPDX-FileCopyrightText: 2021, 2022 Giuseppe Grieco <g.grieco1997@gmail.com>
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import configparser

_state: dict[str, configparser.ConfigParser] = {}


def get_config(path: str | None = None) -> configparser.ConfigParser:
    if "config" not in _state:
        if path is None:
            raise FileNotFoundError(
                "No config file found. Pass the path via --config."
            )
        config = configparser.ConfigParser()
        config.read(path)
        _state["config"] = config
    return _state["config"]


def reset_config() -> None:
    _state.clear()

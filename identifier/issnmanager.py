#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2019, Silvio Peroni <essepuntato@gmail.com>
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


from index.identifier.identifiermanager import IdentifierManager
from re import sub, match


class ISSNManager(IdentifierManager):
    def __init__(self):
        self.p = "issn:"
        super(ISSNManager, self).__init__()

    def is_valid(self, id_string):
        issn = self.normalise(id_string)
        return issn is not None and match("^[0-9]{4}-[0-9]{3}[0-9X]$", issn) and ISSNManager.__check_digit(issn)

    def normalise(self, id_string, include_prefix=False):
        try:
            issn_string = sub("[^X0-9]", "", id_string.upper())
            return "%s%s-%s" % (self.p if include_prefix else "", issn_string[:4], issn_string[4:8])
        except:  # Any error in processing the ISSN will return None
            return None

    @staticmethod
    def __check_digit(issn):
        result_partial_sum = 0
        for i, n in zip(range(8, 1, -1), issn[:4] + issn[5:8]):
            result_partial_sum += i * int(n)
        reminder = result_partial_sum % 11
        reminder_sub = 11 - reminder
        correct_check_digit = \
            str(reminder_sub) == issn[8] or \
            (reminder == 0 and issn[8] == "0") or \
            (reminder_sub == 10 and issn[8] == "X")

        result_full_sum = 0
        for i, n in zip(range(8, 0, -1), issn[:4] + issn[5:]):
            result_full_sum += i * (10 if n == "X" else int(n))
        confirm_check_digit = result_full_sum % 11 == 0

        return correct_check_digit and confirm_check_digit

from oc.index.identifier.base import IdentifierManager
from re import sub


class ISBNManager(IdentifierManager):
    def __init__(self, data={}):
        super(ISBNManager, self).__init__()
        self.p = "isbn:"
        self._data = data

    def is_valid(self, id_string):
        isbn = self.normalise(id_string, with_hyphens=False)
        if isbn is None:
            return False
        elif not isbn in self._data:
            self._data[isbn] = ISBNManager.__check_digit(isbn)
        return self._data[isbn]

    def normalise(self, id_string, include_hyphens=True, include_prefix=False):
        if include_hyphens:
            try:
                isbn_string = sub("[^X0-9|-]", "", id_string.upper())
                return "%s%s" % (self.p if include_prefix else "", isbn_string)
            except:  # Any error in processing the ISBN will return None
                return None
        else:
            try:
                isbn_string = sub("[^X0-9]", "", id_string.upper())
                return "%s%s" % (self.p if include_prefix else "", isbn_string)
            except:  # Any error in processing the ISBN will return None
                return None

    @staticmethod
    def __check_digit(isbn):
        check_digit = False
        if len(isbn) == 13:
            total = 0
            val = 1
            for x in isbn:
                if x == "X":
                    x = 10
                total += int(x) * val
                val = 3 if val == 1 else val == 1
            if (total % 10) == 0:
                check_digit = True
        elif len(isbn) == 10:
            total = 0
            val = 10
            for x in isbn:
                if x == "X":
                    x = 10
                total += int(x) * val
                val -= 1
            if (total % 11) == 0:
                check_digit = True
        return check_digit

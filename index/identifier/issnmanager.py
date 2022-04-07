from index.identifier.identifiermanager import IdentifierManager
from re import sub, match


class ISSNManager(IdentifierManager):
    """This class implements an identifier manager for issn identifier"""

    def __init__(self):
        """ISSN manager constructor."""
        self.p = "issn:"
        super(ISSNManager, self).__init__()

    def is_valid(self, id_string):
        """It returns the validity of an issn.

        Args:
            id_string (str): the issn to validate

        Returns:
            bool: true if the issn is valid, false otherwise.
        """
        issn = self.normalise(id_string)
        return (
            issn is not None
            and match("^[0-9]{4}-[0-9]{3}[0-9X]$", issn)
            and ISSNManager.__check_digit(issn)
        )

    def normalise(self, id_string, include_prefix=False):
        """It normalizes the ISSN.

        Args:
            id_string (str): the issn to normalize.
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.

        Returns:
            str: the normalized issn
        """
        try:
            issn_string = sub("[^X0-9]", "", id_string.upper())
            return "%s%s-%s" % (
                self.p if include_prefix else "",
                issn_string[:4],
                issn_string[4:8],
            )
        except:  # Any error in processing the ISSN will return None
            return None

    @staticmethod
    def __check_digit(issn):
        """Returns True, if ISSN (of length 8 or 9) is valid (this does not mean registered).

        Args:
            issn (str): the issn to check

        Raises:
            ValueError: if the len of issn is not 8 or 9

        Returns:
            bool: true if issn is valid
        """
        issn = issn.replace("-", "")
        if len(issn) != 8:
            raise ValueError("ISSN of len 8 or 9 required (e.g. 00000949 or 0000-0949)")
        ss = sum([int(digit) * f for digit, f in zip(issn, range(8, 1, -1))])
        _, mod = divmod(ss, 11)
        checkdigit = 0 if mod == 0 else 11 - mod
        if checkdigit == 10:
            checkdigit = "X"
        return "{}".format(checkdigit) == issn[7]

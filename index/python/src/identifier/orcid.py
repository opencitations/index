from re import sub, match

from oc.index.identifier.base import IdentifierManager


class ORCIDManager(IdentifierManager):
    """This class implements an identifier manager for orcid identifier."""

    def __init__(self):
        """Orcid Manager constructor."""
        self._p = "orcid:"
        super(ORCIDManager, self).__init__()

    def is_valid(self, id_string):
        """Returns true if the orcid indicated is valid, false otherwise.

        Args:
            id_string (str): the orcid associated to check.

        Returns:
            bool: True if the orcid is valid, false otherwise.
        """
        orcid = self.normalise(id_string)
        return (
            orcid is not None
            and match("^([0-9]{4}-){3}[0-9]{3}[0-9X]$", orcid)
            and ORCIDManager.__check_digit(orcid)
        )

    def normalise(self, id_string, include_prefix=False):
        """It normalize the orcid.

        Args:
            id_string (str): the orcid to normalize
            include_prefix (bool, optional): indicates if includes the prefix. Defaults to False.

        Returns:
            str: normalized orcid
        """
        try:
            orcid_string = sub("[^X0-9]", "", id_string.upper())
            return "%s%s-%s-%s-%s" % (
                self._p if include_prefix else "",
                orcid_string[:4],
                orcid_string[4:8],
                orcid_string[8:12],
                orcid_string[12:16],
            )
        except:  # Any error in processing the ISSN will return None
            return None

    @staticmethod
    def __check_digit(orcid):
        total = 0
        for d in sub("[^X0-9]", "", orcid.upper())[:-1]:
            i = 10 if d == "X" else int(d)
            total = (total + i) * 2
        reminder = total % 11
        result = (12 - reminder) % 11
        return (str(result) == orcid[-1]) or (result == 10 and orcid[-1] == "X")

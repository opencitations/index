from abc import ABCMeta, abstractmethod


class IdentifierManager(metaclass=ABCMeta):
    """This is the interface that must be implemented by any identifier manager
    for a particular identifier scheme. It provides the signatures of the methods
    for checking the validity of an identifier and for normalising it."""

    def __init__(self, **params):
        """Identifier manager constructor."""
        for key in params:
            setattr(self, key, params[key])

        self._headers = {
            "User-Agent": "Identifier Manager / OpenCitations Indexes "
            "(http://opencitations.net; mailto:contact@opencitations.net)"
        }

    @abstractmethod
    def is_valid(self, id_string):
        """Returns true if the id is valid, false otherwise.

        Args:
            id_string (str): id to check
        Returns:
            bool: True if the id is valid, false otherwise.
        """
        pass

    @abstractmethod
    def normalise(self, id_string, include_prefix=False):
        """Returns the id normalized.

        Args:
            id_string (str): the id to normalize
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.
        Returns:
            str: normalized id
        """
        pass

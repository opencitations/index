from abc import ABCMeta, abstractmethod
from os import strerror
from os.path import exists, isfile
from errno import ENOENT


class CitationParser(metaclass=ABCMeta):
    """This class defines the methods required to implement to
    parse a citation data file.
    """

    def __init__(self):
        self._current_item = 0
        self._items = 0

    @abstractmethod
    def is_valid(self, filename: str):
        """It checks if a specific file is valid and so it can to be parse.

        Args:
            file (str): path to the file to check
        """
        if not exists(filename) or not isfile(filename):
            raise FileNotFoundError(ENOENT, strerror(ENOENT), filename)

    @abstractmethod
    def parse(self, filename: str):
        """It updates the file on which the parser is working on.

        Args:
            filename (str): path to the new file
        """
        self._current_item = 0
        self._items = 0

    @property
    def items(self):
        """It returns the number of items to parse."""
        return self._items

    @property
    def current_item(self):
        """It returns the index of the current element."""
        return self._current_item

    @abstractmethod
    def get_next_citation_data(self):
        """This method returns the next citation data available in the file specified.
        The citation data returned is a tuple of six elements: citing id (string), cited id (string),
        citing date (string, or None if unknown), cited date (string or None
        if unknown), if it is a journal self-citation (True = yes, False = no, None = do
        not know), and if it is an author self-citation (True = yes, False = no, None = do
        not know). If no more citation data are available, it returns None.

        Returns:
            tuple: the next citation data available in the source specified
        """
        pass

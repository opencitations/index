from abc import ABCMeta, abstractmethod

from index.citation.data import CitationData


class CitationParser(metaclass=ABCMeta):
    """This class defines the methods required to implement to
    parse a citation data file.
    """

    @abstractmethod
    def is_valid(self, file):
        """It checks if a specific file is valid and so it can to be parse.

        Args:
            file (str): path to the file to check
        """
        pass

    @abstractmethod
    def set_input_file(self, file, targz_fd=None):
        """It updates the file on which the parser is working on.

        Args:
            file (str): path to the new file
            targz_fd (targz_fd, optional): this parameter if set is intended as the
            file descriptor of the targz archive containing the file.
        """
        pass

    @abstractmethod
    def get_next_citation_data(self) -> CitationData:
        """This method returns the next citation data available in the file specified.
        The citation data returned is a tuple of six elements: citing id (string), citing
        date (string, or None if unknown), cited id (string), cited date (string or None
        if unknown), if it is a journal self-citation (True = yes, False = no, None = do
        not know), and if it is an author self-citation (True = yes, False = no, None = do
        not know). If no more citation data are available, it returns None.

        Returns:
            CitationData: the next citation data available in the source specified
        """
        pass

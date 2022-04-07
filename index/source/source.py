from os.path import isdir, exists, dirname
from os import walk, sep, remove
from index.citation.data import CitationData

from index.parser.parser import CitationParser


class CitationSource(object):
    """This class enables reading from any source for which an appropriate
    parser has been developed. In practice, it provides basic methods that
    must be used to get the basic information about citations, in particular
    the citing id and the cited id.
    """

    def __init__(self, src, parser: CitationParser, logger):
        """The constructor allows to associate to the variable 'src' a kind of source,
        that will be used to retrieve citation data.

        Args:
            src (str): path to the data to be used to extract citations. Possible values
            are direct paths to files or directories containing such files.
            parser (CitationParser): the parser to use to read data from the source
            logger (logging.Logger): logger to use to log info and problems.
        """
        self.__parser = parser
        self.__targz_fd = None
        self.__logger = logger

        if isinstance(src, (list, set, tuple)):
            src = src
        else:
            src = [src]

        self.__files = []

        for dir in sorted(src):
            if isdir(dir):
                for cur_dir, _, cur_files in walk(dir):
                    for cur_file in cur_files:
                        full_path = cur_dir + sep + cur_file
                        if self.__parser.is_valid(full_path):
                            self.__files.append(full_path)
            elif self.__parser.is_valid(dir):
                self.__files.append(dir)

            self.__files.sort()

    def __get_next_file(self):
        """It returns the next file unit to process.

        Returns:
            str: the next file unit to process.
        """
        return self.__files.pop()

    def get_next_citation_data(self) -> CitationData:
        """This method returns the next citation data available in the source specified.
        The citation data returned is a tuple of six elements: citing id (string), citing
        date (string, or None if unknown), cited id (string), cited date (string or None
        if unknown), if it is a journal self-citation (True = yes, False = no, None = do
        not know), and if it is an author self-citation (True = yes, False = no, None = do
        not know). If no more citation data are available, it returns None.

        Returns:
            CitationData: the next citation data available in the source specified
        """
        value = self.__parser.get_next_citation_data()

        if value is None:
            self.__current_file = self.__get_next_file()
            if not self.__current_file is None and self.__parser.is_valid(
                self.__current_file
            ):
                self.__logger.info("Opening file " + self.__current_file)
                self.__parser.set_input_file(self.__current_file, self.__targz_fd)
                value = self.__parser.get_next_citation_data()

        return value

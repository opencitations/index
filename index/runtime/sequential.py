from argparse import ArgumentParser
from index.operator.drivers.file import FileDriver

from index.operator.operator import CitationOperator
from index.runtime.base import RuntimeBase
from index.source.source import CitationSource
from index.storer.citationstorer import CitationStorer


class Sequential(RuntimeBase):
    """This class implements the sequential runtime for citation extraction."""

    def __init__(self):
        """Sequential Runtime constructor."""
        super().__init__("Sequential")
        self._driver = FileDriver()

    def set_args(self, arg_parser: ArgumentParser, config_file):
        """_summary_

        Args:
            arg_parser (ArgumentParser): _description_
            config_file (_type_): _description_
        """
        super().set_args(arg_parser, config_file)
        self._driver.set_args(arg_parser, config_file)

    def init(self, args, config_file):
        """It initialize the driver, the citation source and the citation operator
        that are three objects required for running the sequential runtime.

        Args:
            args (dict): parameter values for execution
            config_file (dict): index configuration file values map
        """
        super().init(args, config_file)
        self._driver.init(
            args.data,
            args.doi_file,
            args.date_file,
            args.orcid_file,
            args.issn_file,
            args.orcid,
            args.no_api,
        )
        self.logger.info("File driver initialized.")

        self.__source = CitationSource(args.input, self._parser, self.logger)
        self.__op = CitationOperator(
            self._driver,
            args.baseurl,
            args.agent,
            args.source,
            args.service,
            args.lookup,
            args.prefix,
        )
        self.__storer = CitationStorer(
            args.data,
            args.baseurl + "/" if not args.baseurl.endswith("/") else args.baseurl,
        )

    def run(self, _, __):
        """It start the sequential process of extracting citation data.

        Args:
            _ (dict): parameter values for execution
            __ (dict): index configuration file values map
        """
        citations_added = 0
        citation_raw = self.__source.get_next_citation_data()
        while citation_raw != None:
            if not self.__op.exists(citation_raw):
                citation = self.__op.process(citation_raw)

                # Save only valid citation
                if not citation is None:

                    # Save the citation
                    try:
                        self.__storer.store_citation(citation)
                    except Exception:
                        self.logger.exception("Could not save citation")
                    else:
                        citations_added += 1

            citation_raw = self.__source.get_next_citation_data()

        self.logger.info("Citation added " + str(citations_added))
        self.logger.info(
            "Citation already present " + str(self.__op.citations_already_present)
        )
        self.logger.info(
            "Errors in ids existence " + str(self.__op.error_in_ids_existence)
        )

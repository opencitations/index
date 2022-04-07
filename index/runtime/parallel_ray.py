from argparse import ArgumentParser
import argparse

import ray


from index.operator.drivers.file import FileDriver
from index.operator.operator import CitationOperator
from index.runtime.base import RuntimeBase
from index.source.source import CitationSource
from index.storer.citationstorer import CitationStorer


@ray.remote
class ParallelCitationSource(CitationSource):
    """This class makes the CitationSource class as a remote object"""

    pass


@ray.remote
class ParallelCitationOperator(CitationOperator):
    """This class makes the CitationOperator class as a remote object"""

    pass


@ray.remote
def parallel_extract_citations(logger, psource, poperator, args, process_id):
    """_summary_

    Args:
        logger (_type_): _description_
        psource (_type_): _description_
        poperator (_type_): _description_
        args (_type_): _description_
        process_id (_type_): _description_

    Returns:
        _type_: _description_
    """
    storer = CitationStorer(
        args.data,
        args.baseurl + "/" if not args.baseurl.endswith("/") else args.baseurl,
        suffix=process_id,
    )

    citations_added = 0
    citation_raw = ray.get(psource.get_next_citation_data.remote())

    while citation_raw != None:
        if not ray.get(poperator.exists.remote(citation_raw)):
            citation = ray.get(poperator.process.remote(citation_raw))

            # Save only valid citation
            if not citation is None:

                # Save the citation
                try:
                    storer.store_citation(citation)
                except Exception:
                    logger.exception("Process `{process_id}` could not save a citation")
                else:
                    citations_added += 1

        citation_raw = ray.get(psource.get_next_citation_data.remote())

    return citations_added


class ParallelRay(RuntimeBase):
    """
    This execution mode allows the execution of cnc in parallel
    using ray.
    """

    def __init__(self):
        """Parallel Farm constructor."""
        super().__init__("Parallel Ray")
        self._driver = FileDriver()
        self.__psource = None
        self.__poperator = None

    def __pn_type(self, arg):
        try:
            i = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Must be an integer number")
        if i < 2:
            raise argparse.ArgumentTypeError(
                "Process number must be an integer greater than 2"
            )
        return i

    def set_args(self, arg_parser: ArgumentParser, config_file):
        """It adds the following arguments to the arguments parser:
        - pn: number of process to use
        - file arguments see FileDriver.set_args() for info

        Args:
            arg_parser (ArgumentParser): argument parser to set additional arguments
            config_file (dict): index configuration file values map
        """
        super().set_args(arg_parser, config_file)
        self._driver.set_args(arg_parser, config_file)
        arg_parser.add_argument(
            "-pn",
            "--process_number",
            type=self.__pn_type,
            help="The number of parallel process to run for working on the creation of citations.",
        )

    def init(self, args, config_file):
        """It initialize the driver, the parallel citation source and
        the parallel citation operator to use in order to extract citation
        in parallel using ray.

        Args:
            args (_type_): _description_
            config_file (_type_): _description_
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

        ray.init(num_cpus=args.process_number)
        self.logger.info("Ray initialized")

        self.__psource = ParallelCitationSource.remote(
            args.input, self._parser, self.logger
        )
        self.__poperator = ParallelCitationOperator.remote(
            self._driver,
            args.baseurl,
            args.agent,
            args.source,
            args.service,
            args.lookup,
            args.prefix,
        )
        self.logger.info("Remote objects initialized")

    def run(self, args, _):
        """It start a parallel process using ray (#args.process_number of process) that
        extract citations from the source.

        Args:
            args (dict): parameter values for execution
            _ (dict): index configuration file values map
        """
        self.logger.info("Running on #" + str(args.process_number) + " process")
        futures = [
            parallel_extract_citations.remote(
                self.logger,
                self.__psource,
                self.__poperator,
                args,
                str(i),
            )
            for i in range(args.process_number - 1)
        ]
        citations_added = 0
        for value in ray.get(futures):
            citations_added += value

        self.logger.info("Citation added " + str(citations_added))
        self.logger.info(
            "Citation already present "
            + str(ray.get(self.__poperator.citations_already_present.remote()))
        )
        self.logger.info(
            "Errors in ids existence "
            + str(ray.get(self.__poperator.error_in_ids_existence.remote()))
        )

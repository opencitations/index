import importlib
import os
import logging

from argparse import ArgumentParser
from datetime import datetime
from abc import ABCMeta

from index.runtime.runtime import Runtime


class RuntimeBase(Runtime, metaclass=ABCMeta):
    """It represent a possible runtime mode to execute index."""

    def __init__(self, name="RuntimeBase"):
        """Base Runtime constructor.

        Args:
            name (str, optional): defaults to "RuntimeBase" and its the name of the runtime
        """
        self._name = name

    def set_args(self, arg_parser: ArgumentParser, config_file):
        """It add all the arguments in common amongs all the runtimes.
        Next all the runtime that inherit this methods 'll add their
        specific arguments.

        Args:
            arg_parser (ArgumentParser): argument parser to set additional arguments
            config_file (dict): index configuration file values map
        """
        arg_parser.add_argument(
            "-d",
            "--data",
            required=True,
            help="The directory containing all the CSV files already added in the Index, "
            "including data and provenance files.",
        )
        arg_parser.add_argument(
            "-i",
            "--input",
            required=True,
            help="The input file/directory to provide as input "
            "of the specified input Python file (using -p).",
        )
        arg_parser.add_argument(
            "-o",
            "--orcid",
            default=config_file["base"]["orcid"],
            help="ORCID API key to be used to query the ORCID API.",
        )
        arg_parser.add_argument(
            "-l",
            "--lookup",
            default=config_file["base"]["lookup"],
            help="The lookup table that must be used to produce OCIs.",
        )
        arg_parser.add_argument(
            "-b",
            "--baseurl",
            required=True,
            default="",
            help="The base URL of the dataset",
        )
        arg_parser.add_argument(
            "-ib",
            "--idbaseurl",
            default=config_file["base"]["idbaseurl"],
            help="The base URL of the identifier of "
            "citing and cited entities, if any",
        )
        arg_parser.add_argument(
            "-px",
            "--prefix",
            default="",
            help="The '0xxx0' prefix to use for creating the OCIs.",
        )
        arg_parser.add_argument(
            "-a",
            "--agent",
            default=config_file["base"]["agent"],
            help="The URL of the agent providing or processing the citation data.",
        )
        arg_parser.add_argument(
            "-s",
            "--source",
            required=True,
            help="The URL of the source from where the citation data have been extracted.",
        )
        arg_parser.add_argument(
            "-sv",
            "--service",
            required=True,
            help="The name of the service that will made available the citation data.",
        )
        arg_parser.add_argument(
            "-na",
            "--no_api",
            action="store_true",
            default=config_file["base"]["no_api"],
            help="Tell the tool explicitly not to use the APIs of the various finders.",
        )
        arg_parser.add_argument(
            "-pa",
            "--parser",
            required=True,
            help="It tells to the cnc which parser to use for extracting citation.",
            choices=config_file["parser"].keys(),
        )

    def init(self, args, config_file):
        """It initializes the runtime logger and process some base arguments.

        Args:
            args (dict): parameter values for execution
            config_file (dict): index configuration file values map
        """
        self.logger = logging.getLogger(self._name)
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        fileHandler = logging.FileHandler(
            os.path.expanduser(
                "~/.opencitations/index/logs/"
                + datetime.now().strftime("%m-%d-%Y_%H-%M-%S")
                + ".log"
            )
        )
        fileHandler.setFormatter(formatter)
        self.logger.addHandler(fileHandler)

        if args.verbose:
            streamHandler = logging.StreamHandler()
            streamHandler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] | %(levelname)s | %(name)s | %(message)s"
                )
            )
            self.logger.addHandler(streamHandler)

        parser = config_file["parser"][args.parser].split(":")

        # Import and initialize the parser chosen
        parser_module = importlib.import_module(parser[0])
        parser_class = getattr(parser_module, parser[1])
        self._parser = parser_class()

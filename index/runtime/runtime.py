import abc

from argparse import ArgumentParser
from abc import ABCMeta


class Runtime(metaclass=ABCMeta):
    """A runtime is a class that specifies a cnc execution mode by specifying
    the arguments and the run method that includes the execution code.
    """

    def __init__(self, name="Runtime"):
        """Runtime constructor.

        Args:
            name (str, optional): Defaults to "Runtime", it is the name for the runtime.
        """
        self._name = name

    @abc.abstractmethod
    def set_args(self, arg_parser: ArgumentParser, config_file):
        """It add all the arguments needed by the runtime.

        Args:
            arg_parser (ArgumentParser): argument parser to set additional arguments
            config_file (dict): index configuration file values map
        """
        pass

    def init(self, args, config_file):
        """It initializes the runtime.

        Args:
            args (dict): runtime arguments
            config_file (dict): index configuration file values map
        """
        pass

    @abc.abstractmethod
    def run(self, args, config_file):
        """It implements the process of creating new citations.

        Args:
            args (dict): runtime arguments
            config_file (dict): index configuration file values map
        """
        pass

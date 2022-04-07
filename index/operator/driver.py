from abc import ABC, abstractmethod
from argparse import ArgumentParser


class OperatorDriver(ABC):
    @abstractmethod
    def set_args(self, arg_parser: ArgumentParser, config_file):
        """This method allows one to specify all the arguments needed for a specific
        drivers.

        Args:
            arg_parser (ArgumentParser): argument parser to set additional arguments
            config_file (dict): index configuration file values map
        """
        pass

    @abstractmethod
    def init(self, *params):
        """This method allows one to initialise all internal variable to make
        the data handler works corretly. It must be implemented in each particular
        subclass.

        Args:
            params (dict): initialization parameters
        """
        pass

    @abstractmethod
    def are_valid(self, citing, cited):
        """This method checks if the identifiers of the citing and cited entities
        are both valid (it returns True in this case, otherwise it returns False).

        Args:
            citing (str): citing id
            cited (str): cited id
        Returns:
            bool: true if both citing and cited are valid, false otherwise.
        """
        pass

    @abstractmethod
    def share_orcid(self, citing, cited):
        """This method checks if the citing and cited entities share at least
        one ORCID (it returns True in this case, otherwise it returns False).

        Args:
            citing (str): citing id
            cited (str): cited id
        Returns:
            bool: true if the citing and cited share at least one ORCID.
        """
        pass

    @abstractmethod
    def share_issn(self, citing, cited):
        """This method checks if the citing and cited entities share at least
        one ISSN (it returns True in this case, otherwise it returns False).

        Args:
            citing (str): citing id
            cited (str): cited id
        Returns:
            bool: true if the citing and cited share at least one ISSN.
        """
        pass

    @abstractmethod
    def get_date(self, id_string):
        """This method retrives the date of publication of the entity indetified by
        the input string.

        Args:
            id_string (str): entity id
        Returns:
            str: the date of publication of the entity
        """
        pass

    @abstractmethod
    def oci_exists(self, oci):
        """This method checks if the OCI in input has been already added to a
        database (it returns True in this case, otherwise it returns False).

        Args:
            oci (str): citation identifier
        Returns:
            bool: true if the citation exists, false otherwise.
        """
        pass

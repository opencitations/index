from datetime import datetime

from index.citation.oci import Citation, OCIManager
from index.citation.data import CitationData
from urllib.parse import quote

from index.operator.driver import OperatorDriver


class CitationOperator(object):
    """This class encapsulates all operations performed on the extracted citation data."""

    def __init__(
        self,
        driver: OperatorDriver,
        base_url,
        agent,
        source,
        service,
        lookup,
        prefix="",
    ):
        """Citation Operator constructor.

        Args:
            driver (OperatorDriver): the operator driver to use to perform the operations.
            base_url (str): the base URL of the dataset.
            agent (str): the URL of the agent providing or processing the citation data.
            source (str): the URL of the source from where the citation data have been extracted.
            service (str): the name of the service that will made available the citation data.
            lookup (str): the lookup table that must be used to produce OCIs.
            prefix (str, optional): prefix to use, defaults to "".
        """
        self.__driver = driver
        self.__base_url = base_url
        self.__agent = agent
        self.__source = source
        self.__service = service
        self.__prefix = prefix

        self.citations_already_present = 0
        self.error_in_ids_existence = 0

        self.__oci_manager = OCIManager(lookup_file=lookup)

    def exists(self, raw_citation: CitationData) -> bool:
        """It checks if a citation exists or not by looking at its
        oci. Note if it does not exists than it is created,

        Args:
            raw_citation (CitationData): citation data extracted

        Returns:
            bool: true if the citation exists, false otherwise.
        """
        oci = self.__oci_manager.get_oci(
            raw_citation.citing, raw_citation.cited, self.__prefix
        )
        oci_noprefix = oci.replace("oci:", "")

        result = self.__driver.oci_exists(oci_noprefix)
        if result:
            self.citations_already_present += 1
        return result

    def process(self, raw_citation: CitationData) -> Citation:
        """It process a raw citation validating it and reading all the information
        available and returns a Citation object built on this information.

        Args:
            raw_citation (CitationData): citation data extracted

        Returns:
            Citation: returns the citation constructed from the extracted data.
        """
        oci = self.__oci_manager.get_oci(
            raw_citation.citing, raw_citation.cited, self.__prefix
        )

        if self.__driver.are_valid(raw_citation.citing, raw_citation.cited):

            if raw_citation.created is None:
                citing_date = self.__driver.get_date(raw_citation.citing)
            else:
                citing_date = raw_citation.created

            cited_date = self.__driver.get_date(raw_citation.cited)

            if (
                raw_citation.journal_sc is None
                or type(raw_citation.journal_sc) is not bool
            ):
                journal_sc = self.__driver.share_issn(
                    raw_citation.citing, raw_citation.cited
                )

            if (
                raw_citation.author_sc is None
                or type(raw_citation.author_sc) is not bool
            ):
                author_sc = self.__driver.share_orcid(
                    raw_citation.citing, raw_citation.cited
                )

            if raw_citation.created is not None and raw_citation.timespan is not None:
                cit = Citation(
                    oci,
                    self.__base_url + quote(raw_citation.citing),
                    None,
                    self.__base_url + quote(raw_citation.cited),
                    None,
                    raw_citation.created,
                    raw_citation.timespan,
                    1,
                    self.__agent,
                    self.__source,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    self.__service,
                    "doi",
                    self.__base_url + "([[XXX__decode]])",
                    "reference",
                    journal_sc,
                    author_sc,
                    None,
                    "Creation of the citation",
                    None,
                )
            else:
                cit = Citation(
                    oci,
                    self.__base_url + quote(raw_citation.cited),
                    citing_date,
                    self.__base_url + quote(raw_citation.cited),
                    cited_date,
                    None,
                    None,
                    1,
                    self.__agent,
                    self.__source,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    self.__service,
                    "doi",
                    self.__base_url + "([[XXX__decode]])",
                    "reference",
                    journal_sc,
                    author_sc,
                    None,
                    "Creation of the citation",
                    None,
                )

            return cit
        else:
            self.error_in_ids_existence += 1
            return None

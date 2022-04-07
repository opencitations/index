from index.identifier.identifiermanager import IdentifierManager
from re import sub, match
from urllib.parse import unquote, quote
from requests import get
from json import loads
from index.support.csv_manager import CSVManager
from requests import ReadTimeout
from requests.exceptions import ConnectionError
from time import sleep


class DOIManager(IdentifierManager):
    """This class implements an identifier manager for doi identifier"""

    def __init__(self, valid_doi=None, use_api_service=True):
        """DOI manager constructor.

        Args:
            valid_doi (str, optional): the set of valid doi. Defaults to None.
            use_api_service (bool, optional): indicates if use the api. Defaults to True.
        """
        if valid_doi is None:
            valid_doi = CSVManager(store_new=False)

        self.api = "https://doi.org/api/handles/"
        self.valid_doi = valid_doi
        self.use_api_service = use_api_service
        self.p = "doi:"
        super(DOIManager, self).__init__()

    def set_valid(self, id_string):
        """Set a doi as a valid doi.

        Args:
            id_string (str): the doi to add as valid
        """
        doi = self.normalise(id_string, include_prefix=True)

        if self.valid_doi.get_value(doi) is None:
            self.valid_doi.add_value(doi, "v")

    def is_valid(self, id_string):
        """Check if a doi is valid.

        Args:
            id_string (str): the doi to check

        Returns:
            bool: true if the doi is valid, false otherwise.
        """
        doi = self.normalise(id_string, include_prefix=True)

        if doi is None or match("^doi:10\\..+/.+$", doi) is None:
            return False
        else:
            if self.valid_doi.get_value(doi) is None:
                if self.__doi_exists(doi):
                    self.valid_doi.add_value(doi, "v")
                else:
                    self.valid_doi.add_value(doi, "i")

            return "v" in self.valid_doi.get_value(doi)

    def normalise(self, id_string, include_prefix=False):
        """It returns the doi normalized.

        Args:
            id_string (str): the doi to normalize.
            include_prefix (bool, optional): indicates if include the prefix. Defaults to False.

        Returns:
            str: the normalized doi
        """
        try:
            doi_string = sub(
                "\0+", "", sub("\s+", "", unquote(id_string[id_string.index("10.") :]))
            )
            return "%s%s" % (
                self.p if include_prefix else "",
                doi_string.lower().strip(),
            )
        except:  # Any error in processing the DOI will return None
            return None

    def __doi_exists(self, doi_full):
        if self.use_api_service:
            doi = self.normalise(doi_full)
            tentative = 3
            while tentative:
                tentative -= 1
                try:
                    r = get(self.api + quote(doi), headers=self.headers, timeout=30)
                    if r.status_code == 200:
                        r.encoding = "utf-8"
                        json_res = loads(r.text)
                        return json_res.get("responseCode") == 1
                except ReadTimeout:
                    pass  # Do nothing, just try again
                except ConnectionError:
                    sleep(5)  # Sleep 5 seconds, then try again

        return False

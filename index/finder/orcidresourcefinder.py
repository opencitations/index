from index.finder.resourcefinder import ApiDOIResourceFinder
from requests import get
from urllib.parse import quote
from json import loads
import index.support.dictionary as sd


class ORCIDResourceFinder(ApiDOIResourceFinder):
    """This class implements an identifier manager for orcid identifier"""

    def __init__(
        self, date=None, orcid=None, issn=None, doi=None, use_api_service=True, key=None
    ):
        """ORCID resource finder constructor.

        Args:
            date (str, optional): path to date file. Defaults to None.
            orcid (str, optional): path to orcid file. Defaults to None.
            issn (str, optional): path to issn file. Defaults to None.
            doi (str, optional): path to doi file. Defaults to None.
            use_api_service (bool, optional): true if you want to use api service. Defaults to True.
            key (str, optional): api key. Defaults to None.
        """
        self.key = key
        self.use_api_service = use_api_service
        self.api = "https://pub.orcid.org/v2.1/search?q="
        super(ORCIDResourceFinder, self).__init__(
            date=date, orcid=orcid, issn=issn, doi=doi, use_api_service=use_api_service
        )

    def _get_orcid(self, json_obj):
        result = set()

        if json_obj is not None:
            for item in json_obj:
                orcid = item.get("orcid-identifier")
                if orcid is not None:
                    orcid_norm = self.om.normalise(orcid["path"])
                    if orcid_norm is not None:
                        result.add(orcid_norm)

        return result

    def _call_api(self, doi_full):
        if self.use_api_service:
            if self.key is not None:
                self.headers["Authorization"] = "Bearer %s" % self.key
            self.headers["Content-Type"] = "application/json"

            doi = self.dm.normalise(doi_full)
            r = get(
                self.api + quote('doi-self:"%s" OR doi-self:"%s"' % (doi, doi.upper())),
                headers=self.headers,
                timeout=30,
            )
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                return json_res.get("result")

from argparse import ArgumentParser
from os import sep

from index.finder.crossrefresourcefinder import CrossrefResourceFinder
from index.finder.dataciteresourcefinder import DataCiteResourceFinder
from index.finder.orcidresourcefinder import ORCIDResourceFinder
from index.finder.resourcefinder import ResourceFinderHandler
from index.operator.driver import OperatorDriver
from index.support.csv_manager import CSVManager
from index.identifier.doimanager import DOIManager


class FileDriver(OperatorDriver):
    """It implements citation operator driver routines using
    file."""

    def set_args(self, arg_parser: ArgumentParser, config_file):
        arg_parser.add_argument(
            "-doi",
            "--doi_file",
            default=config_file["file_driver"]["doi_file"],
            help="The file where the valid and invalid DOIs are stored.",
        )
        arg_parser.add_argument(
            "-date",
            "--date_file",
            default=config_file["file_driver"]["date_file"],
            help="The file that maps id of bibliographic resources with their publication date.",
        )
        arg_parser.add_argument(
            "-orcid",
            "--orcid_file",
            default=config_file["file_driver"]["orcid_file"],
            help="The file that maps id of bibliographic resources with the ORCID of its authors.",
        )
        arg_parser.add_argument(
            "-issn",
            "--issn_file",
            default=config_file["file_driver"]["issn_file"],
            help="The file that maps id of bibliographic resources with the ISSN of the journal "
            "they have been published in.",
        )

    def init(self, data, doi_file, date_file, orcid_file, issn_file, orcid, no_api):
        super().__init__()

        valid_doi, id_date, id_orcid, id_issn = FileDriver._create_csv(
            doi_file, date_file, orcid_file, issn_file
        )

        self.id_manager = DOIManager(valid_doi, use_api_service=not no_api)
        crossref_rf = CrossrefResourceFinder(
            date=id_date,
            orcid=id_orcid,
            issn=id_issn,
            doi=valid_doi,
            use_api_service=not no_api,
        )
        datacite_rf = DataCiteResourceFinder(
            date=id_date,
            orcid=id_orcid,
            issn=id_issn,
            doi=valid_doi,
            use_api_service=not no_api,
        )
        orcid_rf = ORCIDResourceFinder(
            date=id_date,
            orcid=id_orcid,
            issn=id_issn,
            doi=valid_doi,
            use_api_service=True if orcid is not None and not no_api else False,
            key=orcid,
        )

        self.rf_handler = ResourceFinderHandler([crossref_rf, datacite_rf, orcid_rf])

        self.exi_ocis = CSVManager.load_csv_column_as_set(data + sep + "data", "oci")

    def _create_csv(doi_file, date_file, orcid_file, issn_file):
        valid_doi = CSVManager(csv_path=doi_file)
        id_date = CSVManager(csv_path=date_file)
        id_orcid = CSVManager(csv_path=orcid_file)
        id_issn = CSVManager(csv_path=issn_file)

        return valid_doi, id_date, id_orcid, id_issn

    def share_orcid(self, citing, cited):
        return self.rf_handler.share_orcid(citing, cited)

    def share_issn(self, citing, cited):
        return self.rf_handler.share_issn(citing, cited)

    def get_date(self, doi):
        return self.rf_handler.get_date(doi)

    def oci_exists(self, oci):
        result = oci in self.exi_ocis
        if not result:
            self.exi_ocis.add(oci)
        return result

    def are_valid(self, citing, cited):
        return self.id_manager.is_valid(citing) and self.id_manager.is_valid(cited)

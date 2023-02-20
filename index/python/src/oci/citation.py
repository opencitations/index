#!python
# Copyright (c) 2022 The OpenCitations Index Authors.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from collections import deque
from csv import DictReader
from csv import DictWriter
from datetime import datetime
from io import StringIO
from json import dumps, load, loads, JSONDecodeError
from os.path import exists, dirname
from os import makedirs
from errno import EEXIST
from re import match, findall, sub
from urllib.parse import quote, unquote
from xml.etree import ElementTree
from SPARQLWrapper import SPARQLWrapper, JSON
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from rdflib import ConjunctiveGraph, RDF, RDFS, XSD, URIRef, Literal, Namespace
from requests import get

REFERENCE_CITATION_TYPE = "reference"
SUPPLEMENT_CITATION_TYPE = "supplement"
DEFAULT_CITATION_TYPE = REFERENCE_CITATION_TYPE
CITATION_TYPES = (REFERENCE_CITATION_TYPE, SUPPLEMENT_CITATION_TYPE)
DEFAULT_DATE = datetime(1970, 1, 1, 0, 0)
AGENT_NAME = "OpenCitations"
USER_AGENT = (
    "OCI / %s (via OpenCitations - http://opencitations.net; mailto:contact@opencitations.net)"
    % AGENT_NAME
)
URL = "https://github.com/opencitations/oci/blob/master/oci.py"
BASE_URL = "https://w3id.org/oc/virtual/"
W = "WARNING"
E = "ERROR"
I = "INFO"
PREFIX_REGEX = "0[1-9]+0"
VALIDATION_REGEX = "^%s[0-9]+$" % PREFIX_REGEX
FORMATS = {
    "xml": "xml",
    "rdfxml": "xml",
    "rdf/xml": "xml",
    "application/rdf+xml": "xml",
    "turtle": "turtle",
    "ttl": "turtle",
    "rdf": "turtle",
    "text/turtle": "turtle",
    "json": "json",
    "scholix": "scholix",
    "application/json": "json",
    "json-ld": "json-ld",
    "jsonld": "json-ld",
    "application/ld+json": "json-ld",
    "n-triples": "nt11",
    "ntriples": "nt11",
    "nt": "nt11",
    "nq": "nquads",
    "text/plain": "nt11",
    "text/n-triples": "nt11",
    "csv": "csv",
    "text/csv": "csv",
}


class Citation(object):
    """This class represents the citation inside index."""

    cito_base = "http://purl.org/spar/cito/"
    cites = URIRef(cito_base + "cites")
    citation = URIRef(cito_base + "Citation")
    author_self_citation = URIRef(cito_base + "AuthorSelfCitation")
    journal_self_citation = URIRef(cito_base + "JournalSelfCitation")
    has_citation_creation_date = URIRef(cito_base + "hasCitationCreationDate")
    has_citation_time_span = URIRef(cito_base + "hasCitationTimeSpan")
    has_citing_entity = URIRef(cito_base + "hasCitingEntity")
    has_cited_entity = URIRef(cito_base + "hasCitedEntity")

    datacite_base = "http://purl.org/spar/datacite/"
    has_identifier = URIRef(datacite_base + "hasIdentifier")
    identifier = URIRef(datacite_base + "Identifier")
    uses_identifier_scheme = URIRef(datacite_base + "usesIdentifierScheme")
    oci = URIRef(datacite_base + "oci")

    literal_base = "http://www.essepuntato.it/2010/06/literalreification/"
    has_literal_value = URIRef(literal_base + "hasLiteralValue")

    prism_base = "http://prismstandard.org/namespaces/basic/2.0/"
    publication_date = URIRef(prism_base + "publicationDate")

    prov_base = "http://www.w3.org/ns/prov#"
    prov_entity = URIRef(prov_base + "Entity")
    was_attributed_to = URIRef(prov_base + "wasAttributedTo")
    had_primary_source = URIRef(prov_base + "hadPrimarySource")
    generated_at_time = URIRef(prov_base + "generatedAtTime")
    invalidated_at_time = URIRef(prov_base + "invalidatedAtTime")
    specialization_of = URIRef(prov_base + "specializationOf")
    was_derived_from = URIRef(prov_base + "wasDerivedFrom")

    oco_base = "https://w3id.org/oc/ontology/"
    has_update_query = URIRef(oco_base + "hasUpdateQuery")

    dc_base = "http://purl.org/dc/terms/"
    description = URIRef(dc_base + "description")

    header_citation_data = [
        "oci",
        "citing",
        "cited",
        "creation",
        "timespan",
        "journal_sc",
        "author_sc",
    ]
    header_provenance_data = [
        "oci",
        "snapshot",
        "agent",
        "source",
        "created",
        "invalidated",
        "description",
        "update",
    ]

    def __init__(
        self,
        oci,
        citing_url,
        citing_pub_date,
        cited_url,
        cited_pub_date,
        creation,
        timespan,
        prov_entity_number,
        prov_agent_url,
        source,
        prov_date,
        service_name,
        id_type,
        id_shape,
        citation_type,
        journal_sc=False,
        author_sc=False,
        prov_inv_date=None,
        prov_description=None,
        prov_update=None,
    ):
        """Citation constructor.

        Args:
            oci (str): citation identifier.
            citing_url (str): citing url.
            citing_pub_date (str): citing publication date.
            cited_url (str): cited url.
            cited_pub_date (str): cited publication date.
            creation (str): creation time.
            timespan (str): timespan.
            prov_entity_number (str): provenance entity number.
            prov_agent_url (str): provenance agent url.
            source (str): source string.
            prov_date (str): provenance date.
            service_name (str): service name.
            id_type (str): id type, e.g. doi.
            id_shape (str): url to the id shape.
            citation_type (str): citation type.
            journal_sc (bool, optional): true if it is a journal self-cited. Defaults to False.
            author_sc (bool, optional): true if it is a  author self-cited. Defaults to False.
            prov_inv_date (str, optional): provenance invalidation time. Defaults to None.
            prov_description (str, optional): provenance description. Defaults to None.
            prov_update (str, optional): provenance update. Defaults to None.
        """
        self.oci = oci
        self.citing_url = citing_url
        self.cited_url = cited_url
        self.duration = Citation.check_duration(timespan)
        self.creation_date = Citation.check_date(
            creation[:10] if creation else creation
        )
        self.author_sc = "yes" if author_sc else "no"
        self.journal_sc = "yes" if journal_sc else "no"
        self.citing_pub_date = Citation.check_date(
            citing_pub_date[:10] if citing_pub_date else citing_pub_date
        )
        self.cited_pub_date = Citation.check_date(
            cited_pub_date[:10] if cited_pub_date else cited_pub_date
        )

        self.citation_type = (
            citation_type if citation_type in CITATION_TYPES else DEFAULT_CITATION_TYPE
        )

        # Set uniformly all the time-related data in a citation
        if self.citing_pub_date is None and self.creation_date is not None:
            self.citing_pub_date = self.creation_date
        if (
            self.cited_pub_date is None
            and self.creation_date is not None
            and self.duration
        ):
            self.cited_pub_date = Citation.check_date(
                Citation.get_date(self.creation_date, self.duration)
            )
        if self.cited_pub_date is None:
            self.duration = None

        if self.contains_years(self.citing_pub_date):
            self.creation_date = self.citing_pub_date[:10]

            if self.contains_years(self.cited_pub_date):
                citing_contains_months = Citation.contains_months(self.citing_pub_date)
                cited_contains_months = Citation.contains_months(self.cited_pub_date)
                citing_contains_days = Citation.contains_days(self.citing_pub_date)
                cited_contains_days = Citation.contains_days(self.cited_pub_date)

                # Handling incomplete dates
                citing_complete_pub_date = self.creation_date
                cited_complete_pub_date = self.cited_pub_date[:10]
                if citing_contains_months and not cited_contains_months:
                    cited_complete_pub_date += self.citing_pub_date[4:7]
                elif not citing_contains_months and cited_contains_months:
                    citing_complete_pub_date += self.cited_pub_date[4:7]
                if citing_contains_days and not cited_contains_days:
                    cited_complete_pub_date += self.citing_pub_date[7:]
                elif not citing_contains_days and cited_contains_days:
                    citing_complete_pub_date += self.cited_pub_date[7:]

                try:
                    citing_pub_datetime = parse(
                        citing_complete_pub_date, default=DEFAULT_DATE
                    )
                except ValueError:  # It is not a leap year
                    citing_pub_datetime = parse(
                        citing_complete_pub_date[:7] + "-28", default=DEFAULT_DATE
                    )
                try:
                    cited_pub_datetime = parse(
                        cited_complete_pub_date, default=DEFAULT_DATE
                    )
                except ValueError:  # It is not a leap year
                    cited_pub_datetime = parse(
                        cited_complete_pub_date[:7] + "-28", default=DEFAULT_DATE
                    )

                delta = relativedelta(citing_pub_datetime, cited_pub_datetime)
                self.duration = Citation.get_duration(
                    delta,
                    citing_contains_months and cited_contains_months,
                    citing_contains_days and cited_contains_days,
                )

        self.prov_entity_number = prov_entity_number
        self.prov_agent_url = prov_agent_url
        self.prov_date = Citation.check_datetime(prov_date)
        self.service_name = service_name
        self.prov_inv_date = Citation.check_datetime(prov_inv_date)
        self.prov_description = Citation.check_string(prov_description)
        self.prov_update = Citation.check_string(prov_update)

        self.id_type = id_type
        self.id_shape = id_shape

        self.source = source
        if "[[citing]]" in self.source:
            self.source = self.source.replace(
                "[[citing]]", quote(self.get_id(citing_url))
            )
        elif "[[cited]]" in self.source:
            self.source = self.source.replace(
                "[[cited]]", quote(self.get_id(cited_url))
            )

    @staticmethod
    def check_duration(s):
        duration = sub("\s+", "", s) if s is not None else ""
        if not match("^-?P[0-9]+Y(([0-9]+M)([0-9]+D)?)?$", duration):
            duration = None
        return duration

    @staticmethod
    def check_date(s):
        date = sub("\s+", "", s)[:10] if s is not None else ""
        if not match("^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$", date):
            date = None
        if date is not None:
            try:  # Check if the date found is valid
                parse(date, default=DEFAULT_DATE)
            except ValueError:
                date = None
        return date

    @staticmethod
    def check_datetime(s):
        datetime = sub("\s+", "", s)[:19] if s is not None else ""
        if not match(
            "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", datetime
        ):
            datetime = None
        return datetime

    @staticmethod
    def check_string(s):
        if not match("^.+$", sub("\s+", "", s) if s is not None else ""):
            return None
        return s

    @staticmethod
    def set_ns(g):
        g.namespace_manager.bind("cito", Namespace(Citation.cito_base))
        g.namespace_manager.bind("datacite", Namespace(Citation.datacite_base))
        g.namespace_manager.bind("literal", Namespace(Citation.literal_base))
        g.namespace_manager.bind("prov", Namespace(Citation.prov_base))

    def get_citation_rdf(
        self, baseurl, include_oci=True, include_label=True, include_prov=True
    ):
        """It returns citation rdf.

        Args:
            baseurl (str): base url
            include_oci (bool, optional): true if you want include the oci. Defaults to True.
            include_label (bool, optional): true if you want include the label. Defaults to True.
            include_prov (bool, optional): true if you want include the provenance. Defaults to True.

        Returns:
            ConjunctiveGraph: citation graph
        """
        (
            citation_graph,
            citation,
            citation_corpus_id,
            prov_entity,
        ) = self.__get_citation_rdf_entity(baseurl)

        if include_label:
            citation_graph.add(
                (
                    citation,
                    RDFS.label,
                    Literal("citation %s [%s]" % (self.oci, citation_corpus_id)),
                )
            )
        citation_graph.add((citation, RDF.type, self.citation))
        if self.author_sc == "yes":
            citation_graph.add((citation, RDF.type, self.author_self_citation))
        if self.journal_sc == "yes":
            citation_graph.add((citation, RDF.type, self.journal_self_citation))

        if self.citing_url is not None:
            citing_br = URIRef(self.citing_url)
            citation_graph.add((citation, self.has_citing_entity, citing_br))

        if self.cited_url is not None:
            cited_br = URIRef(self.cited_url)
            citation_graph.add((citation, self.has_cited_entity, cited_br))

        if self.creation_date is not None:
            if Citation.contains_days(self.creation_date):
                xsd_type = XSD.date
            elif Citation.contains_months(self.creation_date):
                xsd_type = XSD.gYearMonth
            else:
                xsd_type = XSD.gYear

            citation_graph.add(
                (
                    citation,
                    self.has_citation_creation_date,
                    Literal(self.creation_date, datatype=xsd_type, normalize=False),
                )
            )
            if self.duration is not None:
                citation_graph.add(
                    (
                        citation,
                        self.has_citation_time_span,
                        Literal(self.duration, datatype=XSD.duration),
                    )
                )

        if include_oci:
            for s, p, o in self.get_oci_rdf(
                baseurl, include_label, include_prov
            ).triples((None, None, None)):
                citation_graph.add((s, p, o))

        if include_prov:
            for s, p, o in self.get_citation_prov_rdf(baseurl).triples(
                (None, None, None)
            ):
                citation_graph.add((s, p, o))

        return citation_graph

    def get_citation_prov_rdf(self, baseurl):
        """It returns the citation provenance in rdf.

        Args:
            baseurl (str): base url

        Returns:
            ConjunctiveGraph: citation graph
        """
        (
            citation_graph,
            citation,
            citation_corpus_id,
            prov_entity,
        ) = self.__get_citation_rdf_entity(baseurl, is_prov=True)

        citation_graph.add((prov_entity, RDF.type, self.prov_entity))
        citation_graph.add((prov_entity, self.specialization_of, citation))
        citation_graph.add(
            (prov_entity, self.was_attributed_to, URIRef(self.prov_agent_url))
        )
        citation_graph.add((prov_entity, self.had_primary_source, URIRef(self.source)))
        citation_graph.add(
            (
                prov_entity,
                self.generated_at_time,
                Literal(self.prov_date, datatype=XSD.dateTime),
            )
        )

        if self.prov_inv_date is not None:
            citation_graph.add(
                (
                    prov_entity,
                    self.invalidated_at_time,
                    Literal(self.prov_inv_date, datatype=XSD.dateTime),
                )
            )
        if self.prov_description is not None:
            citation_graph.add(
                (prov_entity, self.description, Literal(self.prov_description))
            )
        if self.prov_update is not None:
            citation_graph.add(
                (prov_entity, self.has_update_query, Literal(self.prov_update))
            )
            citation_graph.add(
                (
                    prov_entity,
                    self.was_derived_from,
                    URIRef(
                        str(prov_entity).rsplit("/", 1)[0]
                        + "/"
                        + str(self.prov_entity_number - 1)
                    ),
                )
            )

        return citation_graph

    def __get_citation_rdf_entity(self, baseurl, is_prov=False):
        oci_no_prefix = self.oci.replace("oci:", "")
        citation_corpus_id = "ci/" + oci_no_prefix
        citation = URIRef(baseurl + citation_corpus_id)
        prov_entity = None

        if is_prov:
            prov_url = baseurl + citation_corpus_id + "/prov/"
            prov_entity = URIRef(prov_url + "se/" + str(self.prov_entity_number))
            citation_graph = ConjunctiveGraph(identifier=prov_url)
        else:
            citation_graph = ConjunctiveGraph()
        Citation.set_ns(citation_graph)

        return citation_graph, citation, citation_corpus_id, prov_entity

    def get_oci_rdf(self, baseurl, include_label=True, include_prov=True):
        """It returns the oci rdf.

        Args:
            baseurl (str): base url
            include_label (bool, optional): true if you want include the label. Defaults to True.
            include_prov (bool, optional): true if you want include the provenance. Defaults to True.

        Returns:
            ConjunctiveGraph: identifier graph
        """
        (
            identifier_graph,
            identifier,
            identifier_local_id,
            identifier_corpus_id,
            prov_entity,
        ) = self.__get_oci_rdf_entity(baseurl)

        if include_label:
            identifier_graph.add(
                (
                    identifier,
                    RDFS.label,
                    Literal(
                        "identifier %s [%s]"
                        % (identifier_local_id, identifier_corpus_id)
                    ),
                )
            )
        identifier_graph.add((identifier, RDF.type, self.identifier))
        identifier_graph.add((identifier, self.uses_identifier_scheme, self.oci))
        identifier_graph.add((identifier, self.has_literal_value, Literal(self.oci)))

        if include_prov:
            for s, p, o in self.get_oci_prov_rdf(baseurl).triples((None, None, None)):
                identifier_graph.add((s, p, o))

        return identifier_graph

    def get_oci_prov_rdf(self, baseurl):
        """It returns the oci provenance.

        Args:
            baseurl (str): base url

        Returns:
            ConjunctiveGraph: citation graph
        """
        (
            identifier_graph,
            identifier,
            identifier_local_id,
            identifier_corpus_id,
            prov_entity,
        ) = self.__get_oci_rdf_entity(baseurl, True)

        identifier_graph.add((prov_entity, RDF.type, self.prov_entity))
        identifier_graph.add((prov_entity, self.specialization_of, identifier))
        identifier_graph.add(
            (prov_entity, self.was_attributed_to, URIRef(self.prov_agent_url))
        )
        identifier_graph.add(
            (prov_entity, self.had_primary_source, URIRef(self.source))
        )
        identifier_graph.add(
            (
                prov_entity,
                self.generated_at_time,
                Literal(self.prov_date, datatype=XSD.dateTime),
            )
        )

        return identifier_graph

    def __get_oci_rdf_entity(self, baseurl, is_prov=False):
        identifier_local_id = "ci-" + self.oci.replace("oci:", "")
        identifier_corpus_id = "id/" + identifier_local_id

        identifier = URIRef(baseurl + identifier_corpus_id)
        prov_entity = None

        if is_prov:
            prov_baseurl = baseurl + identifier_corpus_id + "/prov/"
            identifier = URIRef(prov_baseurl + "se/1")
            identifier_graph = ConjunctiveGraph(identifier=prov_baseurl)
        else:
            identifier_graph = ConjunctiveGraph()
        Citation.set_ns(identifier_graph)

        return (
            identifier_graph,
            identifier,
            identifier_local_id,
            identifier_corpus_id,
            prov_entity,
        )

    def get_citation_csv(self):
        """It returns the citation in csv.

        Returns:
            str: citation in csv format.
        """
        s_res = StringIO()
        writer = DictWriter(s_res, Citation.header_citation_data)
        writer.writeheader()
        writer.writerow(loads(self.get_citation_json()))
        return s_res.getvalue()

    def get_citation_prov_csv(self):
        """It returns the citation provenance in csv.

        Returns:
            str: citation provenance in csv format.
        """
        s_res = StringIO()
        writer = DictWriter(s_res, Citation.header_provenance_data)
        writer.writeheader()
        writer.writerow(loads(self.get_citation_prov_json()))
        return s_res.getvalue()

    def get_citation_json(self):
        """It returns the citation in json.

        Returns:
            str: citation in json format.
        """
        result = {
            "oci": self.oci.replace("oci:", ""),
            "citing": self.get_id(self.citing_url),
            "cited": self.get_id(self.cited_url),
            "creation": self.creation_date,
            "timespan": self.duration,
            "journal_sc": self.journal_sc,
            "author_sc": self.author_sc,
        }

        return dumps(result, indent=4, ensure_ascii=False)

    def get_citation_prov_json(self):
        """It returns the citation provenance in json.

        Returns:
            str: citation provenance in json format.
        """
        result = {
            "snapshot": self.prov_entity_number,
            "oci": self.oci.replace("oci:", ""),
            "agent": self.prov_agent_url,
            "source": self.source,
            "created": self.prov_date,
        }

        if self.prov_inv_date is not None:
            result["invalidated"] = self.prov_inv_date
        if self.prov_description is not None:
            result["description"] = self.prov_description
        if self.prov_update is not None:
            result["update"] = self.prov_update

        return dumps(result, indent=4, ensure_ascii=False)

    def get_citation_scholix(self):
        """It returns the citation in scholix.

        Returns:
            str: citation in scholix format.
        """
        if self.citation_type == REFERENCE_CITATION_TYPE:
            rel_type = "References"
        elif self.citation_type == SUPPLEMENT_CITATION_TYPE:
            rel_type = "IsSupplementedBy"
        else:
            rel_type = "References"

        result = {
            "LinkPublicationDate": self.prov_date,
            "LinkProvider": [{"Name": AGENT_NAME}, {"Name": self.service_name}],
            "RelationshipType": {"Name": rel_type},
            "LicenseURL": "https://creativecommons.org/publicdomain/zero/1.0/legalcode",
            "Source": {
                "Identifier": {
                    "ID": self.get_id(self.citing_url),
                    "IDScheme": self.id_type,
                    "IDURL": self.citing_url,
                },
                "Type": {"Name": "literature"},
            },
            "Target": {
                "Identifier": {
                    "ID": self.get_id(self.cited_url),
                    "IDScheme": self.id_type,
                    "IDURL": self.cited_url,
                },
                "Type": {"Name": "literature"},
            },
        }

        if self.citing_pub_date:
            result["Source"]["PublicationDate"] = self.citing_pub_date

        if self.cited_pub_date:
            result["Target"]["PublicationDate"] = self.cited_pub_date

        return dumps(result, indent=4, ensure_ascii=False)

    def get_id(self, entity_url):
        """It returns the id associated to a specific entity url.

        Returns:
            str: the id
        """
        decode = "XXX__decode]]" in self.id_shape
        entity_regex = sub("\[\[[^\]]+\]\]", ".+", self.id_shape)
        entity_token = sub(entity_regex, "\\1", entity_url)
        return unquote(entity_token) if decode else entity_token

    @staticmethod
    def contains_years(date):
        return date is not None and len(date) >= 4

    @staticmethod
    def contains_months(date):
        return date is not None and len(date) >= 7

    @staticmethod
    def contains_days(date):
        return date is not None and len(date) >= 10

    @staticmethod
    def get_duration(delta, consider_months, consider_days):
        result = ""
        if (
            delta.years < 0
            or (delta.years == 0 and delta.months < 0 and consider_months)
            or (
                delta.years == 0
                and delta.months == 0
                and delta.days < 0
                and consider_days
            )
        ):
            result += "-"
        result += "P%sY" % abs(delta.years)

        if consider_months:
            result += "%sM" % abs(delta.months)

        if consider_days:
            result += "%sD" % abs(delta.days)

        return result

    @staticmethod
    def get_date(creation_date, duration):
        params = {}
        for item in findall("^-?P([0-9]+Y)?([0-9]+M)?([0-9]+D)?$", duration)[0]:
            if "Y" in item:
                params["years"] = int(item[:-1])
            elif "M" in item:
                params["months"] = int(item[:-1])
            elif "D" in item:
                params["days"] = int(item[:-1])

        delta = relativedelta(**params)
        d = parse(creation_date, default=DEFAULT_DATE)
        if duration.startswith("-"):
            result = d + delta
        else:
            result = d - delta

        if "D" in duration or Citation.contains_days(creation_date):
            cut = 10
        elif "M" in duration or Citation.contains_months(creation_date):
            cut = 7
        else:
            cut = 4

        return result.strftime("%Y-%m-%d")[:cut]

    @staticmethod
    def format_rdf(g, f="text/turtle"):
        cur_format = f
        if f in FORMATS:
            cur_format = FORMATS[f]
        return g.serialize(format=cur_format, encoding="utf-8").decode("utf-8")


class OCIManager(object):
    """This class manages the oci idientifiers."""

    def __init__(
        self,
        oci_string=None,
        lookup_file=None,
        conf_file=None,
        doi_1=None,
        doi_2=None,
        prefix="",
        is_index=False,
    ):
        """OCI manager constructor.

        Args:
            oci_string (str, optional): _description_. Defaults to None.
            lookup_file (str, optional): path to the lookup file. Defaults to None.
            conf_file (str, optional): path to the config file. Defaults to None.
            doi_1 (str, optional): _description_. Defaults to None.
            doi_2 (str, optional): _description_. Defaults to None.
            prefix (str, optional): prefix to use. Defaults to "".
            is_index (bool, optional): True if the citing and cited entities are OMID identifiers
        """
        self.is_index = is_index
        self.is_valid = None
        self.messages = []
        self.f = {
            "decode": self.__decode,
            "encode": quote,
            "join": OCIManager.__join,
            "shape": OCIManager.__shape,
            "remove": OCIManager.__remove,
            "normdate": OCIManager.__normdate,
            "datestrings": OCIManager.__datestrings,
            "api": OCIManager.__call_api,
            "avoid_prefix_removal": OCIManager.__avoid_prefix_removal,
        }
        self.lookup = {}
        self.inverse_lookup = {}
        self.lookup_file = lookup_file
        self.lookup_code = -1
        if self.lookup_file is not None:
            if exists(self.lookup_file):
                with open(self.lookup_file, "r", encoding="utf8") as f:
                    lookupcsv_reader = DictReader(f)
                    code = -1
                    for row in lookupcsv_reader:
                        self.lookup[row["code"]] = row["c"]
                        self.inverse_lookup[row["c"]] = row["code"]
                        code = int(row["code"])
                    self.lookup_code = code
            else:
                with open(self.lookup_file, "w", encoding="utf8") as f:
                    f.write('"c","code"')
        else:
            self.add_message(
                "__init__",
                W,
                "No lookup file has been found (path: '%s')." % lookup_file,
            )
        self.conf = None
        if conf_file is not None and exists(conf_file):
            with open(conf_file, encoding="utf8") as f:
                self.conf = load(f)
        else:
            self.add_message(
                "__init__",
                W,
                "No configuration file has been found (path: '%s')." % conf_file,
            )

        if oci_string:
            self.oci = oci_string.lower().strip()
        elif doi_1 and doi_2:
            self.oci = self.get_oci(doi_1, doi_2, prefix)
        else:
            self.oci = None
            self.add_message("__init__", W, "No OCI specified!")

    def __match_str_to_lookup(self, str_val):
        ci_str = []
        for c in str_val:
            if c not in self.inverse_lookup:
                self.__update_lookup(c)
            ci_str.append(str(self.inverse_lookup[c]))
        return "".join(ci_str)

    def __update_lookup(self, c):
        if c not in self.inverse_lookup:
            self.__calc_next_lookup_code()
            code = str(self.lookup_code)
            if len(code) == 1:
                code = "0" + code
            self.inverse_lookup[c] = code
            self.lookup[code] = c
            self.__write_txtblock_on_csv(self.lookup_file, '\n"%s","%s"' % (c, code))

    def __write_txtblock_on_csv(self, csv_path, block_txt):
        if csv_path is not None and exists(csv_path):
            self.__check_make_dirs(csv_path)
            with open(csv_path, "a", newline="", encoding="utf8") as csvfile:
                csvfile.write(block_txt)

    def __calc_next_lookup_code(self):
        rem = self.lookup_code % 100
        newcode = self.lookup_code + 1
        if rem == 89:
            newcode = newcode * 10
        self.lookup_code = newcode

    def __check_make_dirs(self, filename):
        if not exists(dirname(filename)):
            try:
                makedirs(dirname(filename))
            except OSError as exc:
                if exc.errno != EEXIST:
                    raise

    def __decode(self, s):
        result = []

        for code in findall("(9*[0-8][0-9])", s):
            if code in self.lookup:
                result.append(self.lookup[code])
            else:
                result.append(code)

        return "10." + "".join(result)

    def __decode_inverse(self, doi):
        return self.__match_str_to_lookup(doi.replace("10.", ""))

    def get_oci(self, doi_1, doi_2, prefix):
        """It returns the oci associated to the citation.

        Args:
            doi_1 (str): citing
            doi_2 (str): cited
            prefix (str): prefix

        Returns:
            str: the oci
        """
        _citing = self.__decode_inverse(doi_1),
        _cited = self.__decode_inverse(doi_2),
        if self.is_index:
            _citing = doi_1.replace("omid:","")
            _cited = doi_2.replace("omid:","")
        self.oci = "oci:%s%s-%s%s" % (
            prefix,
            _citing,
            prefix,
            _cited,
        )
        return self.oci

    @staticmethod
    def __join(l, j_value=""):
        if type(l) is list:
            return j_value.join(l)
        else:
            return l

    @staticmethod
    def __avoid_prefix_removal(s):
        return "0123567890" + s

    @staticmethod
    def __shape(id_s, base=""):
        return base + quote(id_s)

    @staticmethod
    def __remove(id_s, to_remove=""):
        return id_s.replace(to_remove, "")

    @staticmethod
    def __normdate(date_s):
        return sub("[^\d-]", "", date_s)

    @staticmethod
    def __datestrings(l):
        result = []

        for i in l:
            i_str = str(i)
            if len(i_str) == 1:
                i_str = "0" + i_str
            result.append(i_str)

        return result

    def __execute_query(self, citing_entity, cited_entity):
        result = None

        if self.conf is None:
            self.add_message(
                "__execute_query",
                E,
                "No citations can be retrieved since no configuration "
                "file has been specified.",
            )
        else:
            try:
                i = iter(self.conf["services"])
                while result is None:
                    item = next(i)
                    (
                        name,
                        query,
                        api,
                        tp,
                        use_it,
                        preprocess,
                        prefix,
                        id_type,
                        id_shape,
                        citation_type,
                    ) = (
                        item.get("name"),
                        item.get("query"),
                        item.get("api"),
                        item.get("tp"),
                        item.get("use_it"),
                        item["preprocess"] if "preprocess" in item else [],
                        item["prefix"] if "prefix" in item else [],
                        item.get("id_type"),
                        item.get("id_shape"),
                        item["citation_type"]
                        if "citation_type" in item
                        else DEFAULT_CITATION_TYPE,
                    )

                    if use_it == "yes" and all(
                        sub("^(%s).+$" % PREFIX_REGEX, "\\1", p) in prefix
                        for p in (citing_entity, cited_entity)
                    ):
                        citing = sub("^%s(.+)$" % PREFIX_REGEX, "\\1", citing_entity)
                        cited = sub("^%s(.+)$" % PREFIX_REGEX, "\\1", cited_entity)

                        for f_name in preprocess:
                            citing = self.f[f_name](citing)
                            cited = self.f[f_name](cited)

                        if tp is None:
                            rest_query = api.replace(
                                "[[CITING]]", quote(citing)
                            ).replace("[[CITED]]", quote(cited))
                            structured_res, type_res = OCIManager.__call_api(rest_query)
                            if structured_res:
                                result = (
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("citing"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("cited"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("citing_date"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("cited_date"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("creation"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    self.__read_api_data(
                                        structured_res,
                                        type_res,
                                        query.get("timespan"),
                                        citing,
                                        cited,
                                        api,
                                    ),
                                    rest_query,
                                    name,
                                    id_type,
                                    id_shape,
                                    citation_type,
                                )
                        else:
                            sparql = SPARQLWrapper(tp)
                            sparql_query = sub(
                                "\\[\\[CITED\\]\\]",
                                cited,
                                sub("\\[\\[CITING\\]\\]", citing, query),
                            )

                            sparql.setQuery(sparql_query)
                            sparql.setReturnFormat(JSON)
                            q_res = sparql.query().convert()["results"]["bindings"]
                            if len(q_res) > 0:
                                answer = q_res[0]
                                result = (
                                    answer["citing"]["value"],
                                    answer["cited"]["value"],
                                    answer["citing_date"]["value"]
                                    if "citing_date" in answer
                                    else None,
                                    answer["cited_date"]["value"]
                                    if "cited_date" in answer
                                    else None,
                                    answer["creation"]["value"]
                                    if "creation" in answer
                                    else None,
                                    answer["timespan"]["value"]
                                    if "timespan" in answer
                                    else None,
                                    tp + "?query=" + quote(sparql_query),
                                    name,
                                    id_type,
                                    id_shape,
                                    citation_type,
                                )

            except StopIteration:
                pass  # No nothing

        return result

    @staticmethod
    def __call_api(u):
        structured_res = None
        type_res = None

        res = get(u, headers={"User-Agent": USER_AGENT}, timeout=30)

        if res.status_code == 200:
            res.encoding = "utf-8"
            cur_str = res.text

            try:
                structured_res = loads(cur_str)
                type_res = "json"
            except JSONDecodeError:
                structured_res = ElementTree.fromstring(cur_str)
                type_res = "xml"

        return structured_res, type_res

    def __read_api_data(self, data, type_format, access_list, citing, cited, api):
        result = None

        if data and access_list:
            access_queue = deque(access_list)
            while result is None and access_queue:
                access_string = access_queue.popleft()
                access_operations = deque(access_string.split("::"))

                access_operation = access_operations.popleft()
                if citing:
                    access_operation = sub("\[\[CITING\]\]", citing, access_operation)
                if cited:
                    access_operation = sub("\[\[CITED\]\]", cited, access_operation)

                f_to_execute = []
                if "->" in access_operation:
                    for idx, item in enumerate(access_operation.split("->")):
                        if idx:
                            f_to_execute.append(item)
                        else:
                            access_operation = item
                if match("^([^\(]+)\((.*)\)$", access_operation):
                    f_name, f_params = findall("([^\(]+)\((.*)\)", access_operation)[0]
                    f_params = f_params.split(",") if f_params else []
                    result = self.f[f_name](*f_params)
                    if type(result) is tuple:
                        result, type_format = result
                elif match("\[[0-9]+\]", access_operation):
                    cur_n = int(sub("\[([0-9]+)\]", "\\1", access_operation))
                    if type(data) is list and cur_n < len(data):
                        result = data[cur_n]
                elif match("^\[.+\]$", access_operation) and "==" in access_operation:
                    left, right = sub("^\[(.+)\]$", "\\1", access_operation).split("==")
                    if type(data) is list:
                        list_queue = deque(data)
                        while result is None and list_queue:
                            item = list_queue.popleft()
                            item_value = item.get(left)
                            if (
                                item_value is not None
                                and item_value.lower() == right.lower()
                            ):
                                result = item
                else:
                    if type_format == "json":
                        result = data.get(access_operation)
                    elif type_format == "xml":
                        el = None

                        if match("^({.+})?%s$" % access_operation, data.tag):
                            el = data
                        else:
                            children = deque(data)
                            while el is None and children:
                                child = children.popleft()
                                if match("^({.+})?%s$" % access_operation, child.tag):
                                    el = child

                        result = el

                if (
                    result is not None
                    and not access_operations
                    and type_format == "xml"
                ):
                    result = sub("\s+", " ", result.text).strip()

                if f_to_execute and result is not None:
                    for f in f_to_execute:
                        f_name, f_params = findall("([^\(]+)\((.*)\)", f)[0]
                        f_params = f_params.split(",") if f_params else []
                        f_params.insert(0, result)
                        result = self.f[f_name](*f_params)
                        if type(result) is tuple:
                            result, type_format = result

                if access_operations:
                    result = self.__read_api_data(
                        result,
                        type_format,
                        ["::".join(access_operations)],
                        citing,
                        cited,
                        api,
                    )

        return result

    def validate(self):
        """It validates the oci.

        Returns:
            bool: true if the oci is valid, false oterswise.
        """
        if self.is_valid is None:
            if not self.oci.startswith("oci:"):
                self.oci = "oci:" + self.oci
                self.add_message(
                    "validate",
                    W,
                    "The OCI specified as input doesn't start with the 'oci:' "
                    "prefix. This has beed automatically added, resulting in "
                    "the OCI '%s'." % self.oci,
                )

            self.is_valid = False
            entities = self.oci.replace("oci:", "").split("-")
            if all(match(VALIDATION_REGEX, entity) for entity in entities):
                service_queue = deque(self.conf["services"])

                while service_queue and not self.is_valid:
                    service_prefixes = service_queue.popleft()["prefix"]
                    self.is_valid = all(
                        sub("^(%s).+$" % PREFIX_REGEX, "\\1", entity)
                        in service_prefixes
                        for entity in entities
                    )

                if self.is_valid:
                    self.add_message(
                        "validate", I, "The OCI '%s' is syntactically valid." % self.oci
                    )
                else:
                    self.add_message(
                        "validate",
                        E,
                        "The supplier prefixes '%s' and '%s' used in the identifiers of "
                        "the citing and cited entities described by the OCI '%s' must be "
                        "assigned to the same supplier. A list of all the available "
                        "suppliers is available at http://opencitations.net/oci."
                        % (
                            tuple(
                                sub("^(%s).+$" % PREFIX_REGEX, "\\1", entity)
                                for entity in entities
                            )
                            + (self.oci,)
                        ),
                    )

            else:
                self.add_message(
                    "validate",
                    E,
                    "The OCI '%s' is not syntactically correct, since at least "
                    "one of the two identifiers of the citing and cited entities "
                    "described by the OCI are not compliant with the following "
                    "regular expression: '%s'." % (self.oci, VALIDATION_REGEX),
                )

        return self.is_valid

    def get_citation_object(self):
        """It returns the citation object corresponding to the oci.

        Returns:
            Citation: the citation associated to the oci.
        """
        if self.validate():
            citing_entity_local_id = sub("^oci:([0-9]+)-([0-9]+)$", "\\1", self.oci)
            cited_entity_local_id = sub("^oci:([0-9]+)-([0-9]+)$", "\\2", self.oci)

            res = self.__execute_query(citing_entity_local_id, cited_entity_local_id)
            if res is not None:
                (
                    citing_url,
                    cited_url,
                    full_citing_pub_date,
                    full_cited_pub_date,
                    creation,
                    timespan,
                    sparql_query_url,
                    name,
                    id_type,
                    id_shape,
                    citation_type,
                ) = res

                citation = Citation(
                    self.oci,
                    citing_url,
                    full_citing_pub_date,
                    cited_url,
                    full_cited_pub_date,
                    creation,
                    timespan,
                    URL,
                    sparql_query_url,
                    datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                    name,
                    id_type,
                    id_shape,
                    citation_type,
                )

                return citation
            else:
                self.add_message(
                    "get_citation_object",
                    I,
                    "No citation data have been found for the OCI '%s'. "
                    "While the OCI specified is syntactically valid, "
                    "it is possible that it does not identify any "
                    "citation at all." % self.oci,
                )
        else:
            self.add_message(
                "get_citation_object",
                E,
                "No citation data can be returned since the OCI specified is "
                "not valid.",
            )

    def get_citation_data(self, f="json"):
        """It returns the citation data in a specific format.

        Args:
            f (str, optional): format to use. Defaults to "json".

        Returns:
            str: the citation data in the specified format.
        """
        citation = self.get_citation_object()
        if citation:
            result = None
            cur_format = "json"
            if f in FORMATS:
                cur_format = FORMATS[f]

            if cur_format == "json":
                result = citation.get_citation_json()
            elif cur_format == "csv":
                result = citation.get_citation_csv()
            elif cur_format == "scholix":
                result = citation.get_citation_scholix()
            else:  # RDF format
                result = Citation.format_rdf(
                    citation.get_citation_rdf(BASE_URL), cur_format
                )

            return result

    def print_messages(self):
        """_summary_"""
        for mes in self.messages:
            print("{%s} [%s] %s" % (mes["operation"], mes["type"], mes["text"]))

    def add_message(self, fun, mes_type, text):
        """_summary_

        Args:
            fun (_type_): _description_
            mes_type (_type_): _description_
            text (_type_): _description_
        """
        self.messages.append({"operation": fun, "type": mes_type, "text": text})

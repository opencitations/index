#!python
# -*- coding: utf-8 -*-
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
from oc.index.finder.crossref import CrossrefResourceFinder
from oc.index.finder.datacite import DataCiteResourceFinder
from oc.index.finder.wd import WikidataResourceFinder
from oc.index.identifier.doi import DOIManager
from oc.index.identifier.isbn import ISBNManager
from oc.index.identifier.metaid import MetaIDManager
from oc.index.identifier.pmid import PMIDManager
from oc.index.identifier.wikidata import WikiDataIDManager

from oc_graphenricher.APIs import *
import shutil
import csv
from oc_ocdm.graph.graph_entity import GraphEntity
import os
from os import sep
from os.path import join
import random
import requests_cache
from oc_meta.plugins.crossref.crossref_processing import CrossrefProcessing
from oc_meta.run.meta_process import MetaProcess, run_meta_process
from oc_meta.lib.file_manager import get_csv_data

ID_WD_QUERIES = {
    "doi": "SELECT ?doi WHERE {{wd:{value} wdt:P356 ?doi }} ",
    "pmid": "SELECT ?pmid WHERE {{wd:{value} wdt:P698 ?pmid}}",
    "isbn": "SELECT(GROUP_CONCAT( ?booknumber; separator = ' ') as ?isbn) WHERE {{wd:{value} wdt:P212|wdt:P957 ?booknumber}}",
    "qid": """SELECT DISTINCT ?qid 
    WHERE {{{{  ?qid wdt:P356 '{value}'}} UNION {{?qid wdt:P698 '{value}'}} UNION {{  ?qid wdt:P212|wdt:P957 '{value}'}}
        }}""",
}

VALID_QUERIES = {
    "metadata": """SELECT ?id (GROUP_CONCAT( ?a; separator = '; ') as ?author) 
    ?venue ?pub_date ?title ?volume ?editor ?issue ?page ?type_id ?publisher
    {{  {{ BIND("{value}" as ?id)
  OPTIONAL {{ wd:{value} wdt:P98 ?editor}}
  OPTIONAL {{ wd:{value} wdt:P1476 ?title }} 
  OPTIONAL {{
    wd:{value} wdt:P577 ?date
    BIND(SUBSTR(str(?date), 0, 5) as ?pub_date)
  }}
  OPTIONAL {{ wd:{value} wdt:P1433 ?venue_id .
    ?venue_id rdfs:label ?venue}}
  OPTIONAL {{ wd:{value} wdt:P478 ?volume }}
  OPTIONAL {{ wd:{value} wdt:P433 ?issue }}
  OPTIONAL {{ wd:{value} wdt:P304 ?page }}
  OPTIONAL {{ wd:{value} wdt:P31 ?type_id . }}
  OPTIONAL {{
            wd:{value} wdt:P50 ?author_res .
            ?author_res wdt:P735/wdt:P1705 ?g_name ;
                        wdt:P734/wdt:P1705 ?f_name .
            BIND(CONCAT(?f_name, ", ",?g_name) as ?a) }}
 }} }} GROUP BY ?author ?venue ?pub_date ?title ?volume ?issue ?page ?publisher ?type_id ?editor ?id

        """,
}
# 'edited book', 'monograph', 'reference book', 'report', 'standard' 'book series', 'book set', 'journal', 'proceedings series', 'series', 'standard series'
TYPE_DENOMINATIONS_WD = {
    "Q13442814": "journal article",
    "Q18918145": "journal article",
    "Q1266946": "dissertation",
    "Q7318358": "journal article",
    "Q215028": "journal article",
    "Q193495": "monograph",
    "Q13136": "reference book",
    "Q23927052": "proceedings article",
    "Q1980247": "book chapter",
    "Q1172284": "dataset",
    "Q1711593": "edited book",
    "Q277759": "book series",
    "Q121769": "reference entry",
    "Q28062188": "book set",
    "Q317623": "standard",
    "Q265158": "peer review",
    "Q3331189": "book",
    "Q7725634": "book",
    "Q571": "book",
    "Q60534428": "edited book",
    "Q55915575": "journal article",
    "Q223638": "book",
    "Q234460": "other",
    "Q10870555": "report",
    "Q71631512": "other",
    "Q1643932": "other",
    "Q20540385": "book",
    "Q64548048": "report",
    "Q1228945": "report",
    "Q47461344": "other",
    "Q7433672": "book",
}


TYPE_DENOMINATIONS_DATACITE = {
    "Audiovisual": "other",
    "Book": "book",
    "BookChapter": "book chapter",
    "ComputationalNotebook": "other",
    "ConferencePaper": "proceedings article",
    "ConferenceProceeding": "proceedings",
    "DataPaper": "other",
    "Dataset": "dataset",
    "Dissertation": "dissertation",
    "Event": "other",
    "Image": "posted content",
    "InteractiveResource": "posted content",
    "Journal": "journal",
    "JournalArticle": "journal article",
    "Model": "other",
    "OutputManagementPlan": "other",
    "PeerReview": "peer review",
    "PhysicalObject": "other",
    "Preprint": "journal article",
    "Report": "report",
    "Service": "other",
    "Sound": "other",
    "Standard": "standard",
    "Text": "other",
    "Workflow": "other",
    "Other": "other",
}

FIELDNAMES = (
    "id",
    "title",
    "author",
    "pub_date",
    "venue",
    "volume",
    "issue",
    "page",
    "type",
    "publisher",
    "editor",
)


def wd_preprocessing(values, id):
    """This function preprocess the information retrieved from wikidata"""
    if values is None:
        return None
    values["id"] = id
    if "type_id" in values:
        if values["type_id"].split("entity/")[1] in TYPE_DENOMINATIONS_WD:
            values["type"] = TYPE_DENOMINATIONS_WD[
                values.pop("type_id").split("entity/")[1]
            ]
        else:
            values["type"] = ""
            print(
                f"Resource type not recognised from Wikidata: {values.pop('type_id')}"
            )
    else:
        values["type"] = ""
    return values


def datacite_preprocessing(values, id):  # TODO: better preprocessing
    """This function preprocesses the information retrieved from DataCite"""
    if values is None:
        return None
    result = {}
    result["id"] = id
    authors = []
    for person in values["creators"]:
        if "givenName" in person and "familyName" in person:
            name = ", ".join((person["familyName"], person["givenName"]))
            orcid = ""
            for identifier in person["nameIdentifiers"]:
                if identifier.get("nameIdentifierScheme") == "ORCID":
                    orcid = identifier.get("nameIdentifier").split("orcid.org/")[1]
                    orcid = f" [orcid:{orcid}]"
                    break
            name = f"{person['familyName']}, {person['givenName']}{orcid}"
            authors.append(name)
    if values["types"].get("resourceTypeGeneral") in TYPE_DENOMINATIONS_DATACITE:
        result["type"] = TYPE_DENOMINATIONS_DATACITE[
            values["types"]["resourceTypeGeneral"]
        ]
    else:
        result["type"] = ""
        val = values["types"].get("resourceTypeGeneral")
        if val is not None:
            print(f"Type from Datacite not recognised: {val}")

    result["publisher"] = values["publisher"]

    result["author"] = "; ".join(authors)

    result["title"] = values["titles"][0]["title"]

    result["pub_date"] = str(values["publicationYear"])

    return result


class MetadataPopulator:
    def __init__(self) -> None:
        self.crossref_processor = CrossrefProcessing()
        self.cr_finder = CrossrefResourceFinder()
        self.wd_finder = WikidataResourceFinder(queries=VALID_QUERIES)
        self.datacite_finder = DataCiteResourceFinder()

    def launch_service(self, ids) -> dict:
        """
        This method launches which services to query and adds the authors
        :param ids: a dictionary containing ids
        :return: a dictionary containing the information retrieved
        """

        result = None

        if "doi" in ids:
            # this is a doi
            cr_result = self.cr_finder._call_api(ids["doi"])
            if cr_result is not None:
                result = self.crossref_processor.csv_creator(cr_result)
            elif cr_result is None:
                result = datacite_preprocessing(
                    self.datacite_finder._call_api(ids["doi"]), ids
                )

        if "wikidata" in ids and result is None:
            result = wd_preprocessing(
                self.wd_finder._call_api("metadata", value=ids["wikidata"]), ids
            )

        if result is None:
            result = {}

        for field in FIELDNAMES:
            if field not in result:
                result[field] = ""

        id_string = []
        for identifier in ids:
            for id in ids[identifier].split(" "):
                id_string.append(f"{identifier}:{id}")
        result["id"] = " ".join(id_string)

        return result


class IDPopulator:
    """This class is responsible for validating and populating a string of ids"""

    def __init__(self) -> None:
        self.wd_finder = WikidataResourceFinder(queries=ID_WD_QUERIES)

        self.ids = {
            "doi": DOIManager(),
            "pmid": PMIDManager(),
            "wikidata": WikiDataIDManager(),
            "meta": MetaIDManager(),
            "isbn": ISBNManager(),
        }
        self.seen_ids = {}
        self.id_num = 0

    def validate_ids(self, ids: str) -> dict:
        """This method transforms an id string into a dictionary with multiple ids and launchs the right pipeline
        :params ids: a string with the ids separated by ';'
        :params return_ids: a boolean that indicates if just the ids need to be cached
        :returns: a dictionary with the populated and validated ids
        """

        identifiers = {}
        try:
            for id in ids.split(
                "; "
            ):  # first, we check the presence of ids. Is this for loop necessary? maybe there is an alternative
                if "'" in id:
                    id = id.replace("'", "")
                if '"' in id:
                    id = id.replace('"', "")
                prefix_idx = id.index(":")

                prefix, id = id[:prefix_idx], id[prefix_idx + 1 :]
                if prefix in self.ids:
                    id = getattr(self.ids[prefix], "normalise")(id)
                    if id != None:
                        if f"{prefix}:{id}" in self.seen_ids:
                            return self.seen_ids[f"{prefix}:{id}"]
                    else:
                        continue
                    identifiers[prefix] = id
        except:
            return None
        if len(identifiers) == 0:  # if no id is present, we return None
            return None
        else:
            return identifiers

    def complete_ids(self, identifiers: dict):
        missing_ids = list(el for el in self.ids if el not in identifiers)
        if len(missing_ids) > 0:
            if "wikidata" in missing_ids:
                for key in identifiers:

                    possible_wd = self.wd_finder._call_api(
                        "qid", value=identifiers[key]
                    )
                    if possible_wd is None or len(possible_wd) == 0:
                        if any(char.islower() for char in identifiers[key]):
                            tmp = identifiers[key]
                            identifiers[key] = identifiers[key].upper()
                            completed = self.complete_ids(identifiers)
                            completed[key] = tmp
                            return completed
                        else:
                            continue
                    if len(possible_wd) > 1:
                        raise Warning(
                            f"There is more than one wikidata id for {identifiers[key]}"
                        )

                    possible_wd = self.ids["wikidata"].normalise(possible_wd["qid"])

                    if possible_wd is not None:
                        identifiers["wikidata"] = possible_wd
                        missing_ids.remove("wikidata")
                        break

            if "wikidata" in identifiers:
                for id in missing_ids:
                    new_id = self.wd_finder._call_api(id, value=identifiers["wikidata"])

                    if (
                        new_id is not None
                        and new_id.get(id) is not None
                        and new_id.get(id) != ""
                    ):
                        to_add = []
                        for el in new_id[id].split(" "):
                            to_add.append(getattr(self.ids[id], "normalise")(el))
                        identifiers[id] = " ".join(to_add)
        return identifiers

    def populate_ids(self, ids: str) -> tuple:
        validated = self.validate_ids(ids)
        if isinstance(validated, int):
            return validated
        elif validated is not None:

            ids = self.complete_ids(validated)
            tmp = []
            seen = False
            pos = self.id_num
            for identifier in ids:
                id_n = ids[identifier]
                for id in id_n.split(" "):
                    id = f"{identifier}:{id}"
                    if id in self.seen_ids:
                        seen = True
                        pos = self.seen_ids[id]
                        continue
                    tmp.append(id)
            if seen:
                for el in tmp:
                    self.seen_ids[el] = pos
                return pos
            self.id_num += 1
            for el in tmp:
                self.seen_ids[el] = pos
            return ids, pos


class AuthorPopulator:
    """This class adds information about the authors"""

    def __init__(self) -> None:
        self.viaf_api = VIAF()
        self.orcid_api = ORCID()

    def get_author_info(self, ids: dict, resource: dict) -> str:
        authors_complete = []
        for author in resource["author"].split("; "):
            if "orcid:" in author and "viaf:" in author:
                authors_complete.append(author)
                continue
            author_ids = {}
            if "[" in author:  # if there is an identifier:
                author, identifiers = author.split(" [")
                for el in identifiers[:-1].split(" "):
                    prefix, el = el.split(":")
                    author_ids[prefix] = el
            try:
                last_name, first_name = author.split(", ")

            except ValueError:
                authors_complete.append(author)
                continue
            author_list = [(first_name, last_name, None, None)]
            if "orcid" not in author_ids:
                if "doi" in ids:
                    author_list = self.orcid_api.query(
                        author_list, [(GraphEntity.iri_doi, ids["doi"])]
                    )

                elif "pmid" in ids:
                    author_list = self.orcid_api.query(
                        author_list, [(GraphEntity.iri_pmid, ids["pmid"])]
                    )
                if author_list[0][2] is not None:  # If an orcid is found, add it
                    author_ids["orcid"] = author_list[0][2]
            if "viaf" not in author_ids:
                possible_viaf = self.viaf_api.query(
                    first_name, last_name, resource["title"]
                )
                if possible_viaf != None:
                    author_ids["viaf"] = possible_viaf
            author_ids = " ".join(f"{k}:{v}" for k, v in author_ids.items())
            author = f"{last_name}, {first_name} [{author_ids}]"
            authors_complete.append(author)
        return "; ".join(authors_complete)


class MetaFeeder:
    """
    This class manages the creation of files to send to OC Meta, interfaces with the database and
    """

    def __init__(self, meta_config=join("..", "meta_config.yaml")) -> None:
        self.id_populator = IDPopulator()
        self.metadata_populator = MetadataPopulator()
        self.author_pop = AuthorPopulator()
        self.citations = []
        self.meta_process = MetaProcess(config=meta_config)
        self.meta_folder = join("..", "output")
        self.tmp_dir = join("..", "croci_tmp")
        if not os.path.isdir(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        if not os.path.isdir(join(self.tmp_dir, "meta")):
            os.mkdir(join(self.tmp_dir, "meta"))
        self.clean_dir()

    def parse(self, file) -> str:
        """This method manages the parsing of input files."""
        to_meta = []
        citations = []
        with open(file, "r") as input:
            reader = csv.DictReader(input)
            for row in reader:
                populated = self.run(row)
                if populated is not None:
                    to_meta.extend(populated[0])
                    citations.append(populated[1])
        file_to_meta = file
        file_to_meta = file_to_meta.split(sep)[-1]
        file_to_meta = f"from_{file_to_meta[:-4]}_to_meta.csv"
        with open(
            join(self.tmp_dir, "meta", file_to_meta),
            "w+",
            encoding="utf8",
        ) as w:
            writer = csv.DictWriter(w, fieldnames=FIELDNAMES)
            writer.writeheader()
            for row in to_meta:
                writer.writerow(row)
        run_meta_process(self.meta_process)
        # os.remove(join(self.tmp_dir, "meta", file_to_meta))
        meta_info = []
        for dirpath, _, filenames in os.walk(join(self.meta_folder, "csv")):
            for el in filenames:
                if file_to_meta[:-4] in el:
                    meta_info = get_csv_data(join(dirpath, el))
                    os.remove(join(dirpath, el))
                    break
        with open(join(self.tmp_dir, file_to_meta), "w+") as output:
            writer = csv.DictWriter(
                output,
                fieldnames=(
                    "citing_id",
                    "citing_publication_date",
                    "cited_id",
                    "cited_publication_date",
                ),
            )
            writer.writeheader()

            for citation in citations:

                to_write = {}
                citing = meta_info[citation[0]]["id"].split("meta:")[1]
                cited = meta_info[citation[1]]["id"].split("meta:")[1]
                to_write["citing_id"] = f"meta:{citing}"
                to_write["citing_publication_date"] = meta_info[citation[0]]["pub_date"]
                to_write["cited_id"] = f"meta:{cited}"
                to_write["cited_publication_date"] = meta_info[citation[1]]["pub_date"]
                writer.writerow(to_write)
        self.id_populator.id_num = 0
        return join(self.tmp_dir, file_to_meta)

    def run(self, row: dict) -> tuple:
        """This method manages the process for the preprocessing to OC_Meta for each row of the file.
        :params row: dict to be processed
        :returns: a tuple with the populated bibliographic information and the position of citations in the IDs that are found."""
        found_ids = []
        result = []
        ids = {
            row.get("citing_id"): row.get("citing_publication_date"),
            row.get("cited_id"): row.get("cited_publication_date"),
        }
        this_citation = []
        for start_id in ids:
            id = self.id_populator.populate_ids(start_id)

            if id == None:
                return None  # if one of the ids is invalid, break the process
            elif isinstance(id, int):  # change
                this_citation.append(id)
                continue
            else:
                this_citation.append(id[1])
                id = id[0]
                id["start_id"] = start_id
                found_ids.append(id)

        for i in range(len(found_ids)):
            id = found_ids[i]  # get each id

            date = ids[id.pop("start_id")]  # get the date in input for each id

            pop_row = self.metadata_populator.launch_service(
                id
            )  # this launchs the pipeline
            pop_row["author"] = self.author_pop.get_author_info(id, pop_row)

            if pop_row.get("pub_date") is None:
                pop_row["pub_date"] = date
            # If the pipeline does not find a date, use the date given to give to Meta.
            # Else, use the date found in the pipeline. In the end, the date used will be the one validated by meta.
            result.append(pop_row)

        return result, this_citation

    def clean_dir(self) -> None:
        """
        This method cleans the temporary directory with the results, as well as the cached ids.
        """
        self.id_populator.seen_ids = {}
        if os.path.exists(join(self.tmp_dir, "meta")):
            shutil.rmtree(join(self.tmp_dir, "meta"))
            os.mkdir(join(self.tmp_dir, "meta"))


if __name__ == "__main__":
    auth_pop = AuthorPopulator()
    print(
        auth_pop.get_author_info(
            {"pmid": "19060306"},
            {
                "id": {"pmid": "19060306"},
                "author": "Shotton, David [viaf:7484794]",
                "title": "Linked data and provenance in biological data webs.",
            },
        )
    )

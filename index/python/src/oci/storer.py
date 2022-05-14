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

from os import sep
from datetime import datetime
from os.path import exists, basename, isfile
from os import makedirs, SEEK_END
from glob import glob
from re import sub
from csv import DictWriter, DictReader
from json import loads, load
from rdflib import Graph, ConjunctiveGraph
from rdflib.namespace import RDF
from urllib.parse import quote
from collections import OrderedDict

from oc.index.oci.citation import Citation


class CitationStorer(object):
    """This class manages the saving on disk of the citations extracted from index."""

    CSV_EXT = "csv"
    RDF_EXT = "ttl"
    SLX_EXT = "scholix"

    def __init__(
        self,
        dir_data_path,
        rdf_resource_base,
        n_citations_csv_file=10000000,
        n_citations_rdf_file=1000000,
        n_citations_slx_file=5000000,
        suffix="",
    ):
        """CitationStorer constructor.

        Args:
            dir_data_path (str): path to the data
            rdf_resource_base (str): path to the rdf
            n_citations_csv_file (int, optional): number of ciitations in csv file. Defaults to 10000000.
            n_citations_rdf_file (int, optional): number of ciitations in rdf file. Defaults to 1000000.
            n_citations_slx_file (int, optional): number of ciitations in slx file. Defaults to 5000000.
            suffix (str, optional): suffix, defaults to "".
        """
        self.cur_time = datetime.now().strftime("%Y-%m-%dT%H%M%S")
        self.citation_dir_data_path = dir_data_path + sep + "data" + sep
        self.citation_dir_prov_path = dir_data_path + sep + "prov" + sep
        self.csv_dir_local_path = (
            "csv" + sep + self.cur_time[:7].replace("-", sep) + sep
        )
        self.rdf_dir_local_path = (
            "rdf" + sep + self.cur_time[:7].replace("-", sep) + sep
        )
        self.slx_dir_local_path = (
            "slx" + sep + self.cur_time[:7].replace("-", sep) + sep
        )

        self.data_csv_dir = self.citation_dir_data_path + self.csv_dir_local_path
        self.data_rdf_dir = self.citation_dir_data_path + self.rdf_dir_local_path
        self.data_slx_dir = self.citation_dir_data_path + self.slx_dir_local_path
        self.prov_csv_dir = self.citation_dir_prov_path + self.csv_dir_local_path
        self.prov_rdf_dir = self.citation_dir_prov_path + self.rdf_dir_local_path

        if type(suffix) is str and suffix:
            self.suffix = "_" + suffix
        else:
            self.suffix = ""

        try:
            if not exists(self.data_csv_dir):
                makedirs(self.data_csv_dir)
            if not exists(self.data_rdf_dir):
                makedirs(self.data_rdf_dir)
            if not exists(self.data_slx_dir):
                makedirs(self.data_slx_dir)
            if not exists(self.prov_csv_dir):
                makedirs(self.prov_csv_dir)
            if not exists(self.prov_rdf_dir):
                makedirs(self.prov_rdf_dir)
        except FileExistsError:
            pass

        self.rdf_resource_base = rdf_resource_base
        self.n_citations_csv_file = n_citations_csv_file
        self.n_citations_rdf_file = n_citations_rdf_file
        self.n_citations_slx_file = n_citations_slx_file

        (
            self.cur_csv_filename,
            self.cur_csv_citations,
        ) = CitationStorer.__get_right_file_path(
            self.data_csv_dir,
            self.cur_time + self.suffix,
            CitationStorer.CSV_EXT,
            self.n_citations_csv_file,
            CitationStorer.__count_citations_csv,
        )
        (
            self.cur_rdf_filename,
            self.cur_rdf_citations,
        ) = CitationStorer.__get_right_file_path(
            self.data_rdf_dir,
            self.cur_time + self.suffix,
            CitationStorer.RDF_EXT,
            self.n_citations_rdf_file,
            CitationStorer.__count_citations_rdf,
        )
        (
            self.cur_slx_filename,
            self.cur_slx_citations,
        ) = CitationStorer.__get_right_file_path(
            self.data_slx_dir,
            self.cur_time + self.suffix,
            CitationStorer.SLX_EXT,
            self.n_citations_slx_file,
            CitationStorer.__count_citations_slx,
        )

    def get_csv_filename(self, increment=False):
        """It returns the csv filename

        Args:
            increment (bool, optional): if set as true the pointer to the current csv is updated,
            defaults to False.

        Returns:
            str: the current csv filename
        """
        if self.cur_csv_citations >= self.n_citations_csv_file:
            (
                self.cur_csv_filename,
                self.cur_csv_citations,
            ) = CitationStorer.__get_right_file_path(
                self.data_csv_dir,
                self.cur_time + self.suffix,
                CitationStorer.CSV_EXT,
                self.n_citations_csv_file,
            )

        if increment:
            self.cur_csv_citations += 1

        return self.cur_csv_filename

    def get_rdf_filename(self, increment=False):
        """It returns the rdf filename

        Args:
            increment (bool, optional): if set as true the pointer to the current rdf is updated,
            defaults to False.

        Returns:
            str: the current rdf filename
        """
        if self.cur_rdf_citations >= self.n_citations_rdf_file:
            (
                self.cur_rdf_filename,
                self.cur_rdf_citations,
            ) = CitationStorer.__get_right_file_path(
                self.data_rdf_dir,
                self.cur_time + self.suffix,
                CitationStorer.RDF_EXT,
                self.n_citations_rdf_file,
            )

        if increment:
            self.cur_rdf_citations += 1

        return self.cur_rdf_filename

    def get_slx_filename(self, increment=False):
        """It returns the slx filename

        Args:
            increment (bool, optional): if set as true the pointer to the current slx is updated,
            defaults to False.

        Returns:
            str: the current slx filename
        """
        if self.cur_slx_citations >= self.n_citations_slx_file:
            (
                self.cur_slx_filename,
                self.cur_slx_citations,
            ) = CitationStorer.__get_right_file_path(
                self.data_slx_dir,
                self.cur_time + self.suffix,
                CitationStorer.SLX_EXT,
                self.n_citations_slx_file,
            )

        if increment:
            self.cur_slx_citations += 1

        return self.cur_slx_filename

    @staticmethod
    def __count_citations_csv(s):
        return s.startswith("0")

    @staticmethod
    def __count_citations_rdf(s):
        return "<http://purl.org/spar/cito/Citation>" in s

    @staticmethod
    def __count_citations_slx(s):
        return '"RelationshipType"' in s

    @staticmethod
    def __get_right_file_path(
        base_dir, partial_file_name, file_ext, threshold, cit_counter=None
    ):
        final_index = 1

        for f_path in glob(base_dir + sep + partial_file_name + "_*." + file_ext):
            cur_index = int(
                sub("^.+_([0-9]+)\.%s$" % file_ext, "\\1", basename(f_path))
            )
            if cur_index > final_index:
                final_index = cur_index

        number_of_citations = 0
        final_file_path = (
            base_dir + sep + partial_file_name + "_" + str(final_index) + "." + file_ext
        )
        if cit_counter is not None and exists(final_file_path):
            with open(final_file_path, encoding="utf8") as f:
                for row in f:
                    if cit_counter(row):
                        number_of_citations += 1

        if cit_counter is None or number_of_citations >= threshold:
            number_of_citations = 0
            final_file_path = sub(
                "_[0-9]+\.%s$" % file_ext,
                "_%s.%s" % (str(final_index + 1), file_ext),
                final_file_path,
            )

        return basename(final_file_path), number_of_citations

    @staticmethod
    def __store_csv_on_file(f_path, header, json_obj):
        f_exists = exists(f_path)
        with open(f_path, "a", encoding="utf8") as f:
            dw = DictWriter(f, header)
            if not f_exists:
                dw.writeheader()
            dw.writerow(json_obj)

    @staticmethod
    def __store_rdf_on_file(f_path, rdf_obj, format="nt"):
        with open(f_path, "a", encoding="utf8") as f:
            rdf_string = Citation.format_rdf(rdf_obj, format)
            f.write(rdf_string)

    @staticmethod
    def __store_slx_on_file(f_path, slx_string):
        f_exists = exists(f_path)
        if not f_exists:
            with open(f_path, "w", encoding="utf8") as f:
                f.write("[")
        else:
            with open(f_path, "rb+") as f:
                f.seek(-1, SEEK_END)
                f.truncate()

        with open(f_path, "a", encoding="utf8") as f:
            if f_exists:
                f.write(",")
            f.write("\n" + slx_string + "]")

    @staticmethod
    def load_citations_from_file(
        data_f_path,
        prov_f_path=None,
        oci="04201-04201",
        baseurl="",
        service_name="CitationStorer",
        id_type="doi",
        id_shape="http://dx.doi.org/([[XXX__decode]])",
        citation_type="",
        agent="",
        source="",
    ):
        """It loads citations from csv file/s.

        Args:
            data_f_path (str): data path
            prov_f_path (str, optional): provenance files path, Defaults to None.
            oci (str, optional): str, oci defaults to "04201-04201".
            baseurl (str, optional): base url, defaults to "".
            service_name (str, optional): service name, Defaults to "CitationStorer".
            id_type (str, optional): type of data id to use, defaults to "doi".
            id_shape (str, optional): url to the id shape, defaults to "http://dx.doi.org/([[XXX__decode]])".
            citation_type (str, optional): type of the citation, defaults to "".
            agent (str, optional): agent, defaults to "".
            source (str, optional): source, defaults to "".

        Returns:
            _type_: _description_
        """
        result = []

        if exists(data_f_path) and isfile(data_f_path):
            if prov_f_path is not None and exists(prov_f_path) and isfile(prov_f_path):
                if data_f_path.endswith(
                    "." + CitationStorer.CSV_EXT
                ) and prov_f_path.endswith("." + CitationStorer.CSV_EXT):
                    result.extend(
                        CitationStorer.__load_citations_from_csv_file(
                            data_f_path,
                            prov_f_path,
                            baseurl,
                            service_name,
                            id_type,
                            id_shape,
                            citation_type,
                        )
                    )
                elif data_f_path.endswith(
                    "." + CitationStorer.RDF_EXT
                ) and prov_f_path.endswith("." + CitationStorer.RDF_EXT):
                    result.extend(
                        CitationStorer.__load_citations_from_rdf_file(
                            data_f_path,
                            prov_f_path,
                            service_name,
                            id_type,
                            id_shape,
                            citation_type,
                        )
                    )
            elif data_f_path.endswith("." + CitationStorer.SLX_EXT):
                result.extend(
                    CitationStorer.__load_citations_from_slx_file(
                        data_f_path,
                        oci,
                        service_name,
                        id_type,
                        id_shape,
                        citation_type,
                        agent,
                        source,
                    )
                )

        return result

    @staticmethod
    def __load_citations_from_csv_file(
        data_f_path,
        prov_f_path,
        baseurl,
        service_name,
        id_type,
        id_shape,
        citation_type,
    ):
        citation_data = OrderedDict()
        with open(data_f_path, encoding="utf8") as f:
            for row in DictReader(f):
                citation_data[row["oci"]] = row
        citation_prov = OrderedDict()
        with open(prov_f_path, encoding="utf8") as f:
            for row in DictReader(f):
                citation_prov[row["oci"]] = row

        for oci in citation_data:
            dent = citation_data[oci]
            pent = citation_prov[oci]
            c = Citation(
                oci,
                baseurl + quote(dent["citing"]),
                None,
                baseurl + quote(dent["cited"]),
                None,
                dent["creation"],
                dent["timespan"],
                int(pent["snapshot"]),
                pent["agent"],
                pent["source"],
                pent["created"],
                service_name,
                id_type,
                id_shape,
                citation_type,
                dent["journal_sc"] == "yes",
                dent["author_sc"] == "yes",
                pent["invalidated"],
                pent["description"],
                pent["update"],
            )

            yield c

    @staticmethod
    def __load_citations_from_rdf_file(
        data_f_path, prov_f_path, service_name, id_type, id_shape, citation_type
    ):
        citation_data = Graph()
        # Changed from load to parse since load has been deprecated
        citation_data.parse(data_f_path, format="nt11")

        citation_prov = ConjunctiveGraph()
        # Changed from load to parse since load has been deprecated
        citation_prov.parse(prov_f_path, format="nquads")

        for cit_ent in citation_data.subjects(RDF.type, Citation.citation):
            prov_entity = None
            snapshot = 0

            for entity in citation_prov.subjects(Citation.specialization_of, cit_ent):
                entity_snapshot = int(sub("^.+/se/(.+)$", "\\1", entity))
                if prov_entity is None or snapshot < entity_snapshot:
                    prov_entity = entity
                    snapshot = entity_snapshot

            invalidated = None
            update = None
            creation_date = None
            timespan = None
            for en in citation_prov.objects(prov_entity, Citation.invalidated_at_time):
                invalidated = str(en)
            for en in citation_prov.objects(prov_entity, Citation.has_update_query):
                update = str(en)
            for en in citation_data.objects(
                cit_ent, Citation.has_citation_creation_date
            ):
                creation_date = str(en)
            for en in citation_data.objects(cit_ent, Citation.has_citation_time_span):
                timespan = str(en)

            c = Citation(
                sub("^.+/ci/(.+)$", "\\1", str(cit_ent)),
                str(
                    list(citation_data.objects(cit_ent, Citation.has_citing_entity))[0]
                ),
                None,
                str(list(citation_data.objects(cit_ent, Citation.has_cited_entity))[0]),
                None,
                creation_date,
                timespan,
                entity_snapshot,
                str(
                    list(
                        citation_prov.objects(prov_entity, Citation.was_attributed_to)
                    )[0]
                ),
                str(
                    list(
                        citation_prov.objects(prov_entity, Citation.had_primary_source)
                    )[0]
                ),
                str(
                    list(
                        citation_prov.objects(prov_entity, Citation.generated_at_time)
                    )[0]
                ),
                service_name,
                id_type,
                id_shape,
                citation_type,
                Citation.journal_self_citation
                in citation_data.objects(cit_ent, RDF.type),
                Citation.author_self_citation
                in citation_data.objects(cit_ent, RDF.type),
                invalidated,
                str(list(citation_prov.objects(prov_entity, Citation.description))[0]),
                update,
            )

            yield c

    @staticmethod
    def __load_citations_from_slx_file(
        data_f_path, oci, service_name, id_type, id_shape, citation_type, agent, source
    ):
        with open(data_f_path, encoding="utf8") as f:
            citation_data = load(f)

            for obj in citation_data:
                c = Citation(
                    oci,
                    obj["Source"]["Identifier"]["IDURL"],
                    obj["Source"].get("PublicationDate"),
                    obj["Target"]["Identifier"]["IDURL"],
                    obj["Target"].get("PublicationDate"),
                    None,
                    None,
                    1,
                    agent,
                    source,
                    obj["LinkPublicationDate"],
                    service_name,
                    id_type,
                    id_shape,
                    citation_type,
                    False,
                    False,
                    None,
                    None,
                    None,
                )

                yield c

    def store_citation(self, citation):
        """It stores the citation in csv, rdf and scholix.

        Args:
            citation (index.citation.Citation): the citation to save
        """

        # Store data in CSV
        csv_filename = self.get_csv_filename(True)
        data_csv_f_path = self.data_csv_dir + csv_filename
        prov_csv_f_path = self.prov_csv_dir + csv_filename

        CitationStorer.__store_csv_on_file(
            data_csv_f_path,
            Citation.header_citation_data,
            loads(citation.get_citation_json()),
        )
        CitationStorer.__store_csv_on_file(
            prov_csv_f_path,
            Citation.header_provenance_data,
            loads(citation.get_citation_prov_json()),
        )

        # Store data in RDF
        rdf_filename = self.get_rdf_filename(True)
        data_rdf_f_path = self.data_rdf_dir + rdf_filename
        prov_rdf_f_path = self.prov_rdf_dir + rdf_filename

        CitationStorer.__store_rdf_on_file(
            data_rdf_f_path,
            citation.get_citation_rdf(self.rdf_resource_base, False, False, False),
        )
        CitationStorer.__store_rdf_on_file(
            prov_rdf_f_path,
            citation.get_citation_prov_rdf(self.rdf_resource_base),
            "nq",
        )

        # Store data in Scholix
        slx_filename = self.get_slx_filename(True)
        data_slx_f_path = self.data_slx_dir + slx_filename

        CitationStorer.__store_slx_on_file(
            data_slx_f_path, citation.get_citation_scholix()
        )

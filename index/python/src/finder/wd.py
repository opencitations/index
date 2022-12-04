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

from requests import get
from datetime import datetime
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.parse import quote
from oc.index.finder.base import ResourceFinder

BASE_QUERIES = {
    "base_info" : """SELECT (GROUP_CONCAT( ?ids; separator = ', ') as ?orcid) ?issn ?pub_date  
                    WHERE {{OPTIONAL {{ wd:{value} wdt:P123 ?publisher.
                                            ?publisher wdt:P236 ?issn}}
                                   OPTIONAL{{ {value} wdt:P577 ?date.
                                           BIND(SUBSTR(str(?date), 0, 5) as ?pub_date)}}
                                       OPTIONAL {{ {value} wdt:P50 ?author.
                                                 ?author wdt:P496 ?ids}}
                        }} group by ?issn ?pub_date"""
}

class WikidataResourceFinder(ResourceFinder):
    '''This class allows for querying Wikidata'''

    def __init__(self, data={}, use_api_service=True, api_key=None, queries = dict()):
        super().__init__(data, use_api_service)
        self.api = "https://query.wikidata.org/sparql"
        self._headers = 'ResourceFinder / OpenCitations Indexes -'
        ' (http://opencitations.net; mailto:contact@opencitations.net)'
        self.sparql = SPARQLWrapper(self.api , agent= self._headers)
        self.valid_queries = dict()
        for el in queries:
            self.valid_queries[el] = queries[el] 
        self._dm = self.__id_type_manager_class(data, use_api_service)

    
    def normalise(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        return self._dm.normalise(id_string, include_prefix=True)
        
    def get_orcid(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "orcid").split(', ')
        else:
            return self._data[id_string]["orcid"].split(', ')

    def get_pub_date(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "date")
        else:
            return self._data[id_string]["date"]

    def get_container_issn(self, id_string):
        """_summary_

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._get_item(id_string, "issn")
        else:
            return self._data[id_string]["issn"]

    def is_valid(self, id_string):
        """Validates an ID

        Args:
            id_string (_type_): _description_

        Returns:
            _type_: _description_
        """
        if not id_string in self._data or self._data[id_string] is None:
            return self._dm.is_valid(id_string)
        else:
            return self._data[id_string]["valid"]

    def _get_item(self, qid_entity, column):
        if self.is_valid(qid_entity):
            qid = self.normalise(qid_entity)

            if not qid in self._data:
                json_obj = self._call_api("base_info", value = qid)

                if json_obj is not None:
                    if column == "issn":
                        return self._get_issn(json_obj)
                    elif column == "date":
                        return self._get_date(json_obj)
                    elif column == "orcid":
                        return self._get_orcid(json_obj)
                return None

            return self._data[qid][column]
    
    def _call_api(self, to_search, **kwargs):
        query = self.valid_queries.get(to_search)
        if query is None:
            return None
        try:
            query = query.format(**kwargs)
        except:
            raise ValueError("Not enough values to complete the query")
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON) 
        response = self.sparql.query().convert()
        result = {}
        for el in response['results']['bindings']:
            for variable in el:
                if variable not in result:
                    result[variable] = el[variable]['value']
                elif el[variable].get("xml:lang") == "en":
                    result[variable] = el[variable]['value']
        return result

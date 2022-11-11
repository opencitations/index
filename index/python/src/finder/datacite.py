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

from json import loads
from urllib.parse import quote
from requests import get
import datetime
import oc.index.utils.dictionary as dict_utils
from oc.index.finder.base import ApiDOIResourceFinder


class DataCiteResourceFinder(ApiDOIResourceFinder):
    """This class implements an identifier manager for data cite identifier"""

    def __init__(self, data={}, use_api_service=True):
        """Data cite resource finder constructor."""
        super().__init__(data, use_api_service=use_api_service)
        self._api = "https://api.datacite.org/dois/"
        self.RIS_types_map = {'abst': 'abstract',
                              'news': 'newspaper article',
                              'slide': 'presentation',
                              'book': 'book',
                              'data': 'dataset',
                              'thes': 'dissertation',
                              'jour': 'journal article',
                              'mgzn': 'journal article',
                              'gen': 'other',
                              'advs': 'other',
                              'video': 'other',
                              'unpb': 'other',
                              'ctlg': 'other',
                              'art': 'other',
                              'case': 'other',
                              'icomm': 'other',
                              'inpr': 'other',
                              'map': 'other',
                              'mpct': 'other',
                              'music': 'other',
                              'pamp': 'other',
                              'pat': 'other',
                              'pcomm': 'other',
                              'catalog': 'other',
                              'elec': 'other',
                              'hear': 'other',
                              'stat': 'other',
                              'bill': 'other',
                              'unbill': 'other',
                              'cpaper': 'proceedings article',
                              'rprt': 'report',
                              'chap': 'book chapter',
                              'ser': 'book series',
                              'jfull': 'journal',
                              'conf': 'proceedings',
                              'comp': 'computer program',
                              'sound': 'audio document'}
        self.BIBTEX_types_map = {'book': 'book',
                                 'mastersthesis': 'dissertation',
                                 'phdthesis': 'dissertation',
                                 'article': 'journal article',
                                 'misc': 'other',
                                 'unpublished': 'other',
                                 'manual': 'other',
                                 'booklet': 'other',
                                 'inproceedings': 'proceedings article',
                                 'techreport': 'report',
                                 'inbook': 'book chapter',
                                 'incollection': 'book part',
                                 'proceedings': 'proceedings'}
        self.CITEPROC_types_map = {'book': 'book',
                                   'dataset': 'dataset',
                                   'thesis': 'dissertation',
                                   'article-journal': 'journal article',
                                   'article': 'other',
                                   'graphic': 'other',
                                   'post-weblog': 'web content',
                                   'paper-conference': 'proceedings article',
                                   'report': 'report',
                                   'chapter': 'book chapter',
                                   'song': 'audio document'}
        self.SCHEMAORG_types_map = {'book': 'book',
                                    'dataset': 'dataset',
                                    'thesis': 'dissertation',
                                    'scholarlyarticle': 'journal article',
                                    'article': 'journal article',
                                    'creativework': 'other',
                                    'event': 'other',
                                    'service': 'other',
                                    'mediaobject': 'other',
                                    'review': 'other',
                                    'collection': 'other',
                                    'imageobject': 'other',
                                    'blogposting': 'web content',
                                    'report': 'report',
                                    'chapter': 'book chapter',
                                    'periodical': 'journal',
                                    'publicationissue': 'journal issue',
                                    'publicationvolume': 'journal volume',
                                    'softwaresourcecode': 'computer program',
                                    'audioobject': 'audio document'}
        self.RESOURCETYPEGENERAL_types_map = {'book': 'book',
                                              'dataset': 'dataset',
                                              'dissertation': 'dissertation',
                                              'journalarticle': 'journal article',
                                              'text': 'other',
                                              'other': 'other',
                                              'datapaper': 'other',
                                              'audiovisual': 'other',
                                              'interactiveresource': 'other',
                                              'physicalobject': 'other',
                                              'event': 'other',
                                              'service': 'other',
                                              'collection': 'other',
                                              'image': 'other',
                                              'model': 'other',
                                              'peerreview': 'peer review',
                                              'conferencepaper': 'proceedings article',
                                              'report': 'report',
                                              'bookchapter': 'book chapter',
                                              'journal': 'journal',
                                              'conferenceproceeding': 'proceedings',
                                              'standard': 'standard',
                                              'outputmanagementplan': 'data management plan',
                                              'preprint': 'preprint',
                                              'software': 'computer program',
                                              'sound': 'audio document',
                                              'workflow': 'workflow'}

    def Define_Type(self, attributes):
        define_type = None
        if attributes.get('types') is not None:
            types_dict = attributes['types']
            for k, v in types_dict.items():
                if k.lower() == 'ris':
                    if type(v) is str:
                        norm_v = v.strip().lower()
                        if norm_v in self.RIS_types_map.keys():
                            define_type = self.RIS_types_map[norm_v]
                            break
                if k.lower() == 'bibtex':
                    if type(v) is str:
                        norm_v = v.strip().lower()
                        if norm_v in self.BIBTEX_types_map.keys():
                            define_type = self.BIBTEX_types_map[norm_v]
                            break
                if k.lower() == 'schemaorg':
                    if type(v) is str:
                        norm_v = v.strip().lower()
                        if norm_v in self.SCHEMAORG_types_map.keys():
                            define_type = self.SCHEMAORG_types_map[norm_v]
                            break
                if k.lower() == 'citeproc':
                    if type(v) is str:
                        norm_v = v.strip().lower()
                        if norm_v in self.CITEPROC_types_map.keys():
                            define_type = self.CITEPROC_types_map[norm_v]
                            break
                if k.lower() == 'resourcetypegeneral':
                    if type(v) is str:
                        norm_v = v.strip().lower()
                        if norm_v in self.RESOURCETYPEGENERAL_types_map.keys():
                            define_type = self.RESOURCETYPEGENERAL_types_map[norm_v]
                            break
        return define_type

    def Date_Validator(self, date_str):
        result = None
        date_text = date_str
        try:
            return datetime.datetime.strptime(date_text, "%Y-%m-%d").strftime(
                "%Y-%m-%d"
            )
        except ValueError:
            try:
                return datetime.datetime.strptime(date_text, "%Y-%m").strftime("%Y-%m")
            except ValueError:
                try:
                    return datetime.datetime.strptime(date_text, "%Y").strftime("%Y")
                except ValueError:
                    if "-" in date_text:
                        possibiliDate = date_text.split("-")
                        while possibiliDate:
                            possibiliDate.pop()
                            seperator = "-"
                            data = seperator.join(possibiliDate)
                            try:
                                return datetime.datetime.strptime(
                                    data, "%Y-%m-%d"
                                ).strftime("%Y-%m-%d")
                            except ValueError:
                                try:
                                    return datetime.datetime.strptime(
                                        data, "%Y-%m"
                                    ).strftime("%Y-%m")
                                except ValueError:
                                    try:
                                        return datetime.datetime.strptime(
                                            data, "%Y"
                                        ).strftime("%Y")
                                    except ValueError:
                                        pass
        return result

    
    def _get_orcid(self, json_obj):
        result = set()
        if json_obj:
            authors = json_obj.get("creators")
            if authors:
                for c in authors:
                    orcid_ids = [x.get("nameIdentifier") for x in c.get("nameIdentifiers") if x.get("nameIdentifierScheme") == "ORCID"]
                    if orcid_ids:
                        for orc in orcid_ids:
                            orcid = self._om.normalise(orc)
                            if orcid:
                                result.add(orcid)
        return result

    def _get_issn(self, json_obj):
        issn_set = set()
        if json_obj:
            type = self.Define_Type(json_obj)
            # Get resource ISSN
            if json_obj.get('identifiers'):
                for other_id in json_obj.get('identifiers'):
                    if other_id.get('identifier') and other_id.get('identifierType'):
                        o_id_type = other_id.get('identifierType')
                        o_id = other_id.get('identifier')
                        if o_id_type == 'ISSN':
                            if type in {'book series', 'book set', 'journal', 'proceedings series', 'series',
                                               'standard series', 'report series'}:
                                issn = self._im.normalise(o_id)
                                if issn:
                                    issn_set.add(issn)
            # Get ISSN from container
            if json_obj.get('container'):
                container = json_obj.get('container')
                if container.get("identifierType") == "ISSN":
                    if type in {'book', 'data file', 'dataset', 'edited book', 'journal article',
                                       'journal volume',
                                       'journal issue', 'monograph', 'proceedings', 'peer review', 'reference book',
                                       'reference entry', 'report'}:
                        issn = self._im.normalise(container.get("identifier"))
                        if issn:
                            issn_set.add(issn)

                    elif type == 'report series':
                        if container.get("title"):
                            issn = self._im.normalise(container.get("identifier"))
                            if issn:
                                issn_set.add(issn)
        return issn_set

    def _get_date(self, json_obj):
        if json_obj:
            dates = json_obj.get("dates")
            if dates:
                for date in dates:
                    if date.get("dateType") == "Issued":
                        cur_date = self.Date_Validator(date.get("date"))
                        if cur_date:
                            return cur_date


            cur_date = json_obj.get("publicationYear")
            if cur_date:
                cur_date = self.Date_Validator(str(cur_date))
                if cur_date:
                    return cur_date



    def _call_api(self, doi_entity):
        if self._use_api_service:
            doi = self._dm.normalise(doi_entity)
            r = get(self._api + quote(doi), headers=self._headers, timeout=30)
            if r.status_code == 200:
                r.encoding = "utf-8"
                json_res = loads(r.text)
                root = json_res.get("data")
                if root is not None:
                    return root.get("attributes")

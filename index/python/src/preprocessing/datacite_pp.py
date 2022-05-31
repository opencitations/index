from alive_progress import alive_it
import json
from os import sep, makedirs, walk
from tqdm import tqdm
import os.path
import datetime

class DatacitePreProcessing():
    """This class aims at pre-processing DataCite dumps.
    In particular, DatacitePreProcessing splits the original nldJSON in many JSON files,
    each one containing the number of entities specified in input by the user.
    Further, the class discards those entities that do not provide useful information
    (neither for the parser, nor for the glob). """

    def __init__(self):
        self._req_type = ".json"

    def valiDate(self, date_text):
        date_text = str(date_text)
        try:
            return datetime.datetime.strptime(date_text, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            try:
                return datetime.datetime.strptime(date_text, '%Y-%m').strftime('%Y-%m')
            except ValueError:
                try:
                    return datetime.datetime.strptime(date_text, '%Y').strftime('%Y')
                except ValueError:
                    if '-' in date_text:
                        possibiliDate = date_text.split('-')
                        while possibiliDate:
                            possibiliDate.pop()
                            seperator = '-'
                            data = seperator.join(possibiliDate)
                            try:
                                return datetime.datetime.strptime(data, '%Y-%m-%d').strftime('%Y-%m-%d')
                            except ValueError:
                                try:
                                    return datetime.datetime.strptime(data, '%Y-%m').strftime('%Y-%m')
                                except ValueError:
                                    try:
                                        return datetime.datetime.strptime(data, '%Y').strftime('%Y')
                                    except ValueError:
                                        pass


    def get_all_files(self, i_dir):
        result = []
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in alive_it(cur_files):
                if file.lower().endswith(self._req_type):
                    result.append(cur_dir + sep + file)
        return result

    def counter_check(self, cur_n, target_n, out_dir, dict_to_json, data_values):
        if int(cur_n) != 0 and int(cur_n) % int(target_n) == 0:
            print( "Processed lines:", cur_n, ". Reduced json nr.", cur_n //target_n)
            filename = "jSonFile_" + str(cur_n//target_n) + self._req_type
            with (open( os.path.join(out_dir, filename ), 'w', encoding="utf8" )) as json_file:
                dict_to_json["data"] = data_values
                json.dump(dict_to_json, json_file )
                empt_list = []
                empt_dict = {}
                return empt_list, empt_dict
        else:
            return data_values, dict_to_json


    def dump_filter_and_split(self, input_dir, output_dir, numJF):
        relevant_relations = ["references", "isreferencedby", "cites", "iscitedby"]
        all_files = self.get_all_files(input_dir)
        for file_idx, file in enumerate(all_files):
            data = []
            datadict = {}
            count = 0

            with open(file, "r", encoding="utf-8") as f:
                n_lines = 0
                for line in tqdm(f):
                    n_lines += 1
                    print("Processing entity n.:", n_lines)

                    linedict = json.loads(line)
                    attributes = linedict["attributes"]
                    rel_ids = attributes["relatedIdentifiers"]

                    # Default settings
                    creatorsWithOrcid = False
                    issnsFromRelId = False
                    issnsFromCont = False

                    # Check if the entity provides info for support files (DATES)
                    listDates = attributes['dates'] != []
                    publicationYear = self.valiDate(str(attributes['publicationYear']))

                    # Check if the entity provides info for support files (ORCID)
                    creators = attributes['creators']
                    if creators != []:
                        creatorsWithIds = [author for author in creators if 'nameIdentifiers' in author.keys()]
                        if creatorsWithIds != []:
                            creatorsWithIdScheme = [nameId for nameId in creatorsWithIds if 'nameIdentifier' in nameId.keys() and 'nameIdentifierScheme' in nameId.keys()]
                            if creatorsWithIdScheme != []:
                                creatorsWithOrcid = any(idInfo['nameIdentifierScheme'].lower() == "orcid" for idInfo in creatorsWithIdScheme)

                    # Check if the entity provides info for support files (ISSN)
                    if rel_ids != []:
                        idsWithType = [relId for relId in rel_ids if 'relationType' in relId.keys() and 'relatedIdentifierType' in relId.keys() and 'relatedIdentifier' in relId.keys()]
                        if idsWithType != []:
                            issnsFromRelId = [relId for relId in idsWithType if relId['relationType'].lower() == "ispartof" and relId['relatedIdentifierType'].lower() == "issn"] != []
                    elif 'container' in attributes.keys():
                        container = attributes['container']
                        if 'identifier' in container.keys() and 'identifierType' in container.keys():
                            issnsFromCont = container['identifier'] != "" and (container['identifierType']).lower() == "issn"

                    # Check if the entity provides citations
                    if rel_ids != []:
                        for i in rel_ids:
                            if "relationType" in i.keys() and (i["relationType"]).lower() in relevant_relations:
                                if "relatedIdentifierType" in i.keys() and (i["relatedIdentifierType"]).lower() == "doi":
                                    data.append(linedict)
                                    count += 1
                                    break

                                # Keep the entity if it provides at least info for the support files
                                elif creatorsWithOrcid or listDates or publicationYear or issnsFromRelId or issnsFromCont:
                                    data.append(linedict)
                                    count += 1
                                    break

                            # Keep the entity if it provides at least info for the support files
                            elif creatorsWithOrcid or listDates or publicationYear or issnsFromRelId or issnsFromCont:
                                data.append(linedict)
                                count += 1
                                break

                            data, datadict = self.counter_check(count, numJF, output_dir, datadict, data)


                    # Keep the entity if it provides at least info for the support files
                    elif creatorsWithOrcid or listDates or publicationYear or issnsFromRelId or issnsFromCont:
                        data.append(linedict)
                        count += 1


                    data, datadict = self.counter_check(count, numJF, output_dir, datadict, data)

                if len(data) > 0:
                    filename = "jSonFile_rest" + self._req_type
                    with (open( os.path.join(output_dir, filename), 'w', encoding="utf8")) as json_file:
                        datadict["data"] = data
                        json.dump(datadict, json_file)

import json
from os import makedirs, listdir
import glob
import os
from tqdm import tqdm
import os.path
from os.path import exists, join
from oc.index.preprocessing.base import Preprocessing
from oc.index.identifier.doi import DOIManager
from datetime import datetime
from argparse import ArgumentParser
import csv


class DatacitePreProcessing(Preprocessing):
    """This class aims at pre-processing DataCite dumps.
    In particular, DatacitePreProcessing splits the original nldJSON in many JSON files, each one containing the number of entities specified in input by the user. Further, the class discards those entities that are not involved in citations"""

    def __init__(self, input_dir, output_dir, output_dir_p, interval, filter=None, low_memo=True):
        self._doi_manager = DOIManager()
        self._req_type = ".json"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._output_dir_p = output_dir_p
        self._needed_info = ["relationType", "relatedIdentifierType", "relatedIdentifier"]
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        if not exists(self._output_dir_p):
            makedirs(self._output_dir_p)
        self._interval = interval
        if low_memo:
            self._low_memo = low_memo
        else:
            self._low_memo = True
        if filter:
            self._filter = filter
        else:
            self._filter = ["references", "isreferencedby", "cites", "iscitedby"]
        self._csv_col = ["citing", "referenced"]
        super(DatacitePreProcessing, self).__init__()

    def split_input(self):
        # retrieve already processed citations, if any, in order to restart an interrupted process and avoid duplicates
        processed_citations = []
        out_dir_p = listdir(self._output_dir_p)
        if len(out_dir_p) != 0:
            list_of_csv = glob.glob(join(self._output_dir_p, '*.csv'))
            for file in list_of_csv:
                with open(file, 'r') as read_obj:
                    csv_reader = csv.reader(read_obj)
                    next(csv_reader)
                    citations = list(csv_reader)
                    processed_citations.extend(citations)

        # restart from the last processed line, in case of previous process interruption
        out_dir = listdir(self._output_dir)
        # Checking if the list is empty or not
        if len(out_dir) != 0:
            list_of_files = glob.glob(join(self._output_dir, '*.json'))
            latest_file = max(list_of_files, key=os.path.getctime)
            with open(latest_file, encoding="utf8") as f:
                recover_dict = json.load(f)
                data_list = recover_dict["data"]
                last_processed_dict = data_list[-1]
                last_dict_id = last_processed_dict["id"]
                f.close()
        else:
            last_processed_dict = None

        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        len_all_files = len(all_files)
        data = []
        count = 0
        data_csv = []
        count_csv = 0

        for file_idx, file in enumerate(all_files, 1):
            if not self._low_memo:
                f = self.load_json(file, targz_fd, file_idx, len_all_files)
            else:
                f = open(file, encoding="utf8")
            for line in tqdm(f):
                if line:
                    if self._low_memo:
                        if last_processed_dict is not None:
                            if not line.startswith('{"id":"' + last_dict_id+'",') :
                                continue
                            else:
                                last_processed_dict = None
                                continue
                        else:
                            pass
                    else:
                        if last_processed_dict is not None:
                            if line.get("id") != last_dict_id:
                                continue
                            else:
                                last_processed_dict = None
                                continue
                        else:
                            pass

                    if self._low_memo:
                        try:
                            linedict = json.loads(line)
                        except:
                            print(ValueError, line)
                            continue
                    else:
                        linedict = line
                    if 'id' not in linedict or 'type' not in linedict:
                        continue
                    doi_entity = self._doi_manager.normalise(linedict['id'])
                    if linedict['type'] != "dois":
                        continue
                    attributes = linedict["attributes"]
                    rel_ids = attributes.get("relatedIdentifiers")
                    if rel_ids:
                        contains_citations = False
                        for ref in rel_ids:
                            if all(elem in ref for elem in self._needed_info):
                                relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower()
                                relationType = str(ref["relationType"]).lower()
                                if relatedIdentifierType == "doi":
                                    if relationType in self._filter:
                                        data.append(linedict)
                                        count += 1
                                        data = self.splitted_to_file(
                                            count, data, ".json"
                                        )
                                        contains_citations = True
                                        break
                        if contains_citations:
                            for ref in rel_ids:
                                if all(elem in ref for elem in self._needed_info):
                                    relatedIdentifierType = (str(ref["relatedIdentifierType"])).lower().strip()
                                    if relatedIdentifierType == "doi":
                                        rel_id = self._doi_manager.normalise(ref["relatedIdentifier"])
                                        relationType = str(ref["relationType"]).lower().strip()
                                        if relationType == "references" or relationType == "cites":
                                            cit = [str(doi_entity), str(rel_id)]
                                            if cit not in processed_citations:
                                                processed_citations.append(cit)
                                                data_csv.append(cit)
                                                count_csv += 1
                                                data_csv = self.splitted_to_file(count_csv, data_csv, ".csv")
                                        elif relationType == "isreferencedby" or relationType == "iscitedby":
                                            cit = [str(rel_id), str(doi_entity)]
                                            if cit not in processed_citations:
                                                processed_citations.append(cit)
                                                data_csv.append(cit)
                                                count_csv += 1
                                                data_csv = self.splitted_to_file(count_csv, data_csv, ".csv")

            f.close()
        if len(data) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, data, ".json")
        if len(data_csv) > 0:
            count_csv = count_csv + (self._interval - (int(count_csv) % int(self._interval)))
            self.splitted_to_file(count_csv, data_csv, ".csv")


    def splitted_to_file(self, cur_n, data, type):
        if type == ".json":
            dict_to_json = dict()
            if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0: # and len(data)
                filename = "jSonFile_" + str(cur_n // self._interval) + self._req_type
                if exists(os.path.join(self._output_dir, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
                with open(os.path.join(self._output_dir, filename), "w", encoding="utf8") as json_file:
                    dict_to_json["data"] = data
                    json.dump(dict_to_json, json_file)
                    json_file.close()
                return []
            else:
                return data
        elif type == ".csv":
            if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
                # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // self._interval)
                filename = "CSVFile_" + str(cur_n // self._interval) + ".csv"
                if exists(os.path.join(self._output_dir_p, filename)):
                    cur_datetime = datetime.now()
                    dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                    filename = filename[:-len(".csv")] + "_" + dt_string + ".csv"
                with (
                        open(os.path.join(self._output_dir_p, filename), "w", encoding="utf8", newline="")
                ) as f_out:
                    writer = csv.writer(f_out, delimiter=",")
                    writer.writerow(self._csv_col)
                    writer.writerows(data)
                return []
            else:
                return data

if __name__ == '__main__':
    arg_parser = ArgumentParser('datacite_pp.py', description='This script preprocesses a nldjson datacite dump by '
                                                              'deleting the entities which are not involved in citations'
                                                              'and storing the other ones in smaller json files')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help='Either a directory containing the decompressed json input file or the zst compressed '
                                 'json input file')
    arg_parser.add_argument('-out_g', '--output_g', dest='output_g', required=True,
                            help='Directory where the preprocessed json files will be stored (for glob)')
    arg_parser.add_argument('-out_p', '--output_p', dest='output_p', required=True,
                            help='Directory where the preprocessed csv files will be stored (for parser)')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each json file')
    arg_parser.add_argument('-f', '--filter', dest='filter', required=False,
                            help='Optional parameter, allows the user to specify a list of lowercase datacite relation'
                                 'types which will be used as a filter to decide whether or not a an entity will be'
                                 'processed. The elements of the list must be specified as a string of elements separated'
                                 'by semicolon. By default, the "filter" parameter is set to ["references", '
                                 '"isreferencedby", "cites", "iscitedby"], i.e.: the relations concerning citations.')
    arg_parser.add_argument('-lm', '--low_memo', dest='low_memo', required=False, action='store_false',
                            help='Optional parameter, True by default. Set it to False in order to load all the input'
                                 'at once instead of loading in memory each entity individually')

    args = arg_parser.parse_args()

    filter = None
    if args.filter:
        filter = args.filter.split(";")

    dcpp = DatacitePreProcessing(input_dir=args.input, output_dir=args.output_g, output_dir_p=args.output_p, interval=args.number, filter=filter, low_memo=args.low_memo)
    dcpp.split_input()
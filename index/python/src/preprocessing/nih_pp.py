from os import sep, makedirs, walk, listdir
import glob
import os.path
from os.path import exists, join
import csv
import pandas as pd
from oc.index.preprocessing.base import Preprocessing
from argparse import ArgumentParser
from datetime import datetime



class NIHPreProcessing(Preprocessing):
    """This class aims at pre-processing iCite Database Snapshots (NIH Open
    Citation Collection + ICite Metadata), available at:
    https://nih.figshare.com/search?q=iCite+Database+Snapshot. In particular,
    NIHPreProcessing splits the original CSV file in many lighter CSV files,
    each one containing the number of entities specified in input by the user.
    Further, in processing iCiteMetadata Dump, the entities which are not involved
    in citations are discarded"""

    def __init__(self, input_dir, output_dir, interval, input_type):
        self._req_type = ".csv"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._input_type = input_type
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        self._interval = interval
        if input_type == "icmd":
            self._filter = ["pmid", "doi", "title", "authors", "year", "journal", "cited_by", "references"]
        elif input_type == "occ":
            self._filter = ["citing", "referenced"]
        super(NIHPreProcessing, self).__init__()

    def splitted_to_file(self, cur_n, lines, type=None):
        if int(cur_n) != 0 and int(cur_n) % int(self._interval) == 0:
            # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // self._interval)
            filename = "CSVFile_" + str(cur_n // self._interval) + self._req_type
            if exists(os.path.join(self._output_dir, filename)):
                cur_datetime = datetime.now()
                dt_string = cur_datetime.strftime("%d%m%Y_%H%M%S")
                filename = filename[:-len(self._req_type)] + "_" + dt_string + self._req_type
            with (
                open(os.path.join(self._output_dir, filename), "w", encoding="utf8", newline="")
            ) as f_out:
                writer = csv.writer(f_out)
                writer.writerow(self._filter)
                writer.writerows(lines)
                lines = []
            return lines
        else:
            return lines

    def split_input(self):
        # restart from the last processed line, in case of previous process interruption
        out_dir = listdir(self._output_dir)
        # Checking if the list is empty or not
        if len(out_dir) != 0:
            list_of_files = glob.glob(join(self._output_dir, '*.csv'))
            latest_file = max(list_of_files, key=os.path.getctime)
            df = pd.read_csv(latest_file, low_memory=True)
            df.fillna("", inplace=True)
            df_dict_list = df.to_dict("records")
            if self._input_type == "icmd":
                last_processed_pmid = df_dict_list[-1]["pmid"]
        else:
            if self._input_type == "icmd":
                last_processed_pmid = 0

        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        count = 0
        lines = []
        for file_idx, file in enumerate(all_files, 1):
            chunksize = 100000
            with pd.read_csv(file,  usecols=self._filter, chunksize=chunksize) as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    if self._input_type == "icmd":
                        filt_values = [d.values() for d in df_dict_list if d.get("pmid") > last_processed_pmid and (d.get("cited_by") or d.get("references"))]
                    else:
                        filt_values = [d.values() for d in df_dict_list]
                    for line in filt_values:
                        count += 1
                        lines.append(line)
                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                            lines = self.splitted_to_file(count, lines)

        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines)

if __name__ == '__main__':
    arg_parser = ArgumentParser('nih_pp.py', description='This script preprocesses a NIH dump (either compressed or not,'
                                                         'either NIH-OCC or iCite Metadata) by discarding the entities '
                                                         'which are not involved in citations and storing the other ones '
                                                         'in smaller csv files')
    arg_parser.add_argument('-in', '--input', dest='input', required=True,
                            help='Either a directory containing the decompressed csv input file or the zip compressed '
                                 'csv input file')
    arg_parser.add_argument('-out', '--output', dest='output', required=True,
                            help='Directory the preprocessed CSV files will be stored')
    arg_parser.add_argument('-n', '--number', dest='number', required=True, type=int,
                            help='Number of relevant entities which will be stored in each CSV file')
    arg_parser.add_argument('-it', '--input_type', dest='input_type', required=True, choices=['icmd', 'occ'], type=str,
                            help='Type of dump to be preprocessed: choose icmd for iCiteMetadata and occ for NIH Open '
                                 'Citation collection')

    args = arg_parser.parse_args()


    nihpp = NIHPreProcessing(input_dir=args.input, output_dir=args.output, interval=args.number, input_type=args.input_type)
    nihpp.split_input()

import sqlite3
from os import sep, makedirs, walk, listdir
import glob
import os.path
from os.path import exists, join
import csv
import pandas as pd
from oc.index.preprocessing.base import Preprocessing
from argparse import ArgumentParser
from datetime import datetime
from tqdm import tqdm


class NIHPreProcessing(Preprocessing):
    """This class aims at pre-processing iCite Database Snapshots (NIH Open
    Citation Collection + ICite Metadata), available at:
    https://nih.figshare.com/search?q=iCite+Database+Snapshot. In particular,
    NIHPreProcessing splits the original CSV file in many lighter CSV files,
    each one containing the number of entities specified in input by the user"""
    def __init__(self, input_dir, output_dir, interval, input_type, db, testing=False):
        self._req_type = ".csv"
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._input_type = input_type
        self._sql_insert_cit = "INSERT INTO index_db(citing, cited) VALUES(?,?)"
        self._sql_insert_id = "INSERT INTO index_db(id) VALUES(?)"
        if not exists(self._output_dir):
            makedirs(self._output_dir)
        if self._input_type == "occ":
            if not testing:
                if exists(db):
                    self._db_conn = sqlite3.connect(db)
                    self._db_cur = self._db_conn.cursor()
                else:
                    self._db_conn = sqlite3.connect(db)
                    self._db_cur = self._db_conn.cursor()
                    self._db_cur.execute("CREATE TABLE index_db(citing text, cited text)")
                    self._db_conn.commit()
            else:
                self._db_conn = sqlite3.connect(':memory:')
                self._db_cur = self._db_conn.cursor()
                self._db_cur.execute("CREATE TABLE index_db(citing text, cited text)")
                self._db_conn.commit()

        elif self._input_type == "icmd":
            if not testing:
                if exists(db):
                    self._db_conn = sqlite3.connect(db)
                    self._db_cur = self._db_conn.cursor()
                else:
                    self._db_conn = sqlite3.connect(db)
                    self._db_cur = self._db_conn.cursor()
                    self._db_cur.execute("CREATE TABLE index_db(id text)")
                    self._db_conn.commit()
            else:
                self._db_conn = sqlite3.connect(':memory:')
                self._db_cur = self._db_conn.cursor()
                self._db_cur.execute("CREATE TABLE index_db(id text)")
                self._db_conn.commit()


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
        to_be_inserted = []
        appended_counter = 0
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
                for file in list_of_files:
                    with open(file, 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        next(csv_reader)
                        for x in csv_reader:
                            if x:
                                if not self.is_in_db(x[0]) and x[0] not in to_be_inserted:
                                    to_be_inserted.append(x[0])
                                    appended_counter += 1
                                    if appended_counter == 1000:
                                        to_be_inserted = self.insert_many_in_db(to_be_inserted)
                                    #self.insert_in_db([x[0]])
            elif self._input_type == "occ":
                for file in list_of_files:
                    with open(file, 'r') as read_obj:
                        csv_reader = csv.reader(read_obj)
                        next(csv_reader)
                        for x in csv_reader:
                            if x[0] and x[1]:
                                if not self.is_in_db(x) and x not in to_be_inserted:
                                    to_be_inserted.append(x)
                                    appended_counter += 1
                                    if appended_counter == 1000:
                                        to_be_inserted = self.insert_many_in_db(to_be_inserted)
                                #self.insert_in_db(x)

                        #citations = [tuple(x) for x in csv_reader]
                        #processed_citations.update(citations)
        else:
            if self._input_type == "icmd":
                last_processed_pmid = 0

        all_files, targz_fd = self.get_all_files(self._input_dir, self._req_type)
        count = 0
        lines = []
        for file_idx, file in enumerate(tqdm(all_files), 1):
            chunksize = 100000
            with pd.read_csv(file,  usecols=self._filter, chunksize=chunksize) as reader:
                for chunk in reader:
                    chunk.fillna("", inplace=True)
                    df_dict_list = chunk.to_dict("records")
                    if self._input_type == "icmd":
                        filt_values = [list(d.values()) for d in df_dict_list if d.get("pmid") > last_processed_pmid and (d.get("cited_by") or d.get("references"))]
                    else:
                        filt_values = [list(d.values()) for d in df_dict_list]

                    for line in filt_values:
                        if line:
                            if self._input_type == "occ":
                                if line[0] and line[1]:
                                    if not self.is_in_db(line) and line not in to_be_inserted:
        
                                        count += 1
                                        lines.append(line)
                                        if int(count) != 0 and int(count) % int(self._interval) == 0:
                                            lines = self.splitted_to_file(count, lines)
        
                                        to_be_inserted.append(line)
                                        appended_counter += 1
                                        if appended_counter == 1000:
                                            to_be_inserted = self.insert_many_in_db(to_be_inserted)
                                    #self.insert_in_db(line)
    
                            elif self._input_type == "icmd":
                                if not self.is_in_db(line[0]) and line[0] not in to_be_inserted:
                                    count += 1
                                    lines.append(line)
                                    #self.insert_in_db([line[0]])
                                    if int(count) != 0 and int(count) % int(self._interval) == 0:
                                        lines = self.splitted_to_file(count, lines)
                                    to_be_inserted.append(line[0])
                                    appended_counter += 1
                                    if appended_counter == 1000:
                                        to_be_inserted = self.insert_many_in_db(to_be_inserted)
        if len(to_be_inserted) > 0:
            self.insert_many_in_db(to_be_inserted)
            
        if len(lines) > 0:
            count = count + (self._interval - (int(count) % int(self._interval)))
            self.splitted_to_file(count, lines)

        self._db_conn.close()

    def insert_in_db(self, el):
        if self._input_type == "occ":
            with self._db_conn:
                self._db_cur.execute(self._sql_insert_cit, (el[0], el[1]))
        elif self._input_type == "icmd":
            with self._db_conn:
                self._db_cur.execute(self._sql_insert_id, (el,))

    def insert_many_in_db(self, data):
        empt_data = []
        if data:
            if self._input_type == "occ":
                data_to_tuples = [(i[0], i[1]) for i in data]
                with self._db_conn:
                    self._db_cur.executemany(self._sql_insert_cit, data_to_tuples)
            elif self._input_type == "icmd":
                data_to_tuples = [(i,) for i in data]
                with self._db_conn:
                    self._db_cur.executemany(self._sql_insert_id, data_to_tuples)
        return empt_data

    def is_in_db(self, el):
        if self._input_type == "occ":
            self._db_cur.execute("SELECT * FROM index_db WHERE citing=? AND cited=?", (el[0], el[1]))
            res = self._db_cur.fetchone()
            if not res:
                return False
            else:
                return True
        elif self._input_type == "icmd":
            self._db_cur.execute("SELECT * FROM index_db WHERE id=?", (el,))
            res = self._db_cur.fetchone()
            if not res:
                return False
            else:
                return True


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
    arg_parser.add_argument('-db', '--database', dest='database', required=True, type=str,
                            help='path to the database where already processed citations are stored')
    arg_parser.add_argument('-t', '--testing', dest='testing', required=False, choices=[True, False], default =False, type=bool,
                            help='whether or not the script is run in testing mood')

    args = arg_parser.parse_args()


    nihpp = NIHPreProcessing(input_dir=args.input, output_dir=args.output, interval=args.number, input_type=args.input_type, db=args.database, testing=args.testing)
    nihpp.split_input()
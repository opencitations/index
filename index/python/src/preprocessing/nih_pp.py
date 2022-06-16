from os import sep, makedirs, walk
import os.path
from os.path import exists
import csv
import pandas as pd


class NIHPreProcessing:
    """This class aims at pre-processing iCite Database Snapshots (NIH Open
    Citation Collection + ICite Metadata), available at:
    https://nih.figshare.com/search?q=iCite+Database+Snapshot. In particular,
    NIHPreProcessing splits the original CSV file in many lighter CSV files,
    each one containing the number of entities specified in input by the user"""

    def __init__(self):
        self._req_type = ".csv"

    def get_all_files(self, i_dir):
        result = []
        for cur_dir, cur_subdir, cur_files in walk(i_dir):
            for file in cur_files:
                if file.lower().endswith(self._req_type):
                    result.append(cur_dir + sep + file)
        return result

    def chunk_to_file(self, cur_n, target_n, out_dir, headers, lines):
        if not exists(out_dir):
            makedirs(out_dir)
        if int(cur_n) != 0 and int(cur_n) % int(target_n) == 0:
            # to be logged: print("Processed lines:", cur_n, ". Reduced csv nr.", cur_n // target_n)
            filename = "CSVFile_" + str(cur_n // target_n) + self._req_type
            with (
                open(os.path.join(out_dir, filename), "w", encoding="utf8", newline="")
            ) as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers)
                writer.writerows(lines)
                lines = []
            return lines
        else:
            # to be logged: print("Processed lines:", cur_n)
            filename = "CSVFile_" + "Rem" + self._req_type
            with (
                open(os.path.join(out_dir, filename), "w", encoding="utf8", newline="")
            ) as f_out:
                writer = csv.writer(f_out)
                writer.writerow(headers)
                writer.writerows(lines)
            return

    def dump_split(self, input_dir, output_dir, num, filter_col=None):
        all_files = self.get_all_files(input_dir)
        count = 0
        lines = []
        if filter_col is None:
            for file_idx, file in enumerate(all_files):
                with open(file, "r") as f:
                    f = csv.reader(f)
                    headers = next(f)
                    for line in f:
                        count += 1
                        lines.append(line)
                        if int(count) != 0 and int(count) % int(num) == 0:
                            lines = self.chunk_to_file(
                                count, num, output_dir, headers, lines
                            )
        else:
            for file_idx, file in enumerate(all_files):
                df = pd.read_csv(file, usecols=filter_col, low_memory=True)
                df.fillna("", inplace=True)
                f = df.values.tolist()
                headers = filter_col
                for line in f:
                    count += 1
                    lines.append(line)
                    if int(count) != 0 and int(count) % int(num) == 0:
                        lines = self.chunk_to_file(
                            count, num, output_dir, headers, lines
                        )

        if len(lines) > 0:
            self.chunk_to_file(count, num, output_dir, headers, lines)

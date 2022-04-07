from os.path import isdir, exists
from os import walk, sep
from csv import DictReader
from io import StringIO


def key_set_from_csv(csv_file_or_dir, key, line_threshold=10000):
    """It returns all the values belongs to a specific key set.

    Args:
        csv_file_or_dir (str): path to csv
        key (_type_): key from row key set
        line_threshold (int, optional): threshold on number of lines, defaults to 10000.

    Returns:
        set: containing all the values belongs to the specified key.
    """
    result = set()

    if exists(csv_file_or_dir):
        file_list = []
        if isdir(csv_file_or_dir):
            for cur_dir, cur_subdir, cur_files in walk(csv_file_or_dir):
                for cur_file in [f for f in cur_files if f.endswith(".csv")]:
                    file_list.append(cur_dir + sep + cur_file)
        elif csv_file_or_dir.endswith(".csv"):
            file_list.append(csv_file_or_dir)

        header = None
        for file in file_list:
            with open(file, encoding="utf8") as f:
                csv_content = ""
                for idx, line in enumerate(f.readlines()):
                    if header is None:
                        header = line
                        csv_content = header
                    else:
                        if idx % line_threshold == 0:
                            for row in DictReader(StringIO(csv_content), delimiter=","):
                                result.add(row[key])
                            csv_content = header
                        csv_content += line

                for row in DictReader(StringIO(csv_content), delimiter=","):
                    result.add(row[key])

    return result

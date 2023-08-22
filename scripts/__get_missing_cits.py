import argparse
import csv
import os
import time
import csv
import redis
from zipfile import ZipFile
import json
import io
from tqdm import tqdm

def remove_duplicates(input_files, entities, identifier, output):

    unique_entities = set()

    with open(entities, 'r') as infile:
        reader = csv.reader(infile)
        for row in reader:
            unique_entities.add(row[0])

    missing_cits = [["citing","cited"]]
    idx_file = 1

    if identifier == "any":
        identifier = ["doi","pmid"]
    else:
        identifier = [identifier]

    for fzip in input_files:
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                print("Working on the archive:"+str(fzip))
                print("Total number of files in archive is:"+str(len(archive.namelist())))

                # CSV header: oci,citing,cited,creation,timespan,journal_sc,author_sc
                for csv_name in tqdm(archive.namelist()):

                    if csv_name.endswith(".csv"):

                        with archive.open(csv_name) as csv_file:

                            for row in list(csv.DictReader(io.TextIOWrapper(csv_file))):

                                for e_id in identifier:
                                    e_citing = e_id+":"+row["citing"]
                                    e_cited = e_id+":"+row["cited"]

                                    if e_citing in unique_entities or e_cited in unique_entities:
                                        missing_cits.append([e_citing,e_cited])

                            if len(missing_cits) >= 1000000:
                                print("Saving missing cits in file #"+str(idx_file)+"...")
                                with open(output+str(idx_file)+'.csv', 'a+') as f:
                                    write = csv.writer(f)
                                    write.writerows(missing_cits)
                                idx_file += 1
                                missing_cits = [["citing","cited"]]

    print("Saving last missing cits in file #"+str(idx_file)+"...")
    with open(output+str(idx_file)+'.csv', 'a+') as f:
        write = csv.writer(f)
        write.writerows(missing_cits)

    print("Process Done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove duplicate rows from a CSV file.")
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The input directory/Zipfile",
    )
    parser.add_argument(
        "-e",
        "--entities",
        required=True,
        help="A CSV file listing all the entities",
    )
    parser.add_argument(
        "-id",
        "--identifier",
        required=True,
        help="doi | pmid | any",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="output name",
    )
    args = parser.parse_args()


    # input directory/file
    input_files = []
    if os.path.isdir(args.input):
        input = args.input + "/" if args.input[-1] != "/" else args.input
        for filename in os.listdir(input):
            input_files.append(os.path.join(input, filename))
    else:
        input_files.append(args.input)

    remove_duplicates(input_files, args.entities, args.identifier, args.output)

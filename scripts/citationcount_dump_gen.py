import csv
from argparse import ArgumentParser
from collections import defaultdict
from zipfile import ZipFile
import io
from tqdm import tqdm
import re
import sys
import os

# _config = get_config()

def extract_str_part(s_ttl, part = "citing"):

    g_part = None
    pattern = None

    if part == "source":
            g_part = 1
            pattern = r'<https://w3id\.org/oc/index/([A-Za-z]+)/>'

    elif part == "citing" or part == "cited":
            g_part = 1 if part == "citing" else 2
            pattern = r'<https://w3id\.org/oc/index/ci/([0-9A-Za-z]+)-([0-9A-Za-z]+)>'

    if g_part and pattern:
        match = re.search(pattern, s_ttl)
        if match:
            return match.group(g_part)

    return None

def calc_ocindex_citation_count(input_files):

    omid_cits_dict = defaultdict(set)

    for f in tqdm(input_files):
        if f.endswith("ttl"):

            with open(f, 'r') as file:
                lines = file.readlines()

            for line in lines:
                # line – EXAMPLE:
                # <https://w3id.org/oc/index/ci/06501832922-06801258849> <http://www.w3.org/ns/prov#atLocation> <https://w3id.org/oc/index/coci/> .
                if line.strip() != "":
                    citing=  extract_str_part(line, "citing")
                    cited = extract_str_part(line, "cited")
                    omid_cits_dict[cited].add(citing)

    omid_cits_count = [ [omid_cited, len(omid_cits_dict[omid_cited])] for omid_cited in omid_cits_dict ]
    return omid_cits_count


def main():
    arg_parser = ArgumentParser(description="Generate a citation count CSV dump for OpenCitations Index")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The directory storing the contatining the dump of all OpenCitations Index",
    )
    arg_parser.add_argument(
        "-t",
        "--type",
        required=True,
        default="rdf",
        help="The type of data given as input"
    )

    args = arg_parser.parse_args()
    root_dir = args.input if args.input[-1] != "/" else args.input[0:-1]

    f_type = "ttl"
    if args.input == "csv":
        f_type = "csv"


    input_files = []
    # Walk through the directory tree
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            # Check if the file has a .TTL extension
            if filename.lower().endswith(f_type):
                # Add the full path of the file to the list
                input_files.append(os.path.join(dirpath, filename))

    omid_cits_count = calc_ocindex_citation_count(input_files)

    with open("citations.csv", mode='w', newline='') as file:
        writer = csv.writer(file)
        omid_cits_count.insert(0, ["omid","citations"])
        writer.writerows(omid_cits_count)


    print("Done!")

if __name__ == "__main__":
    main()

import os
from argparse import ArgumentParser
from tqdm import tqdm

def remove_rows_with_string(input_file, output_file, strings_to_remove):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    filtered_lines = []
    for line in lines:
        remove_it = False
        for s in strings_to_remove:
            remove_it = remove_it or (s in line)
        if not remove_it:
            filtered_lines.append(line)

    with open(output_file, 'w') as file:
        file.writelines(filtered_lines)

def main():
    arg_parser = ArgumentParser(description="Generates the RDF files to be uploaded to the triplestore")
    arg_parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="The directory storing the TTL files",
    )
    arg_parser.add_argument(
        "-o",
        "--out",
        required=True,
        help="The output directorys",
    )
    args = arg_parser.parse_args()

    directory = args.input if args.input[-1] == "/" else args.input[0:-1]
    out_dir = args.out if args.out[-1] == "/" else args.out[0:-1]
    triples_to_remove = ["hasCitationCreationDate","hasCitationTimeSpan","JournalSelfCitation","AuthorSelfCitation"]
    for filename in tqdm(os.listdir(directory)):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            if filename.endswith(".ttl"):
                print("Processing file: "+filename)
                remove_rows_with_string(file_path, out_dir+"/"+filename, triples_to_remove)

    print("Done!")

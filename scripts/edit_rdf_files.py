import os
from argparse import ArgumentParser
from tqdm import tqdm

def edit_files(input_file, output_file, strings_to_remove, strings_to_replace):
    with open(input_file, 'r') as file:
        lines = file.readlines()

    filtered_lines = []
    for line in lines:
        remove_it = False
        for s in strings_to_remove:
            remove_it = remove_it or (s in line)

        if not remove_it:
            for s in strings_to_replace:
                line = line.replace(s[0],s[1])

            filtered_lines.append(line)

    with open(output_file, 'w') as file:
        file.writelines(filtered_lines)


arg_parser = ArgumentParser(description="Convert RDF files")
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
arg_parser.add_argument(
    "-c",
    "--convert",
    required=False,
    default= "index",
    help="Convert RDF files into: (1) INDEX RDF files, or (2) any [[SOURCE]] RDF files (e.g., COCI, CROCI, etc.) ",
)
args = arg_parser.parse_args()

directory = args.input if args.input[-1] == "/" else args.input[0:-1]
out_dir = args.out if args.out[-1] == "/" else args.out[0:-1]
triples_to_remove = ["hasCitationCreationDate","hasCitationTimeSpan","JournalSelfCitation","AuthorSelfCitation"]
triples_to_replace = []

conver_to = args.convert
if conver_to != "index":
    triples_to_remove = ["hasCitedEntity","hasCitingEntity"]
    triples_to_replace.append(
        (
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/cito/Citation>",
            "<http://www.w3.org/ns/prov#atLocation> <https://w3id.org/oc/index/"+conver_to.strip().lower()+"/>"
        )
    )

for filename in tqdm(os.listdir(directory)):
    file_path = os.path.join(directory, filename)
    if os.path.isfile(file_path):
        if filename.endswith(".ttl"):
            print("Processing file: "+filename)
            edit_files(file_path, out_dir+"/"+filename, triples_to_remove, triples_to_replace)

print("Done!")

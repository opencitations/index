import argparse
import os
import re

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
CITATION = "<http://purl.org/spar/cito/Citation>"
PROV_AT_LOCATION = "<http://www.w3.org/ns/prov#atLocation>"


def extract_citation_subjects(ttl_path):
    """
    Extracts subjects of rdf:type cito:Citation triples.
    """
    subjects = set()
    pattern = re.compile(r'^(<[^>]+>)\s+' + re.escape(RDF_TYPE) + r'\s+' + re.escape(CITATION))

    with open(ttl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            match = pattern.match(line)
            if match:
                subjects.add(match.group(1))

    return subjects


def process_directory(input_dir, param):
    param = param.lower()
    location_uri = f"<https://w3id.org/oc/index/{param}/>"

    for filename in os.listdir(input_dir):
        if not filename.lower().endswith(".ttl"):
            continue

        input_path = os.path.join(input_dir, filename)
        base, _ = os.path.splitext(filename)
        output_filename = f"{base}-{param}.ttl"
        output_path = os.path.join(input_dir, output_filename)

        subjects = extract_citation_subjects(input_path)

        if not subjects:
            continue

        with open(output_path, "w", encoding="utf-8") as out:
            for subj in sorted(subjects):
                out.write(f"{subj} {PROV_AT_LOCATION} {location_uri} .\n")


def main():
    parser = argparse.ArgumentParser(
        description="Create new TTL files with prov:atLocation triples for Citation subjects."
    )
    parser.add_argument(
        "-d", "--dir",
        required=True,
        help="Directory containing .ttl files"
    )
    parser.add_argument(
        "-p", "--param",
        required=True,
        help="Parameter to use in output filename and location URI (will be lowercased)"
    )

    args = parser.parse_args()
    process_directory(args.dir, args.param)


if __name__ == "__main__":
    main()

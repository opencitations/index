import argparse
import os
import re
import csv

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
CITATION = "<http://purl.org/spar/cito/Citation>"
PROV_AT_LOCATION = "<http://www.w3.org/ns/prov#atLocation>"

CI_URI_PATTERN = re.compile(r"<https://w3id.org/oc/index/ci/([^>]+)>")


def extract_citation_subjects(ttl_path):
    """
    Extract subjects of rdf:type cito:Citation triples.
    """
    subjects = set()
    pattern = re.compile(
        r'^(<[^>]+>)\s+' + re.escape(RDF_TYPE) + r'\s+' + re.escape(CITATION)
    )

    with open(ttl_path, encoding="utf-8") as f:
        for line in f:
            match = pattern.match(line.strip())
            if match:
                subjects.add(match.group(1))

    return subjects


def extract_ci_id(subject_uri):
    """
    Extract CI identifier from the subject URI.
    """
    match = CI_URI_PATTERN.match(subject_uri)
    return match.group(1) if match else None


def process_directory(input_dir, param):
    param = param.lower()
    location_uri = f"<https://w3id.org/oc/index/{param}/>"
    output_dir = os.getcwd()

    for filename in os.listdir(input_dir):
        if not filename.lower().endswith(".ttl"):
            continue

        input_path = os.path.join(input_dir, filename)
        base, _ = os.path.splitext(filename)

        ttl_out = os.path.join(output_dir, f"{base}-{param}.ttl")
        csv_out = os.path.join(output_dir, f"{base}-{param}.csv")

        subjects = extract_citation_subjects(input_path)
        if not subjects:
            continue

        # ---- Write TTL ----
        with open(ttl_out, "w", encoding="utf-8") as ttl_file:
            for subj in sorted(subjects):
                ttl_file.write(
                    f"{subj} {PROV_AT_LOCATION} {location_uri} .\n"
                )

        # ---- Write CSV ----
        with open(csv_out, "w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["citation", "source"])

            for subj in sorted(subjects):
                ci_id = extract_ci_id(subj)
                if ci_id:
                    writer.writerow([ci_id, param])


def main():
    parser = argparse.ArgumentParser(
        description="Create TTL and CSV files with prov:atLocation data for Citation entities."
    )
    parser.add_argument(
        "-d", "--dir",
        required=True,
        help="Directory containing input .ttl files"
    )
    parser.add_argument(
        "-p", "--param",
        required=True,
        help="Parameter used in output filenames and URIs (lowercased automatically)"
    )

    args = parser.parse_args()
    process_directory(args.dir, args.param)


if __name__ == "__main__":
    main()

import os
import csv
import re
import zipfile
import sys

# Regex pattern to extract citation ID and source
TTL_PATTERN = re.compile(r"<https://w3id.org/oc/index/ci/([^>]+)>.*?<https://w3id.org/oc/index/([^/]+)/>")

def process_ttl_file(file_content):
    """ Extract citation ID and source from TTL content """
    data = []
    for match in TTL_PATTERN.finditer(file_content):
        citation_id, source = match.groups()
        data.append((citation_id, source))
    return data

def process_zip(zip_path, output_dir):
    """ Extract TTL files from a ZIP, parse them, and save as CSV """
    zip_name = os.path.splitext(os.path.basename(zip_path))[0]  # Get ZIP file name without extension
    csv_file_path = os.path.join(output_dir, f"{zip_name}.csv")

    extracted_data = []

    with zipfile.ZipFile(zip_path, 'r') as z:
        for file_name in z.namelist():
            if file_name.endswith(".ttl"):  # Process only TTL files
                with z.open(file_name) as file:
                    content = file.read().decode("utf-8")
                    extracted_data.extend(process_ttl_file(content))

    # Write to CSV
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["citation", "source"])  # Write header
        writer.writerows(extracted_data)

def main():
    """ Process all ZIP files in the given input directory and save CSVs to the output directory """
    if len(sys.argv) != 3:
        print("Usage: python process_zips.py <input_directory> <output_directory>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    if not os.path.isdir(input_dir):
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    for zip_file in os.listdir(input_dir):
        if zip_file.endswith(".zip"):
            zip_path = os.path.join(input_dir, zip_file)
            print(f"Processing: {zip_file}")
            process_zip(zip_path, output_dir)

if __name__ == "__main__":
    main()

import csv
import argparse
from collections import defaultdict

parser = argparse.ArgumentParser(description='Takes a CSV containing the list of all citations in OC INDEX expressed as OMID-OMID')
parser.add_argument('--citations', required=True, help='Path to the CSV file containing the citation count in the OpenCitations INDEX expressed as OMID > [COUNT]')
parser.add_argument('--omid', required=True, help='Path to the CSV file containing a mapping of the OMID(s) in the OpenCitations INDEX expressed as OMID > ANY-ID(s), e.g. omid:br/123,doi:10.123 pmid:2345')
parser.add_argument('--id',  default='doi', help='Convert OMID(s) to a given ID')
parser.add_argument('--out', default='citation_count.csv', help='Path to the output CSV file (default: citation_count.csv)')
args = parser.parse_args()

omid_map = dict()
with open(args.omid, mode='r') as input_csvfile:
    reader = csv.reader(input_csvfile)
    for row in reader:
        if len(row) == 2:
            for any_id in row[1].split(" "):
                if any_id.startswith(args.id):
                    omid = row[0].replace("omid:","")
                    omid_map[omid] = any_id


citation_count_by_id = dict()
# Open the input and output CSV files
with open(args.citations, mode='r') as input_csvfile:
    reader = csv.reader(input_csvfile)
    for row in reader:
        if len(row) == 2:
            omid = row[0]
            if omid in omid_map:
                any_id = omid_map[omid]
                citation_count_by_id[any_id] = row[1]

with open(args.out, mode='w', newline='') as output_csvfile:
    writer = csv.writer(output_csvfile)
    writer.writerow(['id', 'citation_count'])
    for cited in cits_index:
        writer.writerow([cited,str(cits_index[cited])])

print(f'New CSV file with the citation counts "{args.output_file}" has been created.')

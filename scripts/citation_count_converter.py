import csv
import argparse
from collections import defaultdict
from zipfile import ZipFile
import io
from tqdm import tqdm
import re

csv.field_size_limit(sys.maxsize)

def re_get_ids(val, identifiers, multi_ids = False, group_ids= False):
    res = []
    items = [val]
    if multi_ids:
        items = [item for item in val.split("; ")]

    for item in items:
        re_rule = "(.*)"
        if multi_ids:
            re_rule = "\[(.*)\]"

        re_ids_container = re.search(re_rule,item)
        if re_ids_container:
            re_ids = re.findall("(("+"|".join(identifiers)+")\:\S[^\s]+)", re_ids_container.group(1))
            oids = [oid[0] for oid in re_ids]
            if group_ids:
                res.append(oids)
            else:
                for _id in oids:
                    res.append(_id)
    return res

def get_omid_map(fzip, wanted_id):
    omid_map = dict()
    with ZipFile(fzip) as archive:
        print("Total number of files in the archive is:"+str(len(archive.namelist())))
        for csv_name in tqdm(archive.namelist()):
            with archive.open(csv_name) as csv_file:

                l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))
                for o_row in l_cits:
                    omid_ids = re_get_ids(o_row["id"],["omid"])
                    if len(omid_ids) > 0:
                        omid = omid_ids[0].replace("omid:","")
                        other_ids = re_get_ids(o_row["id"], ["doi","issn","isbn","pmid","pmcid","url","wikidata","wikipedia","jid","arxiv"])
                        for any_id in other_ids:
                            if any_id.startswith(wanted_id):
                                any_id = any_id.replace(wanted_id+":","")
                                omid_map[omid] = any_id
    return omid_map


parser = argparse.ArgumentParser(description='Takes a CSV containing the list of all citations in OC INDEX expressed as OMID-OMID')
parser.add_argument('--citations', required=True, help='Path to the CSV file containing the citation count in the OpenCitations INDEX expressed as OMID > [COUNT]')
# parser.add_argument('--omid', required=True, help='Path to the CSV file containing a mapping of the OMID(s) in the OpenCitations INDEX expressed as OMID > ANY-ID(s), e.g. omid:br/123,doi:10.123 pmid:2345')
parser.add_argument('--metacsv', required=True, help='Path to the Zipped META CSV dump')
parser.add_argument('--id',  default='doi', help='Convert OMID(s) to a given ID')
parser.add_argument('--out', default='citation_count.csv', help='Path to the output CSV file (default: citation_count.csv)')
args = parser.parse_args()

omid_map = get_omid_map(args.metacsv, args.id)

c = 10
for a in omid_map:
    print(a,omid_map[a])
    c -= 1
    if c == 0:
        break


citation_count_by_id = []
# Open the input and output CSV files
with open(args.citations, mode='r') as input_csvfile:
    reader = csv.reader(input_csvfile)
    for row in reader:
        if len(row) == 2:
            omid = row[0]
            if omid.startswith("omid:"):
                omid = omid.replace("omid:","")
            if omid in omid_map:
                any_id = omid_map[omid]
                citation_count_by_id.append( (any_id,row[1]) )

with open(args.out, mode='w', newline='') as output_csvfile:
    writer = csv.writer(output_csvfile)
    writer.writerow([wanted_id, 'citation_count'])
    for c in citation_count_by_id:
        writer.writerow([c[0],str(c[1])])

print(f'New CSV file with the citation counts "{args.out}" has been created.')

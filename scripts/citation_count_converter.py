import csv
import argparse
from collections import defaultdict
from zipfile import ZipFile
import io
from tqdm import tqdm
import re
import sys
import requests

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

def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--citations', required=True, help='Path to the CSV file containing the citation count in the OpenCitations INDEX expressed as OMID > [COUNT]')
    # parser.add_argument('--omid', required=True, help='Path to the CSV file containing a mapping of the OMID(s) in the OpenCitations INDEX expressed as OMID > ANY-ID(s), e.g. omid:br/123,doi:10.123 pmid:2345')
    parser.add_argument('--metacsv', required=True, help='Path to the Zipped META CSV dump')
    parser.add_argument('--id',  default='doi', help='Convert OMID(s) to a given ID')
    parser.add_argument('--out', default='citation_count.csv', help='Path to the output CSV file (default: citation_count.csv)')
    parser.add_argument('--check', action='store_true', help='Set this param if you want a further check for duplicated entities')
    args = parser.parse_args()

    any_id_pre = args.id
    omid_map = get_omid_map(args.metacsv, any_id_pre)

    # Print a sample
    # -----
    # c = 10
    # for a in omid_map:
    #     print(a,omid_map[a])
    #     c -= 1
    #     if c == 0:
    #         break

    multi_any_ids = defaultdict(int)
    citation_count_by_id = dict()
    # Open the input and output CSV files
    with open(args.citations, mode='r') as input_csvfile:
        reader = csv.reader(input_csvfile)
        for row in reader:
            if len(row) == 2:
                omid = row[0]
                if omid.startswith("omid:"):
                    omid = omid.replace("omid:","")
                if not omid.startswith("br/"):
                    omid = "br/"+omid
                if omid in omid_map:
                    any_id = omid_map[omid]

                    cits_count = row[1]
                    if any_id in citation_count_by_id:

                        # in case this any_id was already processed we need to dissambiguate
                        # get the any_ids of all the citing entities
                        multi_any_ids[any_id] += 1

                        if args.check:
                            # call META triplestore on test.opencitations.net and get list of citations
                            url = 'https://test.opencitations.net/index/api/v2/citations/'+any_id_pre+":"+any_id
                            response = requests.get(url)

                            try:

                                l_citing = [set(cit["citing"].split(" ")) for cit in response.json()]
                                # filter only any_id
                                citings_any_id = set()
                                for citing_obj in l_citing:
                                  for k_citing in citing_obj:
                                    if k_citing.startswith(any_id_pre+":"):
                                      citings_any_id.add(k_citing.replace(any_id_pre+":",""))

                                cits_count = len(citings_any_id)

                            except:
                                pass

                    citation_count_by_id[any_id] = cits_count


    # convert it to a list of tuples
    citation_count_by_id = [(k,citation_count_by_id[k]) for k in citation_count_by_id]

    with open(args.out, mode='w', newline='') as output_csvfile:
        writer = csv.writer(output_csvfile)
        writer.writerow([args.id, 'citation_count'])
        for c in citation_count_by_id:
            writer.writerow([c[0],str(c[1])])

    print(f'New CSV file with the citation counts "{args.out}" has been created.')


    # write also duplicates
    with open(any_id_pre+"_dupilcates.csv", mode='w', newline='') as output_csvfile:
        writer = csv.writer(output_csvfile)
        l_duplicates = [ (any_id,multi_any_ids[any_id]) for any_id in multi_any_ids]
        writer.writerow([args.id, 'num_duplicates'])
        for c in l_duplicates:
            writer.writerow([c[0],str(c[1])])

    print(f'The number of duplicated entities is "{str(len(multi_any_ids.keys()))}" ')


if __name__ == "__main__":
    main()

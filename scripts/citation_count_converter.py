import csv
import argparse
from collections import defaultdict
from zipfile import ZipFile
import io
from tqdm import tqdm
import re
import sys
import requests
from time import sleep
import json
import redis

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

csv.field_size_limit(sys.maxsize)
_config = get_config()
conf_br_ids = _config.get("cnc", "br_ids").split(",")

'''
Regex to get all the IDs in the Meta CSV dump
'''
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

'''
To create the omid map using the original OC META CSV dump
'''
def get_omid_map(fzip):
    global conf_br_ids

    omid_map = dict()
    with ZipFile(fzip) as archive:
        logger.info("Total number of files in the archive is:"+str(len(archive.namelist())))
        for csv_name in tqdm(archive.namelist()):
            with archive.open(csv_name) as csv_file:

                l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))
                for o_row in l_cits:
                    omid_ids = re_get_ids(o_row["id"],["omid"])
                    if len(omid_ids) > 0:
                        omid = omid_ids[0].replace("omid:","")
                        other_ids = re_get_ids(o_row["id"], conf_br_ids)
                        omid_map[omid] = set(other_ids)
    return omid_map

'''
To create the omid map using the META BRs index (in CSV)
The META BRs index should be previously generated using 'meta2redis' command
'''
def read_omid_map(f_omidmap):
    global conf_br_ids

    omid_map = defaultdict(set)
    with open(f_omidmap, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in tqdm(csv_reader):
            if len(row) == 2:  # Ensure there are exactly two columns
                br_omid, anyids = row
                br_omid = "br/"+br_omid
                for _id in anyids.split("; "):
                    for anyid_pref in conf_br_ids:
                        if _id.startswith(anyid_pref):
                            omid_map[br_omid].add( _id )

    return omid_map

def main():
    global _config

    parser = argparse.ArgumentParser(description='Converts the citation count dump of OpenCitations Index based on BR OMIDs to any other ID (e.g., DOI, PMID)')
    parser.add_argument('--citations', required=True, help='Path to the CSV file containing the citation count in the OpenCitations INDEX expressed as OMID: [COUNT] (*Note: generated by cits2redis)')
    parser.add_argument('--redisindex', help='Redis DB storing all the citations of opencitations (*Note: populated by cits2redis)')
    parser.add_argument('--metabrs', help='Path to CSV dump containing the index/map of all BR in Meta (OMIDs) (*Note: generated by meta2redis)')
    parser.add_argument('--metacsv', help='Path to the directory containing the ZIP CSV dump of OC Meta')
    parser.add_argument('--id',  default='doi', help='Convert OMID(s) to a given ID')
    parser.add_argument('--out', default='./', help='Path to the output destination dir')
    args = parser.parse_args()
    logger = get_logger()

    anyid_pref = args.id

    # Build OMID map
    # DICT => { <OMID>: <anyid_pref>:<anyid_val> }
    logger.info("Build OMID map ...")
    if args.metabrs:
        omid_map = read_omid_map(args.metabrs)
    elif args.metacsv:
        omid_map = get_omid_map(args.metacsv)


    # Redis DB storing OC Index citations
    redis_cits = None
    if args.redisindex:
        redis_cits = redis.Redis(host='localhost', port=6379, db=args.redisindex)

    # Variables to dump
    anyids_map = defaultdict(set)
    anyid_citation_count = dict()

    # Convert OMIDs in the citation count dump
    logger.info("Convert OMIDs of the citation count dump ...")
    with open(args.citations, mode='r') as input_csvfile:
        for row in tqdm(csv.reader(input_csvfile)):
            omid = row[0]
            if omid.startswith("omid:"):
                omid = omid.replace("omid:","")
            if not omid.startswith("br/"):
                omid = "br/"+omid
            if omid in omid_map:
                s_any_id = omid_map[omid]
                cits_count = row[1]

                # check if the omid has a corresponding anyid with the wanted prefix
                any_id = None
                for __anyid in s_any_id:
                    if __anyid.startswith(anyid_pref):
                        any_id = __anyid.replace(anyid_pref+":","")
                        break

                # check in case this any_id was already processed we need to dissambiguate
                if any_id:
                    anyid_citation_count[any_id] = cits_count
                    anyids_map[any_id].add(omid.replace("br/",""))

    # Walk through duplicated ones
    logger.info("Calculate citation count of duplicated BRs ...")
    # filter those that have multiple OMIDs
    multi_any_ids = {_anyid:anyids_map[_anyid] for _anyid in anyids_map if len(anyids_map[_anyid]) > 1}

    for any_id in tqdm(multi_any_ids):
        '''
        if the DB of redis storing the citations of OpenCitations is specified use that
        otherwise, use APIs to get the citing entities
        '''
        if redis_cits:
            logger.info("Get citations form Redis for: "+str(anyid_pref+":"+any_id)+ " (omid: "+" ".join(multi_any_ids[any_id])+")" )
            citing_omids = []
            
            #__b_cits = redis_cits.mget(multi_any_ids[any_id])
            __b_cits = [_g.decode('utf-8') for _g in redis_cits.mget(multi_any_ids[any_id])]

            citing_omids = {}
            for _g in __b_cits:
                for _c in _g:
                    citing_omids.add(_c)

            l_citing_anyids = [omid_map["br/"+__c] for __c in citing_omids if "br/"+__c in omid_map]

            unique_citing_anyids = []
            for s in l_citing_anyids:
                # check the unique citing anyids
                _c_intersection = 0
                for __unique in unique_citing_anyids:
                    _c_intersection += len(__unique.intersection(s))
                # if there is no common anyids with the other citing entities
                if _c_intersection == 0:
                    unique_citing_anyids.append(s)

            cits_count = len(unique_citing_anyids)
            anyid_citation_count[any_id] = cits_count

        else:
            logger.info("Get citations via API for: "+str(anyid_pref+":"+any_id))
            try:

                # call META triplestore on test.opencitations.net and get list of citations
                url = 'https://opencitations.net/index/api/v2/citations/'+anyid_pref+":"+any_id
                response = requests.get(url)

                l_citing = [set(cit["citing"].split(" ")) for cit in response.json()]
                # filter only any_id
                citings_any_id = set()
                for citing_obj in l_citing:
                  for k_citing in citing_obj:
                    if k_citing.startswith(anyid_pref+":"):
                      citings_any_id.add(k_citing.replace(anyid_pref+":",""))

                cits_count = len(citings_any_id)

                sleep(1)
            except:
                pass


    # dump anyid - citation count
    logger.info('Saving the citation counts of '+anyid_pref+' BRs ...')
    with open(args.out+anyid_pref+"_citation_count.csv", mode='w', newline='') as output_csvfile:
        writer = csv.writer(output_csvfile)
        writer.writerow([anyid_pref, 'citation_count'])
        for c in [(k,anyid_citation_count[k]) for k in anyid_citation_count]:
            writer.writerow([c[0],str(c[1])])

    # dump duplicates
    logger.info('Saving duplicated BR entites ...')
    with open(args.out+anyid_pref+"_dupilcates.csv", mode='w', newline='') as output_csvfile:
        writer = csv.writer(output_csvfile)
        writer.writerow([anyid_pref, 'num_duplicates'])
        for c in [ (any_id,multi_any_ids[any_id]) for any_id in multi_any_ids]:
            writer.writerow([c[0],str(c[1])])


#if __name__ == "__main__":
#    main()

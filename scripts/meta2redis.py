#!python
# Copyright (c) 2023 Ivan Heibi.
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import csv
import json
from zipfile import ZipFile
import os
import datetime
import io
import argparse
from redis import Redis
import re
import sys

from tqdm import tqdm
from collections import defaultdict
from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

_config = get_config()
csv.field_size_limit(sys.maxsize)

class RedisDB(object):

    def __init__(self, redishost, redisport, redisbatchsize, _db):
        self.redisbatchsize = int(redisbatchsize)
        self.rconn = Redis(host=redishost, port=redisport, db=_db)

    def set_data(self, data, force=False):
        if len(data) >= self.redisbatchsize or force:
            for item in data:
                self.rconn.set(item[0], item[1])
            return len(data)
        return 0

# get IDs defined in the CSV dump of META using Regex
def re_get_ids(val, identifiers, multi_ids = True, group_ids= False):
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

def upload2redis(dump_path="", redishost="localhost", redisport="6379", redisbatchsize="10000", br_ids =[], ra_ids=[], db_br="10", db_ra="11", db_metadata="12"):
    global _config
    logger = get_logger()

    rconn_db_br =  RedisDB(redishost, redisport, redisbatchsize, db_br)
    rconn_db_ra = RedisDB(redishost, redisport, redisbatchsize, db_ra)
    rconn_db_metadata = RedisDB(redishost, redisport, redisbatchsize, db_metadata)

    # set buffers
    db_br_buffer = []
    db_ra_buffer = []
    db_metadata_buffer = []

    # glob indexes
    br_index = defaultdict(set)
    ra_index = defaultdict(set)

    for filename in os.listdir(dump_path):
        fzip = os.path.join(dump_path, filename)
        # checking if it is a file
        if fzip.endswith(".zip"):
            with ZipFile(fzip) as archive:
                logger.info("Total number of files in the archive is:"+str(len(archive.namelist())))
                # Each CSV file contain (i.e., CSV header):
                # "id","title","author","pub_date","venue","volume","issue","page","type","publisher","editor"
                for csv_name in archive.namelist():
                    with archive.open(csv_name) as csv_file:
                        l_cits = list(csv.DictReader(io.TextIOWrapper(csv_file)))
                        # walk through each citation in the CSV
                        logger.info("Walking through the citations of: "+str(csv_name))
                        for o_row in tqdm(l_cits):
                            #check BRs from the columns: "id" and "venue"
                            for col in ["id","venue"]:
                                # "venue" is a field with multiple ids
                                multi_ids_bool = col == "venue"
                                omid_ids = re_get_ids(o_row[col],["omid"], multi_ids = multi_ids_bool, group_ids= False)
                                if len(omid_ids) > 0:
                                    omid_br = omid_ids[0].replace("omid:","")
                                    other_ids = re_get_ids(o_row[col],br_ids, multi_ids = multi_ids_bool, group_ids= False)
                                    for oid in other_ids:
                                        db_br_buffer.append( (oid,omid_br) )
                                        br_index[oid].add(omid_br) #update glob index

                                    # add metadata only if the BR entity is in the ID column
                                    if col == "id":
                                        orcids = re_get_ids(o_row["author"],["orcid"])
                                        issns = re_get_ids(o_row["venue"],["issn"])
                                        entity_value = {
                                            "date": str(o_row["pub_date"]),
                                            "valid": True,
                                            "orcid": [a.replace("orcid:","") for a in orcids] if len(orcids) > 0 else [],
                                            "issn": [a.replace("issn:","") for a in issns] if len(issns) > 0 else []
                                        }
                                        db_metadata_buffer.append( (omid_br,json.dumps(entity_value)) )

                            #check RAs from the columns: "author","publisher", and "editor"
                            for col in ["author","publisher","editor"]:
                                for item in o_row[col].split("; "):
                                    omid_ids = re_get_ids(item,["omid"])
                                    if len(omid_ids) > 0:
                                        omid_ra = omid_ids[0].replace("omid:","")
                                        other_ids = re_get_ids(item,ra_ids)
                                        for oid in other_ids:
                                            db_ra_buffer.append( (oid,omid_ra) )
                                            ra_index[oid].add(omid_ra) #update glob index

                        logger.info("> Total BR-OMIDs found in file: "+str(len(db_br_buffer)))

                        #update redis DBs
                        if rconn_db_metadata.set_data(db_metadata_buffer) > 0:
                            db_metadata_buffer = []

                        if rconn_db_br.set_data(db_br_buffer) > 0:
                            db_br_buffer = []

                        if rconn_db_ra.set_data(db_ra_buffer) > 0:
                            db_ra_buffer = []

    # Set last data in Redis
    rconn_db_metadata.set_data(db_metadata_buffer, True)
    rconn_db_br.set_data(db_br_buffer, True)
    rconn_db_ra.set_data(db_ra_buffer, True)

    #print glob indexes to file
    logger.info("Saving global indexes...")
    with open('meta_br.csv', 'a+') as f:
        write = csv.writer(f)
        for id in br_index:
            write.writerow([id,"; ".join(br_index[id])])

    with open('meta_ra.csv', 'a+') as f:
        write = csv.writer(f)
        for id in ra_index:
            write.writerow([id,"; ".join(ra_index[id])])

    return (str(len(br_index)), str(len(ra_index)))


def main():
    global _config

    parser = argparse.ArgumentParser(description='Upload the data of META to Redis. Creates 3 DB on Redis: (DB-1)BRs in META; (DB-2)RAs in META; (DB-3)Metadata (e.g., publication date) of BRs in META')
    parser.add_argument('--dump', type=str, required=True,help='Path to the directory containing the ZIP files of the META dump')
    #parser.add_argument('--db', type=str, required=True,help='The destination DB in redis. The specified DB is used to store <any-id>:<omid> data, while DB+1 is used to store <omid>:{METADATA}')
    #parser.add_argument('--port', type=str, required=False,help='The port of redis', default="6379")

    args = parser.parse_args()
    logger = get_logger()

    logger.info("Start uploading data to Redis.")

    res = upload2redis(
        dump_path = args.dump,
        redishost = _config.get("redis", "host"),
        redisport = _config.get("redis", "port"),
        redisbatchsize = _config.get("redis", "batch_size"),
        br_ids = _config.get("cnc", "br_ids").split(","),
        ra_ids = _config.get("cnc", "ra_ids").split(","),
        db_br = _config.get("cnc", "db_br"),
        db_ra = _config.get("cnc", "db_ra"),
        db_metadata = _config.get("INDEX", "db")
    )

    logger.info("A total of unique "+str(res[0])+" BRs and "+str(res[1])+" RAs have been found and added to Redis.")

from redis import Redis
import argparse
from tqdm import tqdm
import csv

parser = argparse.ArgumentParser(description='Export a DB from Redis')
parser.add_argument('--db', type=str, required=True,help='DB to export')

args = parser.parse_args()

rconn_db = Redis(host="localhost", port="6379", db=args.db)

with open('redis_'+str(args.db)+'.csv', 'a+') as f:
    write = csv.writer(f)
    for key in tqdm(rconn_db.scan_iter()):
        val = rconn_db.get(key).decode('utf-8')
        write.writerow([key.decode('utf-8'),val])

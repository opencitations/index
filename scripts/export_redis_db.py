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
    r_buffer_keys = []
    REDIS_R_BUFFER = 100000
    for key in tqdm(rconn_db.scan_iter()):
        r_buffer_keys.append(key)
        if len(r_buffer_keys) >= REDIS_R_BUFFER:
            values = rconn_db.mget(r_buffer_keys)

            # Print the values
            for key, value in zip(r_buffer_keys, values):
                write.writerow([key.decode('utf-8'),value.decode('utf-8')])

from redis import Redis
import argparse
import csv
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Export a DB from Redis')
parser.add_argument('--db', type=str, required=True,help='DB to populate')
parser.add_argument('--csv', type=str, required=True,help='Input file in CSV format')
parser.add_argument('--key', type=str, required=True,help='column (i.e. number of column) to use as key in redis')
parser.add_argument('--value', type=str, required=True,help='column (i.e. number of column) to use as value in redis')

# The following example add the names as keys and the ages aas values into the DB=0 of Redis
# ******************
# --db 0
# --in file.csv >
#   "james,32
#   nicolas,44"
# --key 0
# --value 1

args = parser.parse_args()

r_db = Redis(host="localhost", port="6379", db=args.db)

print("Insert in redis DB="+str(args.db)+", the data in="+str(args.csv)+" .")
with open(args.csv,'r') as f:
    reader = csv.reader(f)
    w_buffer = dict()
    REDIS_W_BUFFER = 10000
    for row in tqdm(reader):
        key = row[args.key]
        value = row[args.value]
        w_buffer[key] = value
        if len(w_buffer.keys()) >= REDIS_W_BUFFER:
            r_db.mset(w_buffer)
            w_buffer = dict()

    if len(w_buffer.keys()) > 0:
        r_db.mset(w_buffer)

print("Done!")

from redis import Redis
import argparse
import csv
from tqdm import tqdm
import os

parser = argparse.ArgumentParser(description='Export a DB from Redis')
parser.add_argument('--db', type=str, required=True,help='DB to populate')
parser.add_argument('--csv', type=str, required=True,help='Input directory containing CSV files')
parser.add_argument('--key', type=str, required=True,help='column (i.e. number of column) to use as key in redis')
parser.add_argument('--value', type=str, required=True,help='column (i.e. number of column) to use as value in redis')

# ****************** <CALL EXAMPLE> ******************

# Having a CSV file (file.csv) containing something like this:
# james,32
# nicolas,44
# david,14
# ...

# Then the following call adds the names as keys and the ages as values into DB=0 of Redis
# --db 0 --in /PATH/TO/CSVS --key 0 --value 1


# ****************** </CALL EXAMPLE> ******************


args = parser.parse_args()

r_db = Redis(host="localhost", port="6379", db=args.db)

print("Insert in redis DB="+str(args.db)+", the data inside the CSVs in the directoy="+str(args.csv)+" .")
dir_csvs= args.csv
if dir_csvs[-1] != "/":
    dir_csvs = dir_csvs + "/"

for filename in os.listdir(dir_csvs):
    if filename.endswith('.csv'):
        with open(dir_csvs+filename,'r') as f:
            reader = csv.reader(f)
            w_buffer = dict()
            REDIS_W_BUFFER = 10000
            for row in tqdm(reader):
                key = row[int(args.key)]
                value = row[int(args.value)]
                w_buffer[key] = value
                if len(w_buffer.keys()) >= REDIS_W_BUFFER:
                    r_db.mset(w_buffer)
                    w_buffer = dict()

            if len(w_buffer.keys()) > 0:
                r_db.mset(w_buffer)

print("Done!")

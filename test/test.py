from re import sub
from dateutil.parser import parse
from datetime import datetime

DEFAULT_DATE = datetime(1970, 1, 1, 0, 0)

with open("/srv/data/glob/id_date.csv") as f:
    is_first = True
    for r in f.readlines():
        if is_first:
            is_first = False
        else:
            try:
                d = sub('^"[^"]+","([^,]*)"$', "\\1", r)
                citing_pub_datetime = parse(d, default=DEFAULT_DATE)
            except:
                print(r)

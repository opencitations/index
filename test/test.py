from re import sub, match
from dateutil.parser import parse
from datetime import datetime

DEFAULT_DATE = datetime(1970, 1, 1, 0, 0)


def check_date(s):
    date = sub("\s+", "", s)[:10] if s is not None else ""
    if not match("^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$", date):
        date = None
    return date


with open("/srv/data/glob/id_date.csv") as f:
    is_first = True
    for r in f.readlines():
        if is_first:
            is_first = False
        else:
            d = sub('^"[^"]+","([^,]*)"$', "\\1", r)
            parsed_d = None
            if d is not None and d != "":
                parsed_d = check_date(d)
            try:
                if parsed_d is not None and parsed_d != "":
                    parse(parsed_d, default=DEFAULT_DATE)
            except:
                print(r.strip(), d, parsed_d)

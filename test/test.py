from re import search

i = "/srv/data/glob/valid_doi.bak"
o = "/srv/data/glob/valid_doi.csv"

count = 0

with open(i) as f:
    with open(o, "w") as g:
        for r in f.readline():
            if search("\0+", r) is None:
                g.write(r)
            else:
                count += 1

print("N. of problematic rows:", count)
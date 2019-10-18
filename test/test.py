from re import sub

existing_doi = set()

with open("/srv/data/glob/id_date.csv") as f:
    with open("/srv/data/glob/id_date_nodup.csv", "w") as g:
        for r in f.readlines():
            doi = sub('^"(.+)",".*"$', "\\1", r)
            if doi not in existing_doi:
                existing_doi.add(doi)
                g.write(r)

import redis
import os
import zipfile
from typing import List
from datetime import datetime
from math import ceil

from oc.index.oci.citation import Citation

FILE_EXT = {
    "csv": "csv",
    "rdf": "ttl",
    "slx": "scholix",
}


def append_in_file(out_dest, f_id, f_type, f_content, newline=True):

    # Build the full path to the file
    dir_path = os.path.join(out_dest, f_type)
    file_path = os.path.join(dir_path, f"{f_id}.{FILE_EXT[f_type]}")

    # Ensure the directory exists
    os.makedirs(dir_path, exist_ok=True)

    #today_str = datetime.today().strftime('%Y%m%d')

    # Open the file in append mode and write content
    with open(file_path, "a", encoding="utf-8") as f:
        f.write(f_content)
        if newline:
            f.write("\n")


def zip_and_cleanup(base_dir, files_per_zip):
    subdirs = [
        "data/csv", "data/rdf", "data/slx",
        "prov/csv", "prov/rdf"
    ]

    for subdir in subdirs:
        dir_path = os.path.join(base_dir, subdir)

        # List files with non-zip extensions
        files = [f for f in os.listdir(dir_path)
                 if os.path.isfile(os.path.join(dir_path, f)) and not f.endswith(".zip")]

        # Filter files that are named as integers (before extension)
        numbered_files = []
        for f in files:
            name, _ = os.path.splitext(f)
            if name.isdigit():
                numbered_files.append((int(name), f))

        if len(numbered_files) >= files_per_zip:
            # Sort by number
            numbered_files.sort()

            # Get range
            min_num = numbered_files[0][0]
            max_num = numbered_files[-1][0]
            zip_name = f"{min_num}-{max_num}.zip"
            zip_path = os.path.join(dir_path, zip_name)

            # Create zip
            with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
                for _, filename in numbered_files:
                    file_path = os.path.join(dir_path, filename)
                    zipf.write(file_path, arcname=filename)
                    os.remove(file_path)  # Remove after adding

            print(f"Created {zip_name} in {dir_path}")



def zip_and_cleanup(file_paths: List[str], total_cits: int):
    """Zip files and remove the originals."""
    total_k = total_cits // 1000
    zip_name = f"{today_str}-{total_k}k.zip"
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for file_path in file_paths:
            zipf.write(file_path, os.path.basename(file_path))
            os.remove(file_path)
    print(f"Zipped {len(file_paths)} files ({total_cits} citations) into {zip_name}")


def process_cit_data(cit_id,m_citing,m_cited):
    """
    Returns: a dict, <key> represents the data format, and the value is the string to be writen in the file
    """
    return None


def process_cit_prov(cit_id,m_citing,m_cited):
    """
    Returns: a dict, <key> represents the data format, and the value is the string to be writen in the file
    """
    return None


def init_fs(out_dir):
    """
        Init the file system to be ready for writing the files
    """
    subdirs = [
        "data", "data/csv", "data/rdf", "data/scholix",
        "prov", "prov/csv", "prov/rdf"
    ]
    for subdir in subdirs:
        os.makedirs(os.path.join(out_dir, subdir), exist_ok=True)


def fetch_redis_values(redis_db, keys: List[str]) -> dict:
    """Fetch values for all keys from <redis_db> """
    pipeline = redis_db.pipeline()
    for key in keys:
        pipeline.get(key)
    return dict(zip(keys, pipeline.execute()))


# Citation metadata functions

def get_omid(idbase_url,id):
    return idbase_url + quote("br/"+id)

def get_date(meta_obj):
    return meta_obj["pub_date"]

def calc_journal_sc(m_citing,m_cited):
    return False

def calc_author_sc(m_citing,m_cited):
    return False


def main():
    print("Dumping all the citations of OpenCitations ...")

    # === CONF.INI ===
    idbase_url = _config.get("INDEX", "idbaseurl")
    agent = _config.get("INDEX", "agent")
    source = _config.get("INDEX", "source")
    service_name = _config.get("INDEX", "service")
    index_identifier = _config.get("INDEX", "identifier")

    # === CONFIGURATION ===
    CITING_BATCH_SIZE = 1000
    CITING_PER_FILE = 50000
    FILES_PER_ZIP = 100
    FILE_OUTPUT_DIR = 'oc_index_dump'

    # === REDIS ===
    REDIS_CITS_DB = 0
    REDIS_METADATA_DB = 1
    redis_cits = redis.Redis(host='localhost', port=6379, db=REDIS_CITS_DB, decode_responses=True)
    redis_metadata = redis.Redis(host='localhost', port=6379, db=REDIS_METADATA_DB, decode_responses=True)

    # === Process ===
    cursor = '0'
    processed_citing = 0

    # create the output directory
    init_fs(FILE_OUTPUT_DIR)

    # iterate over all the citing entities
    while True:

        # check if the number of files already created should be zipped
        zip_and_cleanup(FILE_OUTPUT_DIR, FILES_PER_ZIP)

        # get from redis first CITING_BATCH_SIZE citing entites
        cursor, keys = redis_cits.scan(cursor=cursor, count=CITING_BATCH_SIZE)
        if cursor == '0':
            break

        processed_citing += CITING_BATCH_SIZE
        # get new name to file in case <processed_citing> > CITING_PER_FILE
        file_id = processed_citing // CITING_PER_FILE

        cit_pairs = []
        brs_involved = set()

        for a in keys:
            b_list_raw = redis_cits.get(a)

            # in case <a> does not exist move to the next citing entity
            if not b_list_raw:
                continue

            # The value of <b_list_raw> is a list of cited entities, e.g. ["061302685395","062403033385" ... ]
            # update pairs and all keys
            cit_pairs.extend([(a, b) for b in json.loads(b_list_raw)])
            brs_involved.update([a] + json.loads(b_list_raw))

        # in case there is no citations mpve to next CITING_BATCH_SIZE of redis
        if len(cit_pairs) == 0:
            continue

        # Fetch metadata for all unique keys
        brs_metadata = fetch_redis_values(redis_metadata, list(brs_involved))

        # For each pair, i.e. citation, get the metadata of the _citing and _cited entity and calculate the attributes of the corresponding citation
        for citing, cited in cit_pairs:
            m_citing = brs_metadata.get(citing, "MISSING")
            m_cited = brs_metadata.get(cited, "MISSING")

            if m_citing == "MISSING" or m_cited == "MISSING":
                continue

            cit_id = citing+"-"+cited
            a_citation = Citation(
                "oci:"+cit_id, # oci,
                get_omid(idbase_url,citing), # citing_url,
                get_date(m_citing), # citing_pub_date,
                get_omid(idbase_url,cited), # cited_url,
                get_date(m_cited), # cited_pub_date,
                None, # creation,
                None, # timespan,
                1, # prov_entity_number,
                agent, # prov_agent_url,
                source, # source,
                datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat(sep="T"), # prov_date,
                service_name, # service_name,
                index_identifier, # id_type,
                idbase_url + "([[XXX__decode]])", # id_shape,
                "reference", # citation_type,
                calc_journal_sc(m_citing,m_cited), # journal_sc=False,
                calc_author_sc(m_citing,m_cited), # author_sc=False,
                None, # prov_inv_date=None,
                "Creation of the citation", # prov_description=None,
                None, # prov_update=None,
            )

            # process_cit_data() returns Data as textual value to write on CSV, RDF, and SCHOLIX files
            # process_cit_prov() returns Provenance as textual value to write on CSV and RDF files
            cits_data = process_cit_data(cit_id,m_citing,m_cited)
            cits_prov = process_cit_prov(cit_id,m_citing,m_cited)

            # dump values (data,prov) on files
            if cits_data:

                # write data value on file
                for f_type,f_content in cits_data.items():
                    append_in_file(
                        os.path.join(FILE_OUTPUT_DIR, "data"),
                        file_id,
                        f_type,
                        f_content)

                # write prov value on file
                for f_type,f_content in cits_prov.items():
                    append_in_file(
                        os.path.join(FILE_OUTPUT_DIR, "prov"),
                        file_id,
                        f_type,
                        f_content)


if __name__ == "__main__":
    main()

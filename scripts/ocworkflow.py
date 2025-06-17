import os
import sys
import subprocess
import re
from redis import Redis

from oc.index.utils.logging import get_logger
from oc.index.utils.config import get_config

logger = get_logger()

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

    def flush_db(self):
        self.rconn.flushdb()


class OCWorkflow:
    """
    Represents a workflow for processing OpenCitations data,
    including indexing and dataset conversion operations.
    """

    def __init__(
        self,
        test: bool,
        index_app: str,
        dsconverter_app: str,
        base_dir: str,
        support_dir: str,
        nproc: int,
        source: str,
        lastdump: str,
        newdump: str
    ):
        """
        Initialize the OCWorkflow object.

        Args:
            index_app (str): Path to the index application. e.g. "oc.index."
            dsconverter_app (str): Path to the dataset converter application.
            base_dir (str): Base directory for the workflow.
            support_dir (str): Directory for support files and data.
            nproc (int): Number of processors to use. e.g. 1.
            source (str): Source identifier (e.g., 'coci', 'joci'). e.g. "coci".
            lastdump (str): Date of the last dump ingested (YYYYMM). e.g. "202401".
            newdump (str): Date of the new dump (YYYYMM). e.g. "202501".
        """

        # Validate directories
        for path_name, path in {
            # "index_app": index_app,
            "dsconverter_app": dsconverter_app,
            "base_dir": base_dir,
            "support_dir": support_dir
        }.items():
            if not os.path.isdir(path):
                raise ValueError(f"{path_name} '{path}' is not a valid directory.")

        # Validate date format (YYYYMM)
        for date_label, date_val in {
            "lastdump": lastdump,
            "newdump": newdump
        }.items():
            if not re.match(r"^\d{6}$", date_val):
                raise ValueError(f"{date_label} '{date_val}' must be in 'YYYYMM' format.")

        # Get index config
        self.index_config = get_config()

        # Assign values
        self.test = test
        self.index_app = index_app
        self.dsconverter_app = dsconverter_app
        self.base_dir = base_dir
        self.support_dir = support_dir
        self.nproc = nproc
        self.source = source.lower().strip()
        self.lastdump = lastdump
        self.newdump = newdump

        # Create directories
        self.dump_dir = os.path.join(self.base_dir, "dump")
        self.ds_converter_dir = os.path.join(self.base_dir, "ds_converter")
        self.cnc_dir = os.path.join(self.base_dir, "cnc")
        os.makedirs(self.dump_dir, exist_ok=True)
        os.makedirs(self.ds_converter_dir, exist_ok=True)
        os.makedirs(self.cnc_dir, exist_ok=True)

        # Vars to update when Running
        self.source_data_dir = None
        self.ocdsconverter_meta_dir = None
        self.ocdsconverter_citations_dir = None
        self.meta_csv_dir = None

    def get_dump(self) -> None:
        """
        Download the new data dump for the configured source
        and process it (e.g., trimming for Crossref).
        Returns:
            str: Absolute path to the downloaded (and possibly processed) dump directory.
        """

        os.chdir(self.dump_dir)

        nd_year = self.newdump[:4]
        nd_month = self.newdump[4:]
        ld_year = self.lastdump[:4]
        ld_month = self.lastdump[4:]

        if self.source.lower() == "coci":
            url = f"https://api.crossref.org/snapshots/monthly/{nd_year}/{nd_month}/all.json.tar.gz"
            dump_filename = f"crossref-data-{nd_year}-{nd_month}.tar.gz"
            dump_path = os.path.join(self.dump_dir, dump_filename)

            # Auth token
            token = (
                "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9."
                "eyJpc3MiOiJodHRwOi8vY3Jvc3NyZWYub3JnLyIsImF1ZCI6Im1kcGx1cyIsImp0aSI6"
                "IjAwNWZmZGRlLWI1ZTQtNDhlNC05ZDcxLWViOGVhNTk3ZTMyNiJ9."
                "PqG8rxDXCY8JCXitX0j-i2LtozkRcrL_QUwVgqRnLuU"
            )

            # Download Crossref dump
            logger.info("Downloading Crossref dump...")
            command = f'wget --header="Crossref-Plus-API-Token: {token}" -O "{dump_path}" "{url}"'
            if not self.test:
                subprocess.run(command, shell=True, check=True)
            logger.info("Download complete!")

            # Trim Crossref dump
            # oc.index.trim_crossref -i /srv/data/coci/202504/dump/crossref-data-2025-03.tar.gz -o /srv/data/coci/202504/dump/ -m "deposited=>date-time" -v ">=:2024-10-01T00:00:00Z"
            logger.info(f"Trimming Crossref dump ({self.newdump} - {self.lastdump})...")
            self.source_data_dir = os.path.join(self.dump_dir, "trim")
            os.makedirs(self.source_data_dir, exist_ok=True)
            trim_command = (
                f'oc.index.trim_crossref '
                f'-i "{dump_path}" '
                f'-o "{self.source_data_dir}" '
                f'-m "deposited=>date-time" '
                f'-v ">=:{ld_year}-{int(ld_month)-1:02d}-01T00:00:00Z"'
            )
            if not self.test:
                subprocess.run(trim_command, shell=True, check=True)
            logger.info("Trimming complete!")


    def ds_converter(self) -> None:
        """
        Run the dataset converter for the configured source.
        Currently supports only 'crossref'.
        """

        # Validate required support files
        support_publishers = os.path.join(self.support_dir, "publishers.csv")
        if not os.path.isfile(support_publishers):
            raise ValueError(f"Support file missing: {support_publishers}")

        support_doi_orcid_index = os.path.join(self.support_dir, "doi_orcid_index.zip")
        if not os.path.isfile(support_doi_orcid_index):
            raise ValueError(f"Support file missing: {support_doi_orcid_index}")

        logger.info(f"Running the data source converter for source <{self.source}>...")

        command = [
            "python",
            "-m",
            "oc_ds_converter.run.crossref_process",
            "-cf", self.source_data_dir,
            "-out", self.ds_converter_dir,
            "-p", support_publishers,
            "-o", support_doi_orcid_index,
            "-ca", os.path.join(self.ds_converter_dir, "cache."),
            "-m", str(self.nproc),
            "-r"
        ]

        if not self.test:
            subprocess.run(command, cwd=self.dsconverter_app, check=True)
        self.ocdsconverter_meta_dir = self.ds_converter_dir
        self.ocdsconverter_citations_dir = os.path.join(self.ds_converter_dir, "_citations")
        logger.info("Data source converter complete!")


    def run_meta(self):
        self.meta_csv_dir = "/META/CSV/DUMP"

    def populate_redis(self):
        logger.info(f"Flush Redis DBs ...")
        # The following Redis DBs must be flushed:
        # + db_br | i.e. 10
        # + db_ra | i.e. 11
        # + index.db | i.e. 12
        redishost = self.index_config.get("redis", "host")
        redisport = self.index_config.get("redis", "port")
        redisbatchsize = self.index_config.get("redis", "batch_size"),
        rconn_db_br =  RedisDB(redishost, redisport, redisbatchsize, self.index_config.get("cnc", "db_br"))
        rconn_db_ra = RedisDB(redishost, redisport, redisbatchsize, self.index_config.get("cnc", "db_ra"))
        rconn_db_metadata = RedisDB(redishost, redisport, redisbatchsize, self.index_config.get("INDEX", "db"))

        rconn_db_br.flush_db()
        rconn_db_ra.flush_db()
        rconn_db_metadata.flush_db()

        logger.info(f"Running the population of Redis with the OC Meta CSV dump ...")
        # oc.index.meta2redis --dump <DIRECTORY_OF_META_CSV>
        command = [
            self.index_app+"meta2redis",
            "--dump",
            self.meta_csv_dir # The directory containing a ZIP file storing the CSV dump with the data (metadata) of OpenCitations Meta
        ]


    def gen_zipbatch(self):

        os.chdir(self.ocdsconverter_citations_dir)

        # Parameters
        batch_size = 100
        csv_files = [f for f in os.listdir('.') if f.endswith('.csv')]

        # Extract numeric part and sort files numerically
        def extract_number(filename):
            match = re.match(r"(\d+)\.csv", filename)
            return int(match.group(1)) if match else float('inf')

        csv_files.sort(key=extract_number)

        # Create zip files in batches
        for i in range(0, len(csv_files), batch_size):
            batch = csv_files[i:i + batch_size]
            if not batch:
                continue

            start = extract_number(batch[0])
            end = extract_number(batch[-1])
            zip_name = f"{start}-{end}.zip"

            with zipfile.ZipFile(zip_name, 'w') as zipf:
                for file in batch:
                    zipf.write(file)

            print(f"Created {zip_name} with files: {batch}")


## Before starting this workflow make sure:

# 1. the index software (https://github.com/opencitations/index) is installed.
#   1.1. `cd /PATH/TO/DEV/INDEX/`
#   1.2. `git clone https://github.com/opencitations/index.git`
#   1.3. `python -m venv py3venv`
#   1.4. `pip install .`

# 2. the oc_ds_converter software (https://github.com/opencitations/oc_ds_converter) is installed.
#   2.1. `cd /PATH/TO/DEV/OC_DS_CONVERTER/`
#   2.2. `git clone https://github.com/opencitations/oc_ds_converter`

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and prepare Crossref dumps.")
    parser.add_argument("--test", action='store_true', default=False, help="Enable testing the software (no proc will run)")
    parser.add_argument("--source", required=True, help="Data source (e.g., crossref)")
    parser.add_argument("--lastdump", required=True, help="Previous dump ID (e.g., 202401)")
    parser.add_argument("--newdump", required=True, help="New dump ID (e.g., 202504)")
    parser.add_argument("--dumpurl", required=False, help="Dump URL to download (if needed)")
    parser.add_argument("--nproc", default=1, type=int, help="Number of parallel processes")
    parser.add_argument("--appdsconverter", default="/srv/dev/oc_ds_converter", help="The oc_ds_converter application")
    parser.add_argument("--appindex", default="oc.index.", help="The index application")
    parser.add_argument("--supportdir", default="/srv/data/meta/support", help="Dir path storing support data")

    args = parser.parse_args()

    # Construct the base directory from the newdump
    base_dir = os.path.abspath(f"./{args.newdump}")
    os.makedirs(base_dir, exist_ok=True)

    oc_workflow = OCWorkflow(
        test = args.test,
        index_app=args.appindex,
        dsconverter_app=args.appdsconverter,
        base_dir=base_dir,
        support_dir=args.supportdir,
        nproc=args.nproc,
        source=args.source,
        lastdump=args.lastdump,
        newdump=args.newdump
    )

    # Get the source data dump
    oc_workflow.get_dump()

    # Run oc_ds_converter on the source data dump after normalizing them
    oc_workflow.ds_converter()

    # Run the meta software and create a new Meta dump
    # this process returns a CSV dump of the new OC Meta distributions
    oc_workflow.run_meta()

    # Populate Redis with the data of Meta
    oc_workflow.populate_redis()

    # Create ZIP batches of the citations (CSVs)
    oc_workflow.gen_zipbatch()

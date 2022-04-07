from asyncio.log import logger
import json
from setuptools import setup, find_packages
from setuptools.command.install import install

import os

# Parse requirements from requirements.txt
requirements = []
for line in open("requirements.txt"):
    if line and not line.startswith("#"):
        requirements.append(line)
with open("README.md", "r") as f:
    long_description = f.read()

index_conf = os.path.expanduser("~/.opencitations/index/logs")
if not os.path.exists(index_conf):
    os.makedirs(index_conf)

default_conf = {
    "runtimes": {
        "farm": "index.runtime.parallel_farm:ParallelFarm",
        "ray": "index.runtime.parallel_ray:ParallelRay",
        "sequential": "index.runtime.sequential:Sequential",
    },
    "parser": {
        "coci": "index.parser.coci.crossref_parser:CrossrefParser",
        "croci": "index.parser.croci.crowdsourced_parser:CrowdsourcedParser",
        "oaoci": "index.parser.oaoci.scholix_parser:ScholixParser",
    },
    "base": {
        "idbaseurl": "http://dx.doi.org/",
        "orcid": "",
        "lookup": "",
        "agent": "https://w3id.org/oc/index/prov/pa/1",
        "no_api": "store_true",
    },
    "file_driver": {
        "doi_file": "/path/to/doi_file.csv",
        "date_file": "/path/to/date_file.csv",
        "orcid_file": "/path/to/orcid_file.csv",
        "issn_file": "/path/to/issn_file.csv",
    },
    "mysql_driver": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "password",
        "db": "cnc",
        "tables": {
            256: ["doi_256", "oci_256"],
            512: ["doi_512", "oci_512"],
            1024: ["doi_1024", "oci_1024"],
            2048: ["doi_2048", "oci_2048"],
        },
    },
    "farm": {
        "kafka": {"host": "localhost", "port": 9091},
    },
}

config_file = os.path.expanduser("~/.opencitations/index/config.json")
if not os.path.exists(config_file):
    with open(config_file, "w") as config_file:
        json.dump(default_conf, config_file, indent=4)

setup(
    name="index",
    version="1.0",
    description="Open Citation Tool for creating new citations",
    license="CC0",
    long_description=long_description,
    author="OpenCitations",
    author_email="tech@open-citations.com",
    url="http://www.opencitations.com",
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        "console_scripts": ["cnc=index.cnc:main"],
    },
)

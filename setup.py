import os
import shutil

from setuptools import setup, find_packages

# Create ocindex dir if does not exists
ocindex_dir = os.path.expanduser(os.path.join("~", ".opencitations", "index"))
if not os.path.exists(ocindex_dir):
    os.makedirs(os.path.join(ocindex_dir, "logs"))

# If configuration file does not exists, copy the default one
config_file = os.path.join(ocindex_dir, "config.ini")
if not os.path.exists(config_file):
    shutil.copy(os.path.join(".", "config.ini"), config_file)

# If lookup file does not exists, copy the default one
lookup_file = os.path.join(ocindex_dir, "lookup.csv")
if not os.path.exists(lookup_file):
    shutil.copy(os.path.join(".", "lookup.csv"), lookup_file)

python_source_dir = os.path.join(".", "index", "python", "src")
with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="oc-index",
    version="1.0.0",
    description="Software for creating all the OpenCitations indexes",
    author="OpenCitations",
    long_description=long_description,
    author_email="tech@opencitations.net",
    url="https://www.python.org/sigs/distutils-sig/",
    package_dir={"oc.index": python_source_dir, "oc.index.scripts": "./scripts"},
    install_requires=[
        "redis",
        "SPARQLWrapper",
        "rdflib",
        "requests",
        "python-dateutil",
        "tqdm",
        "beautifulsoup4",
        "pandas",
        "lxml",
        "regex",
        "zstandard",
        "oc-idmanager"
    ],
    packages=[
        f"oc.index.{mod}"
        for mod in find_packages(where=python_source_dir, exclude="./test_data")
    ]
    + ["oc.index.scripts"],
    exclude_package_data={"": ["./test_data"]},
    entry_points={
        "console_scripts": [

            # main operations
            "oc.index.oci=oc.index.scripts.oci:main",
            "oc.index.cnc=oc.index.scripts.cnc:main",

            # redis operations
            "oc.index.redis.meta2redis=oc.index.scripts.redis.meta2redis:main",
            "oc.index.redis.cits2redis=oc.index.scripts.redis.cits2redis:main",

            # util operations
            "oc.index.util.citscount2anyid=oc.index.scripts.util.anyid_citation_count:main",
            "oc.index.util.edit_rdf=oc.index.scripts.util.edit_rdf:main",

            # crossref operations
            "oc.index.crossref.trim_crossref=oc.index.scripts.crossref.trim_crossref:main",
            "oc.index.metadata.crossref.metadata_crossref=oc.index.scripts.crossref.metadata_crossref:main",

            # upload operations
            "oc.index.upload.internet_archive=oc.index.scripts.upload.internet_archive:main",
        ],
    },
)

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
            "oc.index.gen_index_rdf=oc.index.scripts.gen_index_rdf:main",
            "oc.index.meta2redis=oc.index.scripts.meta2redis:main",
            "oc.index.normalise=oc.index.scripts.normalise_citations:main",
            "oc.index.normalise_dump=oc.index.scripts.normalise_dump:main",
            "oc.index.check_entities=oc.index.scripts.check_entities:main",
            "oc.index.cnc=oc.index.scripts.cnc:main",
            "oc.index.datasource=oc.index.scripts.datasource:main",
            "oc.index.glob.crossref=oc.index.scripts.glob_crossref:main",
            "oc.index.glob.doci=oc.index.scripts.glob_doci:main",
            "oc.index.glob.noci=oc.index.scripts.glob_noci:main",
            "oc.index.trim_crossref=oc.index.scripts.trim_crossref:main",
            "oc.index.validate=oc.index.scripts.validate_citations:main",
            "oc.index.metadata.crossref=oc.index.scripts.metadata_crossref:main",
            "oc.index.oci=oc.index.scripts.oci:main",
        ],
    },
)

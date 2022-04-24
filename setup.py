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
    name="OpenCitations Index",
    version="1.0.0",
    description="Software for creating all the OpenCitations indexes",
    author="OpenCitations",
    long_description=long_description,
    author_email="tech@opencitations.net",
    url="https://www.python.org/sigs/distutils-sig/",
    package_dir={"oc.index": python_source_dir, "oc.index.scripts": "./scripts"},
    packages=[
        f"oc.index.{mod}"
        for mod in find_packages(where=python_source_dir, exclude="./test_data")
    ]
    + ["oc.index.scripts"],
    exclude_package_data={"": ["./test_data"]},
    entry_points={
        "console_scripts": [
            "cnc=oc.index.scripts.cnc:main",
            "ocds=oc.index.scripts.ocds:main",
            "glob_crossref=oc.index.scripts.crossref_glob:main",
            "trim_crossref=oc.index.scripts.crossref_trim:main",
            "checkmetadata_crossref=oc.index.scripts.crossref_checkmetadata:main",
            "oci=oc.index.scripts.oci:main",
        ],
    },
)

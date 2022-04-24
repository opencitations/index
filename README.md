[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
# OpenCitations: Index

This software allows the creation of indexes of open citations (e.g. COCI).

**Branch status:**

[![Python package](https://github.com/opencitations/index/actions/workflows/python-package.yml/badge.svg?branch=farm_revision)](https://github.com/opencitations/index/actions/workflows/python-package.yml)

## Requirements
- Python 3.7+
- Redis, see [Installing Redis](https://redis.io/docs/getting-started/installation/) for additional information.
- libzip, see [INSTALL.md](https://github.com/nih-at/libzip/blob/master/INSTALL.md) from official repository for additional information.
## Install
To install the index software you must first meet all the requirements above.

1. First of all let's compile the cpp source files for the OCI lookup management
```console
$ make build
```
2. Then install the binaries and add them to the $PATH
```console
$ sudo make install
```
3. As a last step the python package must be installed, at the same time all the python scripts will be added to the $PATH
```console
$ pip install .
```

4. To ensure that the installation has been carried out correctly start the tests
```console
$ python -m unittest discover -s ./index/python/test -p "test_*.py"
```

Done, enjoy :)

## Configuration
By default when index is installed a configuration file is created in ~/.opencitations/index/, namely config.ini. For more information about the semantics of the settings see [config.ini](config.ini). Note, by default no value is assigned to the lookup setting, for run cnc the path to the table to be used must be indicated.
## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
OpenCitations Index is released under the [CC0 License](LICENSE).

[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net) [![Python package](https://github.com/opencitations/index/actions/workflows/python-package.yml/badge.svg?branch=farm_revision)](https://github.com/opencitations/index/actions/workflows/python-package.yml)
# OpenCitations: Index

This software allows the production of the OpenCitations index
## Requirements
### Mandatory
- Python 3.7+
- libzip, see [INSTALL.md](https://github.com/nih-at/libzip/blob/master/INSTALL.md) from official repository for additional information.
### Optional
- Redis (* mandatory for redis data source), see [Installing Redis](https://redis.io/docs/getting-started/installation/) for additional information.
## Install
To install the index software you must first meet all the requirements above.
Then the python package must be installed, this adds all the python scripts to the $PATH
```console
$ pip install .
```
## Configuration
By default when index is installed a configuration file is created in ~/.opencitations/index/, namely config.ini. For more information about the semantics of the settings see [config.ini](config.ini).

## Usage
See [Usage](USAGE.md).

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
OpenCitations Index is released under the [ISC License](LICENSE).

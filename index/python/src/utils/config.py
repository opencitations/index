import configparser

from os.path import expanduser, join
import threading

_config = None
_config_lock = threading.Lock()


def _load_config():
    global _config
    _config = configparser.ConfigParser()
    _config.read(expanduser(join("~", ".opencitations", "index", "config.ini")))


def get_config():
    """It returns ocindex configuration."""
    global _config

    if not _config:
        _config_lock.acquire()
        _load_config()
        _config_lock.release()

    return _config

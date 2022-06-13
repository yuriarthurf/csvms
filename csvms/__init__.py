""" CSVMS Module """
__version__ = '0.1.3'
import logging
from os import environ
from pathlib import Path

logging.basicConfig()
logger = logging.getLogger("CSVMS")
logger.setLevel(getattr(logging, environ.get('LOG_LEVEL', 'INFO')))
logger.info("version:%s", __version__)

DEFAULT_DB = environ.get('CSVMS_DEFAULT_DB', "default")
FILE_DIR = Path(environ.get('CSVMS_FILE_DIR', 'data'))
DICT_PATH = environ.get('CSVMS_CATALOG', 'catalog.json')

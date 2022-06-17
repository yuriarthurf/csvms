""" CSVMS Module """
__version__ = '0.2.2'
import logging
from os import environ

logging.basicConfig()
logger = logging.getLogger("CSVMS")
logger.setLevel(getattr(logging, environ.get('LOG_LEVEL', 'INFO')))
logger.info("version:%s", __version__)

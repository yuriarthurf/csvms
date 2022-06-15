"""CSVMS Exception classes"""
from csvms import logger

class DefaultException(Exception):
    """Base class for exceptions"""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)
        logger.error(*args)

class TableException(DefaultException):
    """Base class for Table exceptions"""

class ColumnException(DefaultException):
    """Base class for Column exceptions"""
    
class DataException(DefaultException):
    """Base class for Data exceptions"""

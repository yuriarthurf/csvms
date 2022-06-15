""" Database Module """
from pathlib import Path
from os import makedirs
# Local module
from csvms import logger, FILE_DIR
from csvms.catalog import Catalog

class Database():
    """The database is an file system directory
       Used to store table data files on the local file system"""

    def __init__(self, name:str, temp=False) -> "Database":
        """Database representation
        :param name: Database identifier
        :param temp: If false, create directory. Default False
        """
        self.catalog = Catalog()
        self.name = name
        self.location = str()
        if not temp:
            self.location = Database.create_location(name)

    @classmethod
    def create_location(cls, location:str) -> Path:
        """Create directory
        :param location: Filesystem path. Default directory is "database"
        :return: Location path
        """
        _path = Path(FILE_DIR.joinpath(location))
        try:
            makedirs(_path)
            logger.debug("create:path:%s",_path)
        except OSError:
            logger.debug("Directory %s already exists", location)
        return _path

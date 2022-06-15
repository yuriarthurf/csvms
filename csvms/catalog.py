"""Catalog Module"""
import json
from os import makedirs

# Local module
from csvms import logger, FILE_DIR, DICT_PATH

class Catalog():
    """Represents objects manage by the system"""
    def __init__(self) -> "Catalog":
        self.location = f"{FILE_DIR}/{DICT_PATH}"
        self.objects = list() # List of all objects in the catalog
        try:
            with open(file=self.location, mode="r", encoding="utf-8") as infile:
                self.objects = json.load(infile)
        except FileNotFoundError:
            logger.info("creating new data dictionary in %s", self.location)
            makedirs(FILE_DIR, exist_ok=True)
            self.objects = dict()
            self._save_()

    def _save_(self):
        """Save data dictionary on disk"""
        with open(file=self.location, mode="w", encoding="utf-8") as outfile:
            json.dump(self.objects, outfile)

""" Database catalog """
import json
import re
from csv import reader, writer
from os import makedirs, remove
from os.path import exists
from pathlib import Path
from typing import List, Dict

# Local module
from csvms import logger, FILE_DIR, DEFAULT_DB

# Supported data types
dtypes = {
    "string":str,
    "str":str,
    "text":str,
    "int":int,
    "integer":int,
    "float":float
}
# Supported operations
operations = {
    'lt' :lambda x,y:x < y  ,
    'gt' :lambda x,y:x > y  ,
    'eq' :lambda x,y:x == y ,
    'lte':lambda x,y:x <= y ,
    'gte':lambda x,y:x >= y ,
    'neq':lambda x,y:x != y ,
    'is' :lambda x,y:x is y ,
    'in' :lambda x,y:x in y ,
    'or' :lambda x,y:x or y ,
    'and':lambda x,y:x and y,
}
# Supported operations reverse
strtypes = {value:key for key, value in dtypes.items()}

class Database():
    """The database is an file system directory used to store table data files on the local file system"""
    def __init__(self, name:str, temp=False) -> "Database":
        """Database representation
        :param name: Database identifier
        :param temp: If false, create directory. Default False
        """
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

class Table():
    """Represents a collection of tuples as table"""
    FORMAT="csv" # Data file format
    CSVSEP=";"   # Separator

    def __init__(self, name:str, columns:Dict[str,type], data:List[tuple]=None, temp:bool=False):
        """Table representation and the data file using database location path to store all rows
        :param name: Table identifier composed by database name and the table name separated by '.'. If the database name was omitted, uses the default database instead
        :param columns: Dictionary with columns names and data types
        :param data: Load table tuples into table rows. If 'None' load from data file. Default is 'None'
        :param temp: If 'False' create datafile, other else the rows will be available only on python memory. Default 'False'
        """
        _db = DEFAULT_DB
        self.name = name
        if name.find('.') != -1:
            _db, self.name = name.split('.')
        self.database = Database(_db, temp)
        self.columns = columns
        self.rows = list()
        if data is not None:
            self.rows = data
        else:
            if exists(self.location):
                self.rows = list(self.load_csv())
            elif not temp:
                logger.debug("create:%s",self.location)
                Path(self.location).touch()

    @property
    def full_name(self):
        """Return table full name identifier"""
        return f"{self.database.name}.{self.name}"

    @property
    def definition(self) -> dict:
        """Return table definition as dictionary"""
        return dict(
            name=self.full_name,
            columns = {key: strtypes[val] for key, val in self.columns.items()}
        )

    @property
    def location(self) -> str:
        """Return table location on file system as string"""
        return f"{self.database.location}/{self.name}.{Table.FORMAT}"

    @property
    def empty_row(self) -> tuple:
        """Return an tuple with 'None' values for each column"""
        return tuple([None for _ in self.columns])

    def load_csv(self) -> List[tuple]:
        """Load csv file from path with column formats
        :return: Tuple iterator
        """
        with open(self.location, mode='r', encoding="utf-8") as csv_file:
            csv_reader = reader(csv_file, delimiter=Table.CSVSEP)
            for raw in csv_reader:
                row = list()
                for idx, col in enumerate(self.columns.values()):
                    row.append(col(raw[idx]))
                yield tuple(row)

    def save(self) -> bool:
        """Write data to file system"""
        with open(self.location, mode='w', encoding="utf-8") as csv_file:
            csv_writer = writer(csv_file, delimiter=Table.CSVSEP, quotechar='"')
            for row in self.rows:
                csv_writer.writerow(row)
        return True

    def drop(self) -> bool:
        """Remove data from file system"""
        remove(self.location)
        return True

    def show(self) -> str:
        """Print pretty table data"""
        print(self)

    def where(self, column:str, expression:str) -> List[tuple]:
        """Return a subset of rows based on a condition
        :param column: Identifier of the column where the expression will be apply
        :param expression: A expression composed by the operation and value. 
                           See 'operations' dictionary to get the list of valid options
        :return: List of tuple filtered by the expression

        # Exemples
        # Filter rows where column 'id' have value greater than '1'
        > where("id", "gt 1")
        >
        # Filter rows where column 'name' have value equal to 'Peter'
        > where("name", "eq 'Peter'")
        """
        empty = True
        op_keys = '|'.join(operations.keys())
        match = next(re.finditer(rf"({op_keys})\s+(.+)", expression, re.IGNORECASE))
        for idx, row in enumerate(self):
            if operations[match.group(1)](self[idx][column],eval(match.group(2))):
                yield row
                empty = False
        if empty:
            yield self.empty_row

    def __getitem__(self, key):
        row = dict()
        for idx, name in enumerate(self.columns):
            try:
                row[name]=self.rows[key][idx]
            except IndexError:
                logger.error("Row %s not find", str(key))
                return {col:None for col in self.columns}
        return row

    def __iter__(self):
        for row in self.rows:
            yield row

    def __len__(self):
        return len(self.rows)

    def __repr__(self):
        return json.dumps(self.definition)

    def __str__(self):
        """Pretty table format"""
        idx_size = 3
        # Max size of each column
        col_size = dict()
        for col in self.columns:
            col_size[col]=len(col)+1
            for idx, _ in enumerate(self):
                size = len(str(self[idx][col]))
                if col_size[col] < size:
                    col_size[col]=size
        # Table line separator
        sep = f"{' ':{'>'}{idx_size}}+"
        for key in self.columns.keys():
            sep += f"{'-':{'-'}{'<'}{col_size[key]}}+"
        # Table header
        col = f"{' ':{'>'}{idx_size}}|"
        for key in self.columns.keys():
            col += f"{key.split('.')[-1]:{'<'}{col_size[key]}}|"
        # Table rows
        rows = str()
        for idx, _ in enumerate(self):
            rows += f"{idx:{''}{'>'}{idx_size}}|"
            for key, val in self[idx].items():
                rows += f"{str(val):{'>'}{col_size[key]}}|"
            rows+='\n'
        if len(rows)>0:
            return f"""{sep}\n{col}\n{sep}\n{rows[:-1]}\n{sep}\n"""
        return f"""{sep}\n{col}\n{sep}\n{sep}\n"""

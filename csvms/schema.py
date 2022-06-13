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

    def __init__(self, name:str, columns:Dict[str,type]=None, data:List[tuple]=None, temp:bool=False):
        """Table representation and the data file using database location path to store all rows
        :param name: Table identifier composed by database name and the table name separated by '.'. If the database name was omitted, uses the default database instead
        :param columns: Dictionary with columns names and data types
        :param data: Load table tuples into table rows. If 'None' load from data file. Default is 'None'
        :param temp: If 'False' create datafile, other else the rows will be available only on python memory. Default 'False'
        """
        self.temporary = temp
        self.header = False
        _db = DEFAULT_DB
        self.name = name
        if name.find('.') != -1:
            _db, self.name = name.split('.')
        self.database = Database(_db, temp)
        self.columns = columns
        if self.columns is None:
            self.header = True
        self.rows = list()
        if data is not None:
            self.rows = data
        else:
            if exists(self.location):
                self.rows = list(self.load())
            elif not temp:
                logger.debug("create:%s",self.location)
                Path(self.location).touch()
        if self.columns is None:
            raise ValueError("Can't create table without columns")

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

    def load(self) -> List[tuple]:
        """Load csv file from path with column formats
        :return: Tuple iterator
        """
        with open(self.location, mode='r', encoding="utf-8") as csv_file:
            csv_reader = reader(csv_file, delimiter=Table.CSVSEP)
            _head = self.header
            for raw in csv_reader:
                if _head:
                    self.columns = {val:str for val in raw}
                    _head = False
                    continue
                row = list()
                for idx, col in enumerate(self.columns.values()):
                    row.append(col(raw[idx]))
                yield tuple(row)

    def save(self) -> bool:
        """Write data to file system"""
        with open(self.location, mode='w', encoding="utf-8") as csv_file:
            csv_writer = writer(csv_file, delimiter=Table.CSVSEP, quotechar='"')
            _head = self.header
            for row in self.rows:
                if _head:
                    _head = False
                    csv_writer.writerow(tuple(self.columns.keys()))
                csv_writer.writerow(row)
        return True

    def alter(self, option:str, column:Dict[str,type], new:Dict[str,type]=None) -> bool:
        """Alter table definitions
        :param option: Accepts ADD, DROP and MODIFY
        :param column: Where to apply alteration
        :param new: New column definition. Only used on MODIFY operations. Default is None
        :return: True if table alteration was succeeded
        """
        for key, val in column.items():
            if option.upper() == "ADD":
                return self.add_column(key, val)
            if option.upper() == "DROP":
                return self.drop_column(key)
            if option.upper() == "MODIFY":
                if new is None:
                    raise ValueError("Need to inform new column definition")
                return self.modify_column(key, new)
        return False

    def add_column(self, name:str, dtype:type) -> bool:
        """Add new column to table
        :param name: Column name
        :param dtype: Column data type
        :return: True if table alteration was succeeded
        """
        self.columns.update({name:dtype}) # Add column definition
        for idx, row in enumerate(self.rows):
            self.rows[idx] = row + (dtype(),) # Add default values
        return True

    def drop_column(self, column:str) -> bool:
        """Drop column from table
        :param key: Column name
        :return: True if table alteration was succeeded
        """
        idx = None
        for pos, col in enumerate(self.columns.keys()):
            if col == column:
                idx = pos # Save colum index
                del self.columns[column] # Remove from columns
                break # exit from loop
        if idx is None:
            raise ValueError(f"Column {column} not found")
        for pos, row in enumerate(self.rows):
            row = list(row) # Convert to list
            del row[idx] # remove value for column index
            self.rows[pos] = tuple(row) # Update row
        return True

    def modify_column(self, name:str, column:Dict[str,type]) -> bool:
        """Drop column from table
        :param name: Column name
        :param column: New column
        :return: True if table alteration was succeeded
        """
        idx = None
        val = next(iter(column.values()))
        for pos, col in enumerate(self.columns.keys()):
            if col == name:
                idx = pos # Save colum index
                self.columns.update(column) # Update definition
                del self.columns[col] # Remove old column
                break # exit from loop
        tmp_rows = self.rows
        try:
            for pos, row in enumerate(self.rows):
                row = list(row)
                row[idx] = val(row[idx]) # Update column value
                print(type(val(row[idx])))
                self.rows[pos] = tuple(row) # Update row list
        except Exception as err:
            logger.error(err)
            self.rows = tmp_rows
            raise ValueError(f"Cant change data type for column {name}")
        return True

    def clean(self) -> bool:
        """Remove all table data"""
        self.rows = list()
        if exists(self.location):
            remove(self.location)
        if not self.temporary:
            Path(self.location).touch()
        return True

    def drop(self) -> bool:
        """Remove physical file"""
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
        """Return rows as Dict"""
        row = dict()
        for idx, name in enumerate(self.columns):
            try:
                row[name]=self.rows[key][idx]
            except IndexError:
                logger.error("Row %s not find", str(key))
                return {col:None for col in self.columns}
        return row

    def __iter__(self):
        """Iteration over all rows"""
        for row in self.rows:
            yield row

    def __len__(self):
        """Number of rows"""
        return len(self.rows)

    def __repr__(self):
        """Table definition in JSON format"""
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

""" Database catalog """
import json
import re
from csv import reader, writer
from os import remove
from os.path import exists
from pathlib import Path
from typing import List, Dict

# Local module
from csvms import logger, DEFAULT_DB
from csvms.schema import Database
from csvms.exceptions import ColumnException, DataException, TableException
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

class Table():
    """Represents a collection of tuples as table"""
    FORMAT="csv" # Data file format
    CSVSEP=";"   # Separator

    def __init__(self, name:str, columns:Dict[str,type]=None, data:List[tuple]=list(), temp:bool=False):
        """Table representation and the data file using database location path to store all rows
        :param name: Table identifier composed by database name and the table name separated by '.'
                     If the database name was omitted, uses the default database instead
        :param columns: Dictionary with columns names and data types
        :param data: Load table tuples into table rows. If 'None' load from data file. Default is 'None'
        :param temp: If 'False' create datafile, other else the rows will be available only on python memory.
                     Default 'False'
        """
        self.temporary = temp
        _db = DEFAULT_DB
        self.name = name
        if name.find('.') != -1:
            _db, self.name = name.split('.')
        self.database = Database(_db, temp)
        self.columns = columns
        self._rows = data
        if exists(self.location):
            self._rows = list(self.load())
        if self.columns is None:
            raise TableException("Can't create table without columns")

    @classmethod
    def _condition_parser_(cls, exp:str) -> List[str]:
        """Condition parser
        :param exp: String with operation
        :return: List with operation name and value
        """
        ops = '|'.join(operations.keys())
        match = next(re.finditer(rf"({ops})\s+(.+)", exp, re.IGNORECASE))
        return [match.group(1), match.group(2)]

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
        :param table_id: Table full name
        :return: Tuple iterator
        """
        definition = self.database.catalog[self.full_name]
        self.columns = {key:dtypes[value] for key, value in definition["columns"].items()}
        with open(self.location, mode='r', encoding="utf-8") as csv_file:
            for raw in reader(csv_file, delimiter=Table.CSVSEP):
                row = list()
                for idx, col in enumerate(self.columns.values()):
                    row.append(col(raw[idx]))
                yield tuple(row)

    def save(self) -> bool:
        """Write data to file system"""
        if self.temporary:
            raise TableException("Can't save temporary tables")
        with open(self.location, mode='w', encoding="utf-8") as csv_file:
            csv_writer = writer(csv_file, delimiter=Table.CSVSEP, quotechar='"')
            for row in self._rows:
                csv_writer.writerow(row)
        self.database.catalog[self.full_name] = self.definition
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
                return self._add_column_(key, val)
            if option.upper() == "DROP":
                return self._drop_column_(key)
            if option.upper() == "MODIFY":
                if new is None:
                    raise ColumnException("Need to inform new column definition")
                return self._modify_column_(key, new)
        return False

    def _add_column_(self, name:str, dtype:type) -> bool:
        """Add new column to table
        :param name: Column name
        :param dtype: Column data type
        :return: True if table alteration was succeeded
        """
        self.columns.update({name:dtype}) # Add column definition
        for idx, row in enumerate(self._rows):
            self._rows[idx] = row + (dtype(),) # Add default values
        return True

    def _drop_column_(self, column:str) -> bool:
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
            raise ColumnException(f"Column {column} not found")
        for pos, row in enumerate(self._rows):
            row = list(row) # Convert to list
            del row[idx] # remove value for column index
            self._rows[pos] = tuple(row) # Update row
        return True

    def _modify_column_(self, name:str, column:Dict[str,type]) -> bool:
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
        tmp_rows = self._rows
        try:
            for pos, row in enumerate(self._rows):
                row = list(row)
                row[idx] = val(row[idx]) # Update column value
                print(type(val(row[idx])))
                self._rows[pos] = tuple(row) # Update row list
        except Exception as err:
            logger.debug(err)
            self._rows = tmp_rows
            raise ColumnException(f"Cant change data type for column {name}")
        return True

    def clean(self) -> bool:
        """Remove all table data"""
        self._rows = list()
        if exists(self.location):
            remove(self.location)
        if not self.temporary:
            Path(self.location).touch()
        return True

    def drop(self) -> bool:
        """Remove physical file"""
        remove(self.location)
        del self.database.catalog[self.full_name]
        return True

    def show(self, size:int=10, trunc:bool=True) -> str:
        """Print pretty table data"""
        idx_pad = 3
        # Max size of each column
        col_size = dict()
        for col in self.columns:
            col_size[col]=len(col)+1
            for idx, _ in enumerate(self):
                cols = len(str(self[idx][col]))
                if col_size[col] < cols:
                    col_size[col]= cols
        # Table line separator
        sep = f"{' ':{'>'}{idx_pad}}+"
        for key in self.columns.keys():
            sep += f"{'-':{'-'}{'<'}{col_size[key]}}+"
        # Table header
        col = f"{' ':{'>'}{idx_pad}}|"
        for key in self.columns.keys():
            if trunc:
                col += f"{key.split('.')[-1]:{'<'}{col_size[key]}}|"
            else:
                col += f"{key:{'<'}{col_size[key]}}|"
        # Table rows
        rows = str()
        if len(self) <= size:
            size = len(self)
            for idx in range(size):
                rows += f"{idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
        else:
            for idx in range(int(size/2)):
                rows += f"{idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
            # Separator row,
            rows += f"{' ':{'>'}{idx_pad}}|"
            for key in self.columns.keys():
                rows += f"{'...':{''}{'>'}{col_size[key]}}|"
            rows+='\n'
            #reversed rows
            for idx in reversed(range(int(size/2))):
                _idx = len(self)-idx-1
                rows += f"{_idx:{''}{'>'}{idx_pad}}|"
                for key, val in self[_idx].items():
                    rows += f"{str(val):{'>'}{col_size[key]}}|"
                rows+='\n'
        if len(rows)>0:
            return f"""{sep}\n{col}\n{sep}\n{rows[:-1]}\n{sep}\n"""
        return f"""{sep}\n{col}\n{sep}\n{sep}\n"""

    def _validade_(self, value) -> tuple:
        row = tuple()
        try:
            for idx, val in enumerate(self.columns.values()):
                row += (val(value[idx]),)
            return row
        except IndexError as err:
            logger.debug(err)
        except ValueError as err:
            logger.debug(err)
        raise DataException(f"Invalid data {value} to row {tuple(self.columns.values())}")

    def append(self, *values) -> bool:
        """Add new row
        :param values: list of values, separated by comma, to insert into
        :return: True if table insertion was succeeded
        """
        self._rows.append(self._validade_(values))
        logger.info("Row inserted")
        return True

    def __setitem__(self, idx:int, value:tuple) -> bool:
        """Update row
        :param idx: Index row to update
        :param value: New values to the row
        """
        self._rows[idx] = self._validade_(value)
        logger.info("Row updated")
        return True

    def __delitem__(self, idx) -> None:
        """Remove line from table
        :param idx: Row table index to delete
        """
        del self._rows[idx]
        logger.info("Row deleted")

    def __getitem__(self, key):
        """Return rows as Dict"""
        try:
            return {n:self._rows[key][i] for i,n in enumerate(self.columns)}
        except IndexError:
            logger.debug("Row %s not found", key)
            return {col:None for col in self.columns.keys()}

    def __iter__(self):
        """Iteration over all rows"""
        for row in self._rows:
            yield row

    def __len__(self):
        """Number of rows"""
        return len(self._rows)

    def __repr__(self):
        """Table definition in JSON format"""
        return json.dumps(self.definition)

    def __str__(self):
        """Pretty table format"""
        return self.show()

    # Relational Algebra operators
    def __add__(self, other:"Table") -> "Table":
        """Union Operator"""
        return Table(
            name = f"tmp.{self.name}U{other.name}",
            columns=self.columns,
            data= list(dict.fromkeys(self._rows+other._rows)),
            temp=True)

    def __mod__(self, other:"Table") -> "Table":
        """Inserct Operator"""
        return Table(
            name = f"tmp.{self.name}∩{other.name}",
            columns=self.columns,
            data=[r for r in self for o in other if r == o],
            temp=True)

    def __mul__(self, other:"Table") -> "Table":
        """Times Operator"""
        columns = {f"{self.name}.{k}":v for k, v in self.columns.items()}
        columns.update({f"{other.name}.{k}":v for k, v in other.columns.items()})
        return Table(
            name =  f"tmp.{self.name}x{other.name}",
            columns=columns,
            data=[r+o for r in self for o in other],
            temp=True)

    def __truediv__(self, other:"Table") -> "Table":
        """Divide Operator"""
        return Table(
            name =  f"tmp.{self.name}÷{other.name}",
            columns={k:v for k, v in self.columns.items() if k not in other.columns.keys()},
            data=list(),
            temp=True)

    def __sub__(self, other:"Table") -> "Table":
        """Minus Operator"""
        rows = list()
        for _r_ in self:
            rows.append(_r_)
            for _o_ in other:
                if _r_ == _o_:
                    rows.pop()
        return Table(
            name = f"tmp.{self.name}-{other.name}",
            columns=self.columns,
            data=rows,
            temp=True)

    def __filter__(self, row:dict, ast:dict) -> bool:
        """ Resolve where conditions recursively
        :param ast: parsed where
        :return: Boolean result
        """
        value = lambda v: row[v] if isinstance(v,str) and v in self.columns.keys() else v
        if isinstance(ast, dict):
            for key, val in ast.items():
                if len(val)>2: # Multiple conditions with and/or
                    return self.__filter__(row, {key:[val[-2],val[-1]]})
                _x_, _y_ = val
                if isinstance(_x_, dict):
                    if _x_.get('literal') is None:
                        _x_ = self.__filter__(row, _x_)
                    else:
                        _x_ = _x_['literal']
                if isinstance(_y_, dict):
                    if _y_.get('literal') is None:
                        _y_ = self.__filter__(row, _y_)
                    else:
                        _y_ = _y_['literal']
                return operations[key](value(_x_),value(_y_))

    def π(self, columns:list) -> "Table":
        """Projection Operator"""
        cols = [(idx,key)for idx, key in enumerate(self.columns.keys()) if key in columns]
        rows = list()
        for row in self:
            _r_ = tuple()
            for idx,_ in cols:
                _r_ += (row[idx],)
            rows.append(_r_)
        return Table(
            name = f"tmp.{self.name}π",
            columns={k:self.columns[k] for _,k in cols},
            data=rows,
            temp=True)

    def σ(self, condition:Dict[str,list]) -> "Table":
        """Selection Operator
        :param condition: A expression composed by the logic operation and list of values to compare.
                          See 'operations' dictionary to get the list of valid options

        # Exemples
        # where id < 2
        > where({'lt':['id',2]})
        >
        # where val = 'George' and id > 1
        > where({'and':[{"eq":['val','George']},{"gt":['id',1]}]})
        """
        return Table(
            name = f"tmp.{self.name}σ",
            columns=self.columns,
            data=[r for i, r in enumerate(self) if self.__filter__(self[i], condition)],
            temp=True)

    def ꝏ(self, other:"Table", where:Dict[str,list]) -> "Table":
        """Join Operator"""
        tbl = (self * other).σ(where)
        tbl.name = f"{self.name}⋈{other.name}"
        return tbl

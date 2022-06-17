""" Database catalog """
import json
import re
from csv import reader, writer
from os import remove
from os.path import exists
from pathlib import Path
from typing import List, Dict

# Local module
from csvms import logger
from csvms.schema import Database
from csvms.exceptions import ColumnException
from csvms.exceptions import DataException
from csvms.exceptions import TableException
# Supported data types
dtypes = {
    "string":str,
    "str":str,
    "text":str,
    "int":int,
    "integer":int,
    "float":float
}
# Rename column
rnm = lambda t,c:c if str(c).find('.')!=-1 else f"{t}.{c}"
# Check for None values
NaN = lambda z:False if z is None else z
# Supported operations
operations = {
    'lt'     :lambda x,y:NaN(x) < NaN(y),
    'gt'     :lambda x,y:NaN(x) > NaN(y),
    'eq'     :lambda x,y:NaN(x) == NaN(y),
    'lte'    :lambda x,y:NaN(x) <= NaN(y),
    'gte'    :lambda x,y:NaN(x) >= NaN(y),
    'neq'    :lambda x,y:NaN(x) != NaN(y),
    'is'     :lambda x,y:NaN(x) is NaN(y),
    'in'     :lambda x,y:NaN(x) in NaN(y),
    'nin'    :lambda x,y:NaN(x) not in NaN(y),
    'or'     :lambda x,y:NaN(x) or NaN(y),
    'and'    :lambda x,y:NaN(x) and NaN(y),
    'missing':lambda   x:x is None,
    'exists' :lambda   x:x is not None,
}
# Supported functions
functions = {
    'add': lambda x,y: None if x is None or y is None else x+y,
    'sub': lambda x,y: None if x is None or y is None else x-y,
    'div': lambda x,y: None if x is None or y is None else x/y,
    'mul': lambda x,y: None if x is None or y is None else x*y,
    'concat': NotImplemented} #TODO: Create a new function to CONCATENATE strings
# All logical operations are also supported as function
functions.update(operations)

# Supported operations reverse
strtypes = {value:key for key, value in dtypes.items()}

class Table():
    """Represents a collection of tuples as table"""
    FORMAT="csv" # Data file format
    CSVSEP=";"   # Separator

    def __init__(self, name:str, columns:Dict[str,type]=None, data:List[tuple]=None, temp:bool=False):
        """Table representation and the data file using database location path to store all rows
        :param name: Table identifier composed by database name and the table name separated by '.'
                     If the database name was omitted, uses the default database instead
        :param columns: Dictionary with columns names and data types
        :param data: Load table tuples into table rows. If 'None' load from data file. Default is 'None'
        :param temp: If 'False' create datafile, other else the rows will be available only on python memory.
                     Default 'False'
        """
        if data is None:
            data = list()
        self.temporary = temp
        _db = None
        self.name = name
        if name.count('.') == 1:
            _db, self.name = name.split('.')
        self.database = Database(_db, temp)
        self.columns = columns
        if exists(self.location):
            self._rows = list(self.load())
        if data is not None:
            self._rows = data
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

    def _value_(self, row:tuple, key:str):
        """Get valeu from row by column name if it's a columnn identifier
        :param row: Row tuple
        :param key: Column identifier
        """
        if key in self.columns.keys():
            return row[key]
        return key

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
        """Print as pretty table data"""
        # Ugly code for a pretty table...
        idx_pad = 3
        # Max size of each column
        col_size = dict()
        for _c_ in self.columns:
            col_size[_c_]=len(_c_)+1
            for idx, _ in enumerate(self):
                cols = len(str(self[idx][_c_]))
                if col_size[_c_] < cols:
                    col_size[_c_]= cols
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
        tbl = f"{' ':{'>'}{idx_pad}}TABLE: {self.full_name}\n"
        if len(rows)>0:
            return f"""{tbl}{sep}\n{col}\n{sep}\n{rows[:-1]}\n{sep}\n"""
        return f"""{tbl}{sep}\n{col}\n{sep}\n{sep}\n"""

    def _validade_(self, value) -> tuple:
        row = tuple()
        try:
            for idx, val in enumerate(self.columns.values()):
                if value[idx] is None:
                    row += (None,)
                else:
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

    def _extend_(self, row:dict, ast:dict):
        """ Resolve functions recursively
        :param ast: parsed expression
        :return: Calculated value
        """
        if isinstance(ast, dict):
            for key, val in ast.items():
                _x_, _y_ = val
                if isinstance(_x_, dict):
                    if _x_.get('literal') is None:
                        _x_ = self._extend_(row, _x_)
                    else:
                        _x_ = _x_['literal']
                if isinstance(_y_, dict):
                    if _y_.get('literal') is None:
                        _y_ = self._extend_(row, _y_)
                    else:
                        _y_ = _y_['literal']
                return functions[key](self._value_(row,_x_),self._value_(row,_y_))
        raise DataException(f"Can't evaluate expression: {ast}")

    def _logical_evaluation_(self, row:dict, ast:dict) -> bool:
        """Recursively evaluate conditions 
        :param ast: Abstract Syntax Tree
        :return: Boolean result
        """
        if isinstance(ast, dict):
            for key, val in ast.items():
                if key in ['missing','exists']:
                    return operations[key](self._value_(row,val))
                if len(val)>2: # Multiple conditions with and/or
                    return self._logical_evaluation_(row, {key:[val[-2],val[-1]]})
                _x_, _y_ = val
                if isinstance(_x_, dict):
                    if _x_.get('literal') is None:
                        _x_ = self._logical_evaluation_(row, _x_)
                    else:
                        _x_ = _x_['literal']
                if isinstance(_y_, dict):
                    if _y_.get('literal') is None:
                        _y_ = self._logical_evaluation_(row, _y_)
                    else:
                        _y_ = _y_['literal']
                return operations[key](self._value_(row,_x_),self._value_(row,_y_))
        raise DataException(f"Can't evaluate expression: {ast}")

    ### Relational Algebra operators ###

    def __add__(self, other:"Table") -> "Table":
        """Union Operator (∪)"""
        return Table(
            name = f"({self.name}∪{other.name})",
            # Copy all columns from self
            columns={k:v for k,v in self.columns.items()},
            # Sum all distinct rows from self and other table
            data= list(dict.fromkeys(self._rows + other._rows)))

    def __mod__(self, other:"Table") -> "Table":
        """Inserct Operator (∩)"""
        return Table(
            name = f"({self.name}∩{other.name})",
            # Copy all columns from self
            columns={k:v for k,v in self.columns.items()},
            # Filter rows of self equal to rows of other
            data=[r for r in self for o in other if r == o])

    def __sub__(self, other:"Table") -> "Table":
        """Difference Operator (−)"""
        rows = list() # Create a new list of rows
        for _r_ in self: # For each row in self
            rows.append(_r_) # Add the self rows to the new list
            for _o_ in other: # Check if are any tuple in other table that match
                if _r_ == _o_: # If finds a row in other that are equal to self
                    rows.pop() # Remove self rows
        return Table(
            name = f"({self.name}−{other.name})",
            columns={k:v for k,v in self.columns.items()},
            data=rows)

    def __mul__(self, other:"Table") -> "Table":
        """Times Operator (×)"""
        # Join the tow sets of columns, and concatenate the table name if needed
        columns = {rnm(self.name,k):v for k, v in self.columns.items()}
        columns.update({rnm(other.name,k):v for k, v in other.columns.items()})
        return Table(
            name = f"({self.name}×{other.name})",
            # Concatenated columns
            columns=columns,
            # Cartesian product of a set of self rows with a set of other rows
            data=[r+o for r in self for o in other])

    def π(self, columns:list) -> "Table":
        """Projection Operator (π)"""
        # Create a list of projected columns and your index
        cols = [(idx,key)for idx, key in enumerate(self.columns.keys()) if key in columns]
        rows = list() # Create a new list of rows
        for row in self: # For each row
            _r_ = tuple() # Create a new tuple
            for idx,_ in cols: # For each projected column
                _r_ += (row[idx],) # Add values for projected column index
            rows.append(_r_) # Append the new sub tuple to the new list of rows
        return Table(
            name = f"({self.name}π)",
            # Use only projected columns
            columns={k:self.columns[k] for _,k in cols},
            data=rows)

    def σ(self, condition:Dict[str,list]) -> "Table":
        """Selection Operator (σ)
        :param condition: A expression composed by the logic operation and list of values.
                          See 'operations' dictionary to get the list of valid options

        # Exemples
        ## where id < 2
        > where({'lt':['id',2]})
        ## where val = 'George' and id > 1
        > where({'and':[{"eq":['val','George']},{"gt":['id',1]}]})

        # Operations
        ## List of supported operations and the logical equivalent python evaluation
        +---------+-------------+
        | Name    | Python eval |
        +---------+-------------+
        | lt      | <           |
        | gt      | >           |
        | eq      | ==          |
        | lte     | <=          |
        | gte     | >=          |
        | neq     | !=          |
        | is      | is          |
        | in      | in          |
        | nin     | not in      |
        | or      | or          |
        | and     | and         |
        | missing | is None     |
        | exists  | is not None |
        +---------+-------------+
        """
        return Table(
            name = f"({self.name}σ)",
            # Create a copy of columns
            columns={k:v for k,v in self.columns.items()},
            # Filter rows with conditions are true
            data=[r for i, r in enumerate(self) if self._logical_evaluation_(self[i], condition)])

    def ᐅᐊ(self, other:"Table", where:Dict[str,list]) -> "Table":
        """Join Operator (⋈)"""
        # Create a new table with the Cartesian product of self and otther
        tbl = (self * other)\
            .σ(where) # And select rows where the join condition is true
        tbl.name = f"({self.name}⋈{other.name})"
        return tbl

    def ρ(self, alias:str) -> "Table":
        """Rename Operator (ρ)"""
        # Function to rename column names for the new table name
        _rnm = lambda x: x if x.count('.')==0 else x.split('.')[-1]
        return Table(
            # Set new table name
            name = f"{alias}",
            # Copy all columns from source table
            columns={_rnm(k):v for k,v in self.columns.items()},
            # Copy all rows from source table
            data=[r for r in self])

    def Π(self, extend:dict, alias:str=None) -> "Table":
        """Extended projection Operator (Π)"""
        rows = list() # New list of rows
        dtype = None # Use to store the data type of the new extended column
        for idx, row in enumerate(self): # For each row
            val = self._extend_(self[idx],extend) # Evaluated expression
            if dtype is None: # If is the first evaluation
                dtype = type(val) # Use the result data type
            # if you find any different type in the next rows
            elif dtype != type(val) and val is not None:
                # Raise an Data exeption to abort the operation
                raise DataException(f"{type(val)} error")
            # If Successful add new value to the row tuple
            rows.append(row + (val,))
        # Copy the columns from source table
        cols = {k:v for k,v in self.columns.items()}
        # Add new extended column
        if alias is None: # Remova some characters and use the expression as column name
            cols[f"{str(extend).replace(' ','').replace('.',',')}"]=dtype
        else: # Use alias for the new extended column
            cols[alias]=dtype
        return Table(
            name = f"({self.name}Π)",
            columns=cols,
            data=rows)

    def ᗌᐊ(self, other:"Table", where:Dict[str,list]) -> "Table":
        """Left outer join (⟕)"""
        # Cross join between self and other table
        tbl = (self * other).σ(where)
        # Diference between self and cross jouin is the left rows
        out = self - tbl.π([f"{self.name}.{k}"for k in self.columns.keys()])
        # Crate a copy of data structure of other table with one empty row
        emp = Table("⟕",{k:v for k,v in other.columns.items()},[other.empty_row])
        # Create a product of left rows and empty row of other table and add to the cross
        lef = tbl + (out * emp)
        # Rename the result table
        lef.name = f"({self.name}⟕{other.name})"
        return lef

    def ᐅᗏ(self, other:"Table", where:Dict[str,list]) -> "Table":
        """Right outer join operator(⟖)"""
        # Cross join between self and other table
        tbl = (self * other).σ(where)
        # Diference between other table and cross jouin is the right rows
        out = other - tbl.π([f"{other.name}.{k}"for k in other.columns.keys()])
        # Crate a copy of data structure of self with one empty row
        emp = Table("⟖",{k:v for k,v in self.columns.items()},[self.empty_row])
        # Create a product of right rows and empty row of other table and add to the cross
        rig = tbl + (emp * out)
        # Rename the result table
        rig.name = f"({self.name}⟖{other.name})"
        return rig

#TODO: Implement DIVIDEBY operator `/`
#TODO: Implement the FULL join operator `ᗌᗏ`

# **C**omma-**S**eparated **V**alues **M**anagement **S**ystem


[![PyPI](https://img.shields.io/pypi/v/csvms)](https://pypi.org/project/csvms/) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/csvms)

Python module to manage **CSV** data like a DBMS application

## Installation

```bash
pip install csvms
```

## Usage

You can use the `help` command in all objects to read the complete documentation

### Database

This object represents a physical location on the file system with a set of [Tables](#table)

```python
from csvms.schema import Database
db = Database("dbname")
```

This will create a new directory, if not exists, inside `$CSVMS_FILE_DIR` path

> In most cases this will not be necessary because the Database object is implicit created based on the [Table](#table) name using the notation `database.table_name`

### Table

The `Table` object represents a CSV data file.

You can create a **sample** table like

```python
from csvms.schema import Table
tbl = Table(
    name="sample",
    columns={
        "c1":int,
        "c2":str,
        "c3":float
    },
    data=[
        (1,"Hello",0.1),
        (2,"World",1.0),
    ]
)
```

Without spefify a database on **name** this table will be created under a default directory (`$CSVMS_DEFAULT_DB`). The **columns** is a dictionary composed by the name and type using [python primitive data types](https://www.w3schools.com/python/python_datatypes.asp) and the **data** need to be a list of tuples

Using `print` you can they see the object as a table representation

```python
print(tbl)
```

```log
   +---+-----+---+
   |c1 |c2   |c3 |
   +---+-----+---+
  0|  1|Hello|0.1|
  1|  2|World|1.0|
   +---+-----+---+
```

the `save` function will write all data in a **CSV** format based on the table `location` property

```python
tbl.save()
```

```bash
cat data/default/sample.csv
1;Hello;0.1
2;World;1.0
```

> For more informatios use `help(Table)`
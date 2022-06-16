# **C**omma-**S**eparated **V**alues **M**anagement **S**ystem


[![PyPI](https://img.shields.io/pypi/v/csvms)](https://pypi.org/project/csvms/) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/csvms)

Python module to manage **CSV** data like a DBMS application

![logo](https://raw.githubusercontent.com/Didone/csvms/main/img/logo.png)

## Installation

```bash
pip install csvms
```

## Usage

You can use the `help` command in all objects to read the complete documentation

### Database

This object represents a physical location on the file system with a set of [tables](#table)

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
from csvms.table import Table
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
>>> print(tbl)
   TABLE: default.sample
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

#### Data access

It's possible to access a row by your **index** value, like a simple python tuple

```python
>>> tbl[1]
{'c1': 2, 'c2': 'World', 'c3': 1.0}
```

The row will be return as an **dictionary**, so, with the column name (after the index) you can access the value associated

```python
>>> tbl[1]["c2"]
'World'
```

It's also possible iterate into all rows using a `for` loop

```python
>>> for row in tbl:
...     print(row)
... 
(1, 'Hello', 0.1)
(2, 'World', 1.0)
```

#### Data manipulation

You can add a new row using the `append` function

```python
>>> tbl.append(3, "Some", 0)
>>> print(tbl)
   TABLE: default.sample
   +---+-----+---+
   |c1 |c2   |c3 |
   +---+-----+---+
  0|  1|Hello|0.1|
  1|  2|World|1.0|
  2|  3| Some|0.0|
   +---+-----+---+
```

Update a specific row by your **index**

```python
>>> tbl[0] = (4, "Value", 3.3)
>>> print(tbl)
   TABLE: default.sample
   +---+-----+---+
   |c1 |c2   |c3 |
   +---+-----+---+
  0|  4|Value|3.3|
  1|  2|World|1.0|
  2|  3| Some|0.0|
   +---+-----+---+
```

And also remove a row by the **index**

```python
>>> del tbl[1]
>>> print(tbl)
   TABLE: default.sample
   +---+-----+---+
   |c1 |c2   |c3 |
   +---+-----+---+
  0|  4|Value|3.3|
  1|  3| Some|0.0|
   +---+-----+---+
```

### Catalog

When you instantiate an object the Catalog objet will save the [table](#table) definitions for future queries and save in [json format](https://www.w3schools.com/whatis/whatis_json.asp) on root directory.

```json
{
    "default.sample": {
        "name": "default.sample",
        "columns": {
            "c1": "integer",
            "c2": "text",
            "c3": "float"
        }
    }
}
```

### Relational algebra

The main purpose of the relational algebra is to define operators that transform one or more input relations to an output relation. Given that these operators accept relations as input and produce relations as output, they can be combined and used to express potentially complex queries that transform potentially many input relations (whose data are stored in the database) into a single output relation (the query results).

This are the current operations supported:

|Simbolo|Oprador |Operação |Sintaxe|
|---|--------|---------|-------|
|**σ**|σ|Select|A.σ([`<logic functions>`])|
|**π**|π|Project|A.π(`<attribute list>`)|
|**∪**|+|Union|A + B|
|**∩**|%|Intersection|A % B|
|**-**|-|Difference|A – B|
|**X**| * |Product|A * B|
|**⋈**|ᐅᐊ|Join|A.ᐅᐊ( B, `<logic functions>` )|
|**ρ**|ρ|Rename|A.ρ(`name`)|
|**Π**|Π|Extend|A.Π(`<arithmetic functions>`)|
|**⟕**|ᐅᐸ|Left Outer Join|A.ᐅᐸ( B, `<logic functions>` )|
|**⟖**|ᐳᐊ|Right Outer Join|A.ᐳᐊ( B, `<logic functions>` )|

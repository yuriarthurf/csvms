Module csvms.table
==================
Table Module

Functions
---------

    
`rnm(table: str, column: str) ‑> str`
:   Rename table column
    
    Parameters
    ----------
    table:`str`
        Table name
    
    column:`str`
        Column name
    
    Returns
    ----------
    `str`
        Column renamed

Classes
-------

`Table(name: str, columns: Dict[str, type] = None, data: List[tuple] = None, temp: bool = False)`
:   Represents a collection of tuples as table
    
    Class variables
    ----------
    dtypes : `Dict[str,type]`
        Supported columns data types
    
    operations : `Dict[str,Callable]`
        Supported operator on selection operations
    
    functions : `Dict[str,Callable]`
        Supported functions on extended operations
    
    Attributes
    ----------
    name : `str`
        Table name identifier
    
    database : `Database`
        Database where the table are located
    
    columns : `Dict[str,type]`
        Table attributes
    
    temporary : `bool`
        True if table is temporary.
        Important: Temporary table can't be save on disk
    
    Table representation and the data file using database location path to store all rows
    
    Parameters
    ----------
    name : `str`
        Table identifier composed by database name and the table name separated by '.'
        If the database name was omitted, uses the default database instead
    
    columns : `Dict[str,type]`, `optional`
        Dictionary with columns names and data types. Only python primitive type are alowed.
        If None, load from catalog definition. *Default is None*
    
        #### Example
    
        ```
        Table(
            name='sample',
            columns={
                'att1':str,
                'att2':int})
        ```
    
    data : `List[tuple]`, `optional`
        Load table tuples into table rows. If None load from data file. *Default is None*
    
        #### Example
    
        ```
        Table(
            name='sample',
            columns={
                'att1':str,
                'att2':int},
            data=[
                ('a',1),
                ('b',2)])
        ```
    
    temp : `bool`, `optional`
        If 'False' create datafile, other else the rows will be available only on python memory.
        *Default False*

    ### Class variables

    `dtypes`
    :

    `functions`
    :

    `operations`
    :

    ### Instance variables

    `definition: dict`
    :   Return table definition as dictionary

    `empty_row: tuple`
    :   Return an tuple with 'None' values for each column

    `full_name`
    :   Return table full name identifier

    `location: str`
    :   Return table location on file system as string

    `transaction_log: pathlib.Path`
    :   Path to transaction log

    ### Methods

    `alter(self, option: str, column: Dict[str, type], new: Dict[str, type] = None) ‑> csvms.table.Table`
    :   Alter table definitions
        :param option: Accepts ADD, DROP and MODIFY
        :param column: Where to apply alteration
        :param new: New column definition. Only used on MODIFY operations. Default is None
        :return: The new modified table

    `append(self, *values) ‑> bool`
    :   Add new row
        :param values: list of values, separated by comma, to insert into
        :return: True if table insertion was succeeded

    `clean(self) ‑> bool`
    :   Remove all table data

    `drop(self) ‑> bool`
    :   Remove physical file

    `extend(self, row: dict, ast: dict)`
    :   Resolve functions recursively
        :param ast: parsed expression
        :return: Calculated value

    `load(self) ‑> List[tuple]`
    :   Load csv file from path with column formats
        :param table_id: Table full name
        :return: Tuple iterator

    `logical_evaluation(self, row: dict, ast: dict) ‑> bool`
    :   Recursively evaluate conditions
        :param ast: Abstract Syntax Tree
        :return: Boolean result

    `save(self) ‑> bool`
    :   Write data to file system

    `show(self, size: int = 20, trunc: bool = True) ‑> str`
    :   Print as pretty table data

    `Π(self, extend: dict, alias: str = None) ‑> csvms.table.Table`
    :   Extended projection Operator (Π)

    `π(self, select: list) ‑> csvms.table.Table`
    :   Projection Operator (π)

    `ρ(self, alias: str) ‑> csvms.table.Table`
    :   Rename Operator (ρ)

    `σ(self, condition: Dict[str, list], null=False) ‑> csvms.table.Table`
    :   Selection Operator (σ)
        :param condition: A expression composed by the logic operation and list of values.
                          See 'operations' dictionary to get the list of valid options
        ### Exemples
        
        where id < 2
        `where({'lt':['id',2]})`
        
        where val = 'George' and id > 1
        `where({'and':[{"eq":['val','George']},{"gt":['id',1]}]})`
        
        ### Operations
        List of supported operations and the logical equivalent python evaluation
        
        | Name    | Python eval |
        |---------|-------------|
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

    `ᐅᐊ(self, other: Table, where: Dict[str, list]) ‑> csvms.table.Table`
    :   Join Operator (⋈)
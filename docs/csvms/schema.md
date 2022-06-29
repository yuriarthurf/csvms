Module csvms.schema
===================
Schema Module

Classes
-------

`Database(name: str = None, temp=False)`
:   The database is an file system directory
    Used to store table data files on the local file system
    
    Database representation
    :param name: Database identifier. If 'None' use default name. Default is None
    :param temp: If false, create directory. Default False

    ### Class variables

    `DEFAULT_DB`
    :

    `FILE_DIR`
    :

    ### Static methods

    `create_location(location: str) ‑> str`
    :   Create directory
        :param location: Filesystem path. Default directory is "database"
        :return: Location path
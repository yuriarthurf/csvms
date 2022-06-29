Module csvms.exceptions
=======================
Exception Module

Classes
-------

`ColumnException(*args: object)`
:   Base class for Column exceptions

    ### Ancestors (in MRO)

    * csvms.exceptions.DefaultException
    * builtins.Exception
    * builtins.BaseException

`DataException(*args: object)`
:   Base class for Data exceptions

    ### Ancestors (in MRO)

    * csvms.exceptions.DefaultException
    * builtins.Exception
    * builtins.BaseException

`DefaultException(*args: object)`
:   Base class for exceptions

    ### Ancestors (in MRO)

    * builtins.Exception
    * builtins.BaseException

    ### Descendants

    * csvms.exceptions.ColumnException
    * csvms.exceptions.DataException
    * csvms.exceptions.TableException

`TableException(*args: object)`
:   Base class for Table exceptions

    ### Ancestors (in MRO)

    * csvms.exceptions.DefaultException
    * builtins.Exception
    * builtins.BaseException
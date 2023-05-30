# pg_select

Simple example for Python C extension.
Execute SQL-statements for PostgreSQL.

## Parameters

* connection string
* SQL sentence
* cursor iteration size
* should the field names be displayed (boolean)
* should the debug information be displayed (boolean)

## Getting Started

    $ pip install distutils
    $ git clone {pg_select_url}
    $ cd pg_select
    $ virtualenv .
    $ source bin/activate
    $ python setup.py install
    $ python
    >>> import pg_select
    >>> connection_string = "host=localhost port=5432 dbname=postgres..."
    >>> sql = "SELECT * FROM..."
    >>> for x in pg_select.cursor(connection_string, sql, iteration=15, column_names=True, debug=True):
    ...     print(x)

## Attention!

* The calling script must validate the SQL statement.
* The passed query will be executed without validation.

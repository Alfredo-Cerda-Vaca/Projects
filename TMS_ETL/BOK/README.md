# TMS_ETL
Developed on Visual Studio Code
Python 3.11.2

## Requirements
ODBC Driver 18 for SQL Server

Install it from here:
[Release Notes for Microsoft ODBC Driver for SQL Server on Windows](https://learn.microsoft.com/en-us/sql/connect/odbc/windows/release-notes-odbc-sql-server-windows?view=sql-server-ver16)

## Python dependencies
Install the package `pipreqs`
```
pip install pipreqs
```

Save a snapshot of the main requirements
```
pipreqs /path/to/requirements.txt
or
pipreqs ./requirements.txt
```

Install main dependencies
```
pip install -r ./requirements.txt
```

## Python linter
Use `pylint` on vscode
https://code.visualstudio.com/docs/python/linting

## Virtual environments on Windows
https://pypi.org/project/virtualenvwrapper-win/

# Docstring example
```
"""
Summary line.

Extended description of function.

Parameters:
arg1 (int): Description of arg1

Returns:
int: Description of return value

"""
```
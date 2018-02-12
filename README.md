# odbctest

Tool for ODBC driver testing.

## Prerequisite
* Already installed ODBC driver.
  * Defined in odbcinst.ini similar to:
  ```
  [testdriver]
  Description = ODBC for PostgreSQL
  Driver = /opt/redhat/jboss-dv/v6/psqlodbc/lib64/psqlodbc.so
  Setup = /usr/lib64/libodbcpsqlS.so
  FileUsage = 1
  ```
* Accessible server to which driver should connect.
* Python 2.7
  * pip
  * installation from sources pyodbc module [nativeSQLTypes fork](https://github.com/jstastny-cz/pyodbc/tree/nativeSQLTypes)
    * introduces coldescription field into pyodbc.Cursor exposing DB native types, in addition to Python types
  * sqlparse module

## Running
python odbctest.py <ARGUMENTS>

There are two modes:
* GENERATE
  * used to generate new results, which needs to be reviewed. After initial review, these are used as expected results in testing.
* COMPARE
  * used to actually test the driver/server
  * using the expected results generated in GENERATE mode
  
### Arguments
| argument | short | description |
| :----- | :----- |:----- |
| --mode | -m | COMPARE |
| --odbcdriver | -o | driver name as in odbcinst.ini |
| --server | -s | server:port |
| --database | -d | database |
| --username | -u | user name |
| --password | -p | password |
| --queryfile | -q | queryfile, can be either file or directory. In case of a directory all the files are processed recursively. |
| --resultsdir | -r | location where to put results |
| --expecteddir | -e | location where expected results are (COMPARE mode only) |
 
### GENERATE mode:
```
odbctest.py -m GENERATE -o <odbcdriver> -s <server>:<port> -d <database> -u <username> -p <password> -q <queryfile> -r <resultsdir>
or
odbctest.py
 --mode GENERATE
 --odbcdriver <odbcdriver>
 --server <server>:<port>
 --database <database>
 --username <username>
 --password <password>
 --queryfile <queryfile>
 --resultsdir <resultsdir>
```

### COMPARE mode:
```
odbctest.py -m COMPARE -o <odbcdriver> -s <server>:<port> -d <database> -u <username> -p <password> -q <queryfile> -r <resultsdir> -e <expectedresultsdir>
or
odbctest.py
 --mode COMPARE
 --odbcdriver <odbcdriver>
 --server <server>:<port>
 --database <database>
 --username <username>
 --password <password>
 --queryfile <queryfile>
 --resultsdir <resultsdir>
 --expecteddir <expectedresultsdir>
 ```

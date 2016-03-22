import re
from odbc_helper import ODBCHelper
from results_container import ResultsContainer
from query_reader import QueryReader
from collections import namedtuple
import pyodbc
'''
Class QueryRunner is used to process sql queries on specified
ODBC enabled datasource. It requires existing odbc driver created.
The connection is determined by the constructor parameters.
'''
DatabaseConnection = namedtuple('DatabaseConnection','driver server port database user password')

class QueryRunner(object):
    helper = None

    def __init__(self, db):
        self.helper = ODBCHelper(db.driver,db.server,db.port,db.database,db.user,db.password)
	self.helper.connect()

    def destroy(self):
	self.helper.disconnect()
        self.helper.destroy()

# Method runs given query with specified name. Query is forwarded
# to odbc driver as is, the specified name is used to name the xml
# file with query output.

    def run(self,query_name, query):
        print "performing query: "+query_name
	rows = []
	columns = []
	exc_class = None
	exc_type = None
	exc_message = None
        try:
            rows = self.helper.execute_query(query)
	    columns = self.helper.parse_columns_from_query(query)
	except pyodbc.Error as e:
	    try:
		print e
		exc_class = re.findall(r"([.*\.]*.*Exception)",e.args[1])[0]
		exc_message_with_sql_state = e.args[1].split(exc_class)[0].replace("\n","")
		exc_message = re.split("\[.*\]\s*ERROR:\s*(.*)",exc_message_with_sql_state)[1]
		exc_type = exc_class.split(".")[-1]
		print "exception during query: "+query_name
    	    except Exception as e:
		print e
		if exc_message is None:
			exc_message="Unexpected exception occured."
	    	print "Error occured during connecting to database. Check the server is up."
	return ResultsContainer(query_name,query,columns,rows,exc_type,exc_message,exc_class)

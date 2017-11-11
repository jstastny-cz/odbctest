import pyodbc
from pyodbc import ProgrammingError
import sqlparse
import re
from sqlparse.sql import Token
from sqlparse.sql import TokenList
from sqlparse.sql import IdentifierList
from copy import deepcopy
from results_container import Column


class ODBCHelper:
    driver = ''
    server = ''
    port = ''
    database = ''
    user = ''
    password = ''
    connection = None
    cursor = None
    rows = None
    tables = None
    db_tables = None
    db_columns = {}

    def __init__(self, driver, server, port, database, user, password):
        self.driver = driver
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        self.connection = pyodbc.connect(
            'DRIVER={' + self.driver + '};SERVER=' + self.server + ';PORT=' +
            self.port + ';DATABASE=' + self.database + ';UID=' + self.user +
            ';PWD=' + self.password)

    def disconnect(self):
        self.clean()
        self.connection.close()

    def clean(self):
        if (self.cursor is not None):
            self.cursor.close()
        self.rows = None
        self.tables = None
        self.db_columns = {}
        self.db_tables = None
        self.cursor = self.connection.cursor()
        self.db_tables = deepcopy([(x[2], x[1], x[0])
                                   for x in self.cursor.tables()])

    def clear_cache(self):
        self.db_columns = {}

    def destroy(self):
        self.clear_cache()

    def execute_query(self, query):
        self.clean()
        stmt = sqlparse.parse(query)[0]
        query = query.encode('utf-8')
        self.cursor.execute(query)
        if stmt.get_type() in ["SELECT"] and self.cursor.rowcount > 0:
            try:
                self.rows = self.cursor.fetchall()
            except ProgrammingError as e:
                self.rows = []
        else:
            if stmt.get_type() in ["DELETE"]:
                self.cursor.commit()
            self.rows = []
        result = map(lambda x:[cell.encode('utf-8') if isinstance(cell,unicode) else str(cell) if cell is not None  else None for cell in x],[row for row in self.rows])
        return result

    def get_query_type(self):
        if self.rows:
            return "select"
        elif not self.rows and self.cursor.rowcount < 0:
            return "select"
        else:
            return "update"

    def get_update_count(self):
        return self.cursor.rowcount

    def parse_columns_from_query(self, query):
        if self.cursor.description is None:
            return []
        column_names = [col[0] for col in self.cursor.description]
        column_types = [col[0] for col in self.cursor.coldescription]
        columns = []
        for col_name, col_type_code in zip(column_names, column_types):
            self.cursor.getTypeInfo(col_type_code)
            col_info = self.cursor.fetchone()
            col_type_name = col_info[0]
            columns.append(Column(col_name, col_name, col_type_name))
        return columns

from xml.etree import ElementTree as etree
from results_container import ResultsContainer, Column
import re

XMLNS = "{http://xml.whipper.org/result}"
QUERY_RESULT = XMLNS + "query-result"
EXCEPTION = XMLNS + "exception"
QUERY = XMLNS + "query"
SELECT = XMLNS + "select"
UPDATE = XMLNS + "update"
TABLE = XMLNS + "table"
TABLE_ROW = XMLNS + "table-row"
TABLE_CELL = XMLNS + "table-cell"
EXCEPTION_TYPE = XMLNS + "type"
EXCEPTION_MESSAGE = XMLNS + "message"
EXCEPTION_REGEX = XMLNS + "message-regex"
EXCEPTION_CLASS = XMLNS + "class"


class ResultsLoader(object):
    is_exception = False
    is_ordered = False
    filename = None
    xml = None
    query_name = None
    query_type = None
    update_count = None
    query = None
    columns = []
    rows = []
    exception_type = None
    exception_message = None
    exception_regex = None
    exception_class = None

    def __init__(self, filename):
        self.filename = filename
        self.xml = etree.parse(filename).getroot()
        self.query_name = self.xml.find(QUERY_RESULT).get("name")
        el_query = self.xml.find(QUERY)
        self.query = el_query.text if not el_query is None else None
        self.is_except = self.xml.find(QUERY_RESULT + "/" + EXCEPTION)
        if self.is_except is not None:
            self.parse_exception()
        else:
            el_update = self.xml.find(QUERY_RESULT + "/" + UPDATE)
            self.query_type = "update" if el_update is not None else "select"
            if self.query_type == "update":
                self.update_count = el_update.get("update-count")
            elif self.query_type == "select":
                self.columns = self.parse_columns()
                self.rows = self.parse_rows()
        self.is_ordered = "order by" in self.query.lower(
        ) if not self.query is None else None

    def parse_columns(self):
        if self.is_except is not None: return None
        columns = []
        el_cols_container = self.xml.find(QUERY_RESULT + "/" + SELECT)
        for el in el_cols_container.getchildren():
            columns.append(Column(el.text, None, el.get("type")))
        return columns

    def parse_rows(self):
        if self.is_except is not None: return None
        rows = []
        el_rows_container = self.xml.find(QUERY_RESULT + "/" + TABLE)
        for el_row in el_rows_container.findall(TABLE_ROW):
            row = []
            for el_cell in el_row.findall(TABLE_CELL):

                if len(el_cell.getchildren()) == 0:
                    row.append(None)
                else:
                    row.append(el_cell.getchildren()[0].text)
            rows.append(row)
        return rows

    def parse_exception(self):
        if self.is_except is None: return None
        el_exception = self.xml.find(QUERY_RESULT + "/" + EXCEPTION)
        self.exception_type = el_exception.find(EXCEPTION_TYPE).text
        el_message = el_exception.find(EXCEPTION_MESSAGE)
        self.exception_message = el_message.text if el_message is not None else None
        el_regex = el_exception.find(EXCEPTION_REGEX)
        self.exception_regex = re.compile(
            el_regex.text) if el_regex is not None else None
        self.exception_class = el_exception.find(EXCEPTION_CLASS).text

    def load(self):
        return ResultsContainer(self.query_name, self.query, self.query_type,
                                self.update_count, self.columns, self.rows,
                                self.exception_type, self.exception_message,
                                self.exception_regex, self.exception_class)

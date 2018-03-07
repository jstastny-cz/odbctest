from xml.etree import ElementTree as etree
from collections import namedtuple
from results_container import Column
from xml.dom import minidom
'''
Class ResultsBuilder serves to create xml file with specific format
from given information.
'''
URI_RESULT = "http://xml.whipper.org/result"
XMLNS = "{" + URI_RESULT + "}"
RESULT = XMLNS + "result"
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
EXCEPTION_MESSAGE_REGEX = XMLNS + "message-regex"
EXCEPTION_CLASS = XMLNS + "class"
DATA_ELEMENT = XMLNS + "data-element"
URI_ERROR = "http://xml.whipper.org/error"
XMLNS_ERROR = "{" + URI_ERROR + "}"
QUERY_ERROR = XMLNS_ERROR + "query-error"
QUERY_ERROR_QUERY = XMLNS_ERROR + "query"
FAILURES = XMLNS_ERROR + "failures"
ACTUAL_RESULT = XMLNS_ERROR + "actual-result"
EXPECTED_RESULT = XMLNS_ERROR + "expected-result"
FAILURE = XMLNS_ERROR + "failure"


class ResultsWriter:
    xml = ''
    query_name = ''
    query = ''
    result_cols = []
    raw_results = []
    num_rows = 0
    num_cols = 0
    exception_type = None
    exception_message = None
    exception_class = None

    def __init__(self, results, exp_results=None, comparation_results=None):
        self.xml = None
        self.raw_results = results.rows
        self.query = results.query
        self.query_type = results.query_type
        self.exp_query_type = exp_results.query_type if exp_results is not None else None
        self.result_cols = results.columns
        self.update_count = results.update_count
        self.exp_update_count = exp_results.update_count if exp_results is not None else None
        self.num_rows = len(self.raw_results)
        self.num_cols = len(self.result_cols)
        self.exp_raw_results = exp_results.rows if exp_results is not None else None
        self.exp_result_cols = exp_results.columns if exp_results is not None else None
        self.exp_num_rows = len(
            self.exp_raw_results) if exp_results is not None else None
        self.exp_num_cols = len(
            self.exp_result_cols) if exp_results is not None else None
        self.query_name = results.query_name
        self.exception_type = results.exception_type
        self.exception_message = results.exception_message
        self.exception_class = results.exception_class
        self.exp_exception_type = exp_results.exception_type if exp_results is not None else None
        self.exp_exception_message = exp_results.exception_message if exp_results is not None else None
        self.exp_exception_regex = exp_results.exception_regex if exp_results is not None else None
        self.exp_exception_class = exp_results.exception_class if exp_results is not None else None
        if exp_results is None:
            self.write_generated_results()
        elif (comparation_results is
              not None) and (not comparation_results.success):
            self.write_comparation_results(comparation_results)

    def write_generated_results(self):
        self.xml = etree.Element(RESULT)
        el_query = etree.SubElement(self.xml, QUERY)
        el_query.text = self.query
        el_query_results = etree.SubElement(self.xml, QUERY_RESULT, {
            "name": self.query_name
        })
        if self.exception_message:
            self.write_exception(QUERY_RESULT, EXCEPTION, self.exception_type,
                                 self.exception_message, self.exception_class)
        else:
            el = self.xml.find(QUERY_RESULT)
            if self.query_type == "update":
                self.write_update(el, self.update_count)
            elif self.query_type == "select":
                self.write_cols(el, self.result_cols)
                self.write_results(el, self.num_rows, self.num_cols,
                                   self.result_cols, self.raw_results)

    def write_comparation_results(self, comparation_results):
        self.xml = etree.Element(QUERY_ERROR)
        etree.register_namespace('', URI_ERROR)
        el_error = self.xml
        el_query = etree.SubElement(el_error, QUERY_ERROR_QUERY)
        el_query.text = self.query
        el_failures = etree.SubElement(el_error, FAILURES)
        for failure in comparation_results.failures:
            etree.SubElement(
                el_failures,
                FAILURE).text = failure.message + " (Expected:" + unicode(
                    failure.expected) + "; Actual:" + unicode(
                        failure.actual) + ";)"
        act_el = etree.SubElement(el_error, ACTUAL_RESULT, {
            "name": str(self.query_name)
        })
        if self.exception_message:
            self.write_exception(ACTUAL_RESULT, EXCEPTION, self.exception_type,
                                 self.exception_message, self.exception_class)
        else:
            if self.query_type == "update":
                self.write_update(act_el, self.update_count)
            elif self.query_type == "select":
                self.write_cols(act_el, self.result_cols)
                self.write_results(act_el, self.num_rows, self.num_cols,
                                   self.result_cols, self.raw_results)
        ex_el = etree.SubElement(el_error, EXPECTED_RESULT, {
            "name": str(self.query_name)
        })
        if self.exp_exception_message or self.exp_exception_regex:
            self.write_exception(
                EXPECTED_RESULT, EXCEPTION, self.exp_exception_type,
                self.exp_exception_regex.pattern
                if self.exp_exception_regex is not None else
                self.exp_exception_message, self.exp_exception_class)
        else:
            if self.query_type == "update":
                self.write_update(ex_el, self.exp_update_count)
            elif self.query_type == "select":
                self.write_cols(ex_el, self.exp_result_cols)
                self.write_results(ex_el, self.exp_num_rows, self.exp_num_cols,
                                   self.exp_result_cols, self.exp_raw_results)

    def write_update(self, parent_element, update_count):
        el_update = etree.SubElement(parent_element, UPDATE, {
            "update-count": str(self.update_count)
        })

    def write_cols(self, parent_element, result_cols):
        el_select = etree.SubElement(parent_element, SELECT)
        for x in result_cols:
            etree.SubElement(el_select, DATA_ELEMENT, {
                "type": x.col_type
            }).text = x.name

    def write_results(self, parent_element, num_rows, num_cols, result_cols,
                      raw_results):
        el_table = etree.SubElement(parent_element, TABLE, {
            "row-count": str(num_rows),
            "column-count": str(num_cols)
        })
        for i in range(0, num_rows):
            el_row = etree.SubElement(el_table, TABLE_ROW)
            for j in range(0, num_cols):
                el_cell = etree.SubElement(el_row, TABLE_CELL)
                el_text = etree.SubElement(
                    el_cell, XMLNS + str(result_cols[j].col_type))
                text_string = raw_results[i][j]
                if text_string is not None:
                    el_text.text = unicode(text_string)

    def write_exception(self, parent_element_name, element_name,
                        exception_type, exception_message, exception_class):
        el_exc = etree.SubElement(
            self.xml.find(parent_element_name), element_name)
        el_exc_type = etree.SubElement(el_exc, EXCEPTION_TYPE)
        el_exc_type.text = str(exception_type)
        el_exc_message = etree.SubElement(el_exc, EXCEPTION_MESSAGE)
        el_exc_message.text = str(exception_message)
        el_exc_class = etree.SubElement(el_exc, EXCEPTION_CLASS)
        el_exc_class.text = str(exception_class)

    def export(self, filename):
        xmlstr = minidom.parseString(etree.tostring(
            self.xml)).toprettyxml(indent="    ")
        with open(filename, "w") as f:
            f.write(xmlstr.encode('utf-8'))
        self.xml.clear()
        #etree.ElementTree(self.xml).write(
        #    filename, xml_declaration=True, encoding='utf-8', method="xml")

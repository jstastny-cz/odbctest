from xml.etree import ElementTree as etree
from results_container import ResultsContainer,Column

class ResultsLoader(object):
	is_exception = False
	is_ordered = False
	filename = None
	xml = None
	query_name = None
	query = None
	columns = []
	rows = []
	exception_type = None
	exception_message = None
	exception_class = None
	
	def __init__(self,filename):
		self.filename = filename
		self.xml = etree.parse(filename).getroot()
		self.query_name = self.xml.find("queryResults").get("name")
		el_query = self.xml.find("query")
		self.query = el_query.text if not el_query is None else None
		self.is_except = self.xml.find("queryResults/exception")
		if self.is_except is not None:
			self.parse_exception()
		else:
			self.columns = self.parse_columns()
			self.rows = self.parse_rows()
		self.is_ordered = "order by" in self.query.lower() if not self.query is None else None
	

	def parse_columns(self):
		if self.is_except is not None: return None
		columns = []
		el_cols_container = self.xml.find("queryResults/select")
		for el in el_cols_container.getchildren():
			columns.append(Column(el.text, None, el.get("type")))
		return columns

	def parse_rows(self):
		if self.is_except is not None: return None
		rows = []
		el_rows_container = self.xml.find("queryResults/table")
		for el_row in el_rows_container.findall("tableRow"):
			row = []
			for el_cell in el_row.findall("tableCell"):
				
				if len(el_cell.getchildren())==0:
					row.append("null")
				else: 
					row.append(el_cell.getchildren()[0].text)
			rows.append(row)
		return rows

	def parse_exception(self):
		if self.is_except is None: return None
		el_exception = self.xml.find("queryResults/exception")
		self.exception_type = el_exception.find("exceptionType").text
		self.exception_message = el_exception.find("message").text
		self.exception_class = el_exception.find("class").text

	def load(self):
		return ResultsContainer(
			self.query_name
			,self.query
			,self.columns
			,self.rows
			,self.exception_type
			,self.exception_message
			,self.exception_class
		)


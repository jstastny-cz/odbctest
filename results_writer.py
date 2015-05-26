from lxml import etree
from collections import namedtuple
from results_container import Column
'''
Class ResultsBuilder serves to create xml file with specific format
from given information. 
'''
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

	def __init__(self,results):
		self.xml = etree.Element('root')
		self.raw_results = results.rows
		self.query = results.query
		self.result_cols = results.columns
		self.num_rows = len(self.raw_results)
		self.num_cols = len(self.result_cols)
		self.query_name = results.query_name
		self.exception_type = results.exception_type
		self.exception_message = results.exception_message
		self.exception_class = results.exception_class
		self.write_query()
		self.write_query_results()
		if self.exception_message:
			self.write_exception()
		else:
			self.write_cols()
			self.write_results()
		
	def write_query(self):
		el_query = etree.SubElement(self.xml,"query")
		el_query.text = self.query

	def write_query_results(self):
		el_query_results = etree.SubElement(self.xml,"queryResults",{"name":self.query_name})

	def write_cols(self):
		el_select = etree.SubElement(self.xml.find("queryResults"),"select")
		for x in self.result_cols:
			etree.SubElement(el_select,"dataElement",{"type":x.col_type}).text=x.name 

	def write_results(self):
		
		el_table = etree.SubElement(self.xml.find("queryResults"), "table", {"rowCount":str(self.num_rows),"columnCount":str(self.num_cols)})
		for i in range(0,self.num_rows):
			el_row = etree.SubElement(el_table,"tableRow")
			for j in range(0,self.num_cols):
				el_cell = etree.SubElement(el_row,"tableCell");
				el_text = etree.SubElement(el_cell,str(self.result_cols[j].col_type))
				text_string = self.raw_results[i][j]
				el_text.text=unicode(text_string)
		
	def write_exception(self):
		el_exc = etree.SubElement(self.xml.find("queryResults"), "exception")
		el_exc_type = etree.SubElement(el_exc,"exceptionType")
		el_exc_type.text = str(self.exception_type)
		el_exc_message = etree.SubElement(el_exc,"message")
		el_exc_message.text = str(self.exception_message)
		el_exc_class = etree.SubElement(el_exc,"class")
		el_exc_class.text = str(self.exception_class)

	def export(self,filename):
		etree.ElementTree(self.xml).write(filename, pretty_print=True,xml_declaration=True,encoding='utf-8')	
			
			

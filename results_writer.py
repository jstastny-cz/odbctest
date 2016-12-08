from xml.etree import ElementTree as etree
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

	def __init__(self,results, exp_results=None,comparation_results=None):
		self.xml = etree.Element('root')
		self.raw_results = results.rows
		self.query = results.query
		self.result_cols = results.columns
		self.num_rows = len(self.raw_results)
		self.num_cols = len(self.result_cols)
		self.exp_raw_results = exp_results.rows if exp_results is not None else None
		self.exp_result_cols = exp_results.columns if exp_results is not None else None
		self.exp_num_rows = len(self.exp_raw_results) if exp_results is not None else None 
		self.exp_num_cols = len(self.exp_result_cols) if exp_results is not None else None 
		self.query_name = results.query_name
		self.exception_type = results.exception_type
		self.exception_message = results.exception_message
		self.exception_class = results.exception_class
		self.exp_exception_type = exp_results.exception_type if exp_results is not None else None
		self.exp_exception_message = exp_results.exception_message if exp_results is not None else None
		self.exp_exception_class = exp_results.exception_class if exp_results is not None else None
		if exp_results is None:
			self.write_generated_results()
		elif (comparation_results is not None) and (not comparation_results.success):
			self.write_comparation_results(comparation_results)

	def write_generated_results(self):
		self.write_query()
		self.write_query_results()
		if self.exception_message:
			self.write_exception("queryResults","exception",self.exception_type,self.exception_message,self.exception_class)
		else:
			el = self.xml.find("queryResults")
			self.write_cols(el,self.result_cols)
			self.write_results(el,self.num_rows,self.num_cols,self.result_cols,self.raw_results)

	def write_comparation_results(self,comparation_results):
		self.write_query_results_compare_mode()
		for failure in comparation_results.failures:
			etree.SubElement(self.xml.find("queryResults"),"failureMessage").text = failure.message+" (Expected:"+unicode(failure.expected) +"; Actual:"+unicode(failure.actual)+";)"
		if self.exception_message:
			self.write_exception("queryResults","actualException",self.exception_type,self.exception_message,self.exception_class)
		else:
			act_el = etree.SubElement(self.xml.find("queryResults"),"actualQueryResults")
			self.write_cols(act_el,self.result_cols)
			self.write_results(act_el,self.num_rows,self.num_cols,self.result_cols,self.raw_results)
		if self.exp_exception_message:
			self.write_exception("queryResults","expectedException",self.exp_exception_type,self.exp_exception_message,self.exp_exception_class)
		else:
			ex_el = etree.SubElement(self.xml.find("queryResults"),"expectedQueryResults")
			self.write_cols(ex_el,self.exp_result_cols)
			self.write_results(ex_el,self.exp_num_rows,self.exp_num_cols,self.exp_result_cols,self.exp_raw_results)
		

	def write_query(self):
		el_query = etree.SubElement(self.xml,"query")
		el_query.text = self.query

	def write_query_results_compare_mode(self):
		el_query_results = etree.SubElement(self.xml,"queryResults",{"name":self.query_name,"value":self.query})
	
	def write_query_results(self):
		el_query_results = etree.SubElement(self.xml,"queryResults",{"name":self.query_name})

	def write_cols(self, parent_element, result_cols):
		el_select = etree.SubElement(parent_element,"select")
		for x in result_cols:
			etree.SubElement(el_select,"dataElement",{"type":x.col_type}).text=x.name 

	def write_results(self,parent_element,num_rows,num_cols,result_cols,raw_results):
		el_table = etree.SubElement(parent_element, "table", {"rowCount":str(num_rows),"columnCount":str(num_cols)})
		for i in range(0,num_rows):
			el_row = etree.SubElement(el_table,"tableRow")
			for j in range(0,num_cols):
				el_cell = etree.SubElement(el_row,"tableCell");
				el_text = etree.SubElement(el_cell,str(result_cols[j].col_type))
				text_string = raw_results[i][j]
				el_text.text=unicode(text_string)
		
	def write_exception(self,parent_element_name,element_name,exception_type,exception_message,exception_class):
		el_exc = etree.SubElement(self.xml.find(parent_element_name), element_name)
		el_exc_type = etree.SubElement(el_exc,"exceptionType")
		el_exc_type.text = str(exception_type)
		el_exc_message = etree.SubElement(el_exc,"message")
		el_exc_message.text = str(exception_message)
		el_exc_class = etree.SubElement(el_exc,"class")
		el_exc_class.text = str(exception_class)

	def export(self,filename):
		etree.ElementTree(self.xml).write(filename, xml_declaration=True,encoding='UTF-8')	
			
			

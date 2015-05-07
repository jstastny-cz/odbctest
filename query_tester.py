from query_reader import QueryReader
from query_runner import QueryRunner,DatabaseConnection
from results_writer import ResultsWriter
from results_loader import ResultsLoader
from results_comparator import ResultsComparator,ComparationResult
from os import listdir, makedirs
from os.path import isfile, join, exists
from lxml import etree
class QueryTester(object):
	db_connection = None 
	runner = None
	query_dir = None
	results_dir = None
	expected_dir = None

	def __init__(self, db_connection, query_dir, results_dir, expected_dir):
		self.db_connection = db_connection
		self.runner = QueryRunner(self.db_connection)	
		self.query_dir = query_dir
		self.results_dir = results_dir
		self.expected_dir = expected_dir

	def query_files(self, filename=None):
		query_files = []
		if filename: 
			query_files.append(filename)
		else:
			query_files += [ f for f in listdir(self.query_dir) if isfile(join(self.query_dir,f)) ]
		return filter(lambda x:x.endswith(".xml"),query_files)

	def generate_results(self,filename=None):
		reader = QueryReader()
		for filename in self.query_files(filename):	
			query_set_name = filename.split(".")[0]
			result_dirname = self.results_dir+"/"+query_set_name
			if not exists(result_dirname):
				makedirs(result_dirname)
			for query_tuple in reader.read(self.query_dir+"/"+filename):
				ResultsWriter(self.runner.run(*query_tuple)).export(result_dirname+"/"+query_set_name+"_"+query_tuple[0]+".xml")

	def compare_results(self,filename_given=None):
		reader = QueryReader()
		num_queries = 0
		num_errors = 0
		for filename in self.query_files(filename_given):
			query_set_name = filename.split(".")[0]
			for query_tuple in reader.read(self.query_dir+"/"+filename):
				num_queries +=1
				expected_loader = ResultsLoader(self.expected_dir+"/"+query_set_name+"/"+query_set_name+"_"+query_tuple[0]+".xml")
				expected_results = expected_loader.load() 
				actual_results = self.runner.run(*query_tuple)
				comp_result = ResultsComparator().compare(expected_results,actual_results)
				if not comp_result.success:
					num_errors +=1
					self.report_failures(query_set_name,expected_results, actual_results,comp_result.failures)
		print "succeeded:"+str(num_queries-num_errors)+", failed: "+str(num_errors)+", overall: "+str(num_queries)

	def report_failures(self, query_set_name,expected_results, actual_results,failures):
		exp_writer = ResultsWriter(expected_results)
		act_writer = ResultsWriter(actual_results)
		el_root = etree.Element('root')
		el_query_results = etree.SubElement(el_root,"queryResults",{"name":actual_results.query_name,"value":actual_results.query})
		for failure in failures:
			etree.SubElement(el_query_results,"failureMessage").text = failure.message+" (Expected:"+str(failure.expected) +"; Actual:"+str(failure.actual)+";)"
		el_actual_query_results = etree.SubElement(el_query_results,"actualQueryResults")
		el_expected_query_results = etree.SubElement(el_query_results,"expectedQueryResults")
		for el in exp_writer.xml.find("queryResults").getchildren():
			el_expected_query_results.append(el)
		for el in  act_writer.xml.find("queryResults").getchildren():
			el_actual_query_results.append(el)
		error_dirname = self.results_dir+"/errors-for-COMPARE/"+query_set_name + "/"
		#makedirs(error_dirname) if not exists(error_dirname)
		exists(error_dirname) or makedirs(error_dirname) 
		etree.ElementTree(el_root).write(error_dirname+query_set_name+"/"+actual_results.query_name+".err", pretty_print=True,xml_declaration=True,encoding='UTF-8')	

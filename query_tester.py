from query_reader import QueryReader
from query_runner import QueryRunner,DatabaseConnection
from results_writer import ResultsWriter
from results_loader import ResultsLoader
from results_comparator import ResultsComparator,ComparationResult
from os import listdir, makedirs
from os.path import isfile, isdir, join, exists
from lxml import etree
class QueryTester(object):
	db_connection = None 
	runner = None
	query_dir = None
	results_dir = None
	expected_dir = None
	scenario_name = None

	def __init__(self, db_connection, query_dir, results_dir, scenario_name, expected_dir):
		self.db_connection = db_connection
		self.runner = QueryRunner(self.db_connection)	
		self.query_dir = query_dir
		self.results_dir = results_dir
		self.expected_dir = expected_dir
		self.scenario_name = scenario_name

	def query_files(self,filename=None,query_files=[]):
		full_file_name = join(self.query_dir,filename) if filename else self.query_dir;
		if isfile(full_file_name):
			query_files.append(filename)
		elif isdir(full_file_name):
			for f in listdir(full_file_name):
				self.query_files(join(filename,f) if filename else f,query_files)
		return filter(lambda x:x.endswith(".xml"),query_files)

	def generate_results(self,filename=None):
		reader = QueryReader()
		result = True
		for filename in self.query_files(filename):
			query_set_full_name = filename.split(".")[0]
                        query_set_name = query_set_full_name.split("/")[-1]
			result_dirname = self.results_dir+"/"+ "/"+self.scenario_name+"/"+query_set_full_name
			if not exists(result_dirname):
				makedirs(result_dirname)
			for query_tuple in reader.read(self.query_dir+"/"+filename):
				results_container = self.runner.run(*query_tuple)
				result = result and (results_container is not None)
				ResultsWriter(results_container).export(result_dirname+"/"+query_set_name+"_"+query_tuple[0]+".xml")
		return result

	def compare_results(self,filename_given=None):
		reader = QueryReader()
		scenario_name=self.scenario_name
		sc_num_queries=0
		sc_num_errors=0
		sc_num_skipped=0
		if not exists(self.results_dir):
			makedirs(self.results_dir)
		for filename in self.query_files(filename_given):
			num_queries = 0
			num_errors = 0
			query_set_full_name = filename.split(".")[0]
                        query_set_name = query_set_full_name.split("/")[-1]
                        print "Scenario:",scenario_name,"Query Set:", query_set_full_name
			for query_tuple in reader.read(self.query_dir+"/"+filename):
				num_queries +=1
				try:	
					expected_filename = self.expected_dir+"/"+query_set_full_name+"/"+query_set_name+"_"+query_tuple[0]+".xml"
					expected_loader = ResultsLoader(expected_filename)
					expected_results = expected_loader.load() 
					actual_results = self.runner.run(*query_tuple)
					comp_result = ResultsComparator().compare(expected_results,actual_results)
					if not comp_result.success:
						num_errors +=1
						self.report_failures(query_set_name,expected_results, actual_results,comp_result)
				except(IOError,etree.XMLSyntaxError) as e:
					num_errors+=1
					print "ERROR: Couldn't load expected results file "+expected_filename , e
			sc_num_queries+=num_queries
			sc_num_errors+=num_errors
		filepath_summary_totals = self.results_dir+"/Summary_totals.txt"
		if not exists(filepath_summary_totals):
			f_totals = open(filepath_summary_totals,"w")
			self.write_summary_totals_header(f_totals)
			f_totals.close()
		f_totals = open(filepath_summary_totals,"a")
		f_totals.write(scenario_name + "\t" + str(sc_num_queries-sc_num_errors)+"\t"+str(sc_num_errors)+"\t"+str(sc_num_queries)+"\t"+str(sc_num_skipped)+"\n")
		f_totals.close()
                return (sc_num_errors+sc_num_skipped)==0

	def report_failures(self, query_set_name,expected_results, actual_results,comparation_results):
		writer = ResultsWriter(actual_results,expected_results,comparation_results)
		error_dirname = self.results_dir+"/"+self.scenario_name+"/errors_for_COMPARE/"
		if not exists(error_dirname):
			makedirs(error_dirname)
		writer.export(error_dirname+"/"+query_set_name+"_"+actual_results.query_name+".err")

	def write_summary_totals_header(self, f):
		f.write("==================\n");
		f.write("TestResult Summary\n");
		f.write("==================\n");
		f.write("Scenario\tPass\tFail\tTotal\tSkipped\n\n");

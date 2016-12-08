from results_container import ResultsContainer,Column
from operator import itemgetter
from collections import namedtuple

ComparationFailure= namedtuple("ComparationFailure","message expected actual query_name query")
ComparationResult = namedtuple("ComparationResult","success failures")
class ResultsComparator(object):

	def __init__(self):
		self.failures = []
	
	def compare(self, expected_results, actual_results):
		result = True
		if (not expected_results.query is None and expected_results.query != actual_results.query):
			self.failures.append(ComparationFailure("Query mismatch for "+expected_results.query_name,expected_results.query,actual_results.query, expected_results.query_name,None))
			result = False
		if expected_results.is_exception and not actual_results.is_exception:
			self.failures.append(ComparationFailure("Comparation error: exception expected but didn't occur.",expected_results.exception_type,None,expected_results.query_name,expected_results.query))
			result = False
		if not expected_results.is_exception and actual_results.is_exception:
			self.failures.append(ComparationFailure("Comparation error: unexpected exception occured.",None,actual_results.exception_type,expected_results.query_name,expected_results.query))
			result = False
		result_columns = self.compare_columns(expected_results, actual_results)
		result = result and result_columns 
		result_rows =  self.compare_rows(expected_results, actual_results)
		result = result and result_rows 
		result_exceptions = self.compare_exceptions(expected_results, actual_results)
		result = result and result_exceptions
		return ComparationResult(result,self.failures)

	def compare_columns(self, ex_res, ac_res):
		failures = []
		if len(ex_res.columns) != len(ac_res.columns):
			failures.append(ComparationFailure("Compare error: Unexpected number of columns returned.",len(ex_res.columns),len(ac_res.columns),ac_res.query_name,ac_res.query))
		else:
			for col1, col2 in [(ex_res.columns[i],ac_res.columns[i]) for i in range(0,len(ex_res.columns))]:
				if col1.name.lower()!=col2.name.lower():
					failures.append(ComparationFailure("Compare error: column names differ.",col1.name,col2.name,ac_res.query_name, ac_res.query))
				if col1.col_type!=col2.col_type:				
					failures.append(ComparationFailure("Compare error: column types differ.",col1.col_type,col2.col_type,ac_res.query_name, ac_res.query))
		self.failures+=failures
		return len(failures)==0

	def compare_rows(self, ex_res, ac_res):
		failures = []
		if len(ex_res.rows)!=len(ac_res.rows):
			failures.append(ComparationFailure("Compare error: Unexpected number of rows returned.",len(ex_res.rows),len(ac_res.rows),ac_res.query_name,ac_res.query))
		else:
			all_sorted = ex_res.is_sorted and ac_res.is_sorted

			sort_order = tuple([x for x in range(0,len(ex_res.rows[0])if len(ex_res.rows)>0 else 1)])
			
			if not all_sorted and len(sort_order)>0:
				ex_rows=sorted(ex_res.rows,key = itemgetter(*sort_order))
				ac_rows=sorted(ac_res.rows, key=itemgetter(*sort_order))
			else:
				ex_rows = ex_res.rows
				ac_rows = ac_res.rows
			
			
			for row1, row2 in [(ex_rows[i],ac_rows[i]) for i in range(0,len(ex_rows))]:
				if len(row1)!=len(row2):
					failures.append(ComparationFailure("Compare error: number of cells in row differ.",len(row1),len(row2),ac_res.query_name,ac_res.query))
				for ex_cell,ac_cell in [(row1[i],row2[i])for i in range(0, len(row1))]:
					if((isinstance(ex_cell,str) and isinstance(ac_cell,str) and str(ex_cell)!=str(ac_cell))or(isinstance(ex_cell,unicode)and isinstance(ac_cell,unicode)and ex_cell.encode('utf-8')!=ac_cell.encode('utf-8'))):
						failures.append(ComparationFailure("Compare error: value mismatch in column "+str((self.index_id(row2,ac_cell)+1))+ " and row "+ str(self.index_id(ac_res.rows,row2)+1),str(ex_cell).encode('UTF-8'),str(ac_cell).encode('UTF-8'),ac_res.query_name.encode('UTF-8'), ac_res.query.encode('UTF-8')))
		self.failures += failures
		return len(failures)==0

	def index_id(self,a_list, elem):
#		return a_list.index(elem)
		return (index for index, item in enumerate(a_list) if item is elem).next()

	def compare_exceptions(self, ex_res,ac_res):
		failures=[]
		if (ex_res.is_exception and ac_res.is_exception):
			if ex_res.exception_type != ac_res.exception_type:
				failures.append(ComparationFailure("Compare error: exception type mismatch",str(ex_res.exception_type),str(ac_res.exception_type),ac_res.query_name,ac_res.query))
			if ex_res.exception_message != ac_res.exception_message:
				failures.append(ComparationFailure("Compare error: exception message mismatch",str(ex_res.exception_message),str(ac_res.exception_message),ac_res.query_name,ac_res.query))
			if ex_res.exception_class != ac_res.exception_class:
				failures.append(ComparationFailure("Compare error: exception class mismatch",str(ex_res.exception_class),str(ac_res.exception_class),ac_res.query_name,ac_res.query))
		self.failures+=failures
		return len(failures)==0
		

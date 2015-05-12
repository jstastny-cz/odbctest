import sys
import getopt
import getpass
from query_tester import QueryTester
from query_runner import DatabaseConnection
COMPARE_MODE = "COMPARE"
GENERATE_MODE = "GENERATE"
sys.settrace
class Configuration(object):
	def set_mode(self,mode_string):
		self.mode = mode_string

	def set_odbcdriver(self,odbcdriver):
		self.odbcdriver=odbcdriver

	def set_server(self, server):
		self.server=server

	def set_port(self,port):
		self.port = port

	def set_database(self,database):
		self.database=database

	def set_username(self,username):
		self.username = username

	def set_password(self,password):
		self.password = password

	def set_querydir(self,querydir):
		self.querydir = querydir

	def set_queryfile(self,queryfile):
		self.queryfile = queryfile

	def set_resultsdir(self,resultsdir):
		self.resultsdir = resultsdir

	def set_expecteddir(self,expecteddir):
		self.expecteddir = expecteddir
	
	def __str__(self):
		return str(self.__dict__)

	def dict(self):
		return self.__dict__

def main(argv):
	opts, args = getopt.getopt(argv,"hm:o:s:d:u:p:q:r:e:",["help","mode=","odbcdriver=","server=","database=","username=","password=","querydir=","resultsdir=","expecteddir="])
	# if password wasn't provided in cli, ask for it
	config = Configuration()
	for opt,value in opts:
		if opt in ['-h','--help']:		
			print_help()
			return
		elif opt in ['-m','--mode']:
			config.set_mode(value)
		elif opt in ['-o','--odbcdriver']:
			config.set_odbcdriver(value)
		elif opt in ['-s','--server']:
			parts = value.split(":")
			config.set_server(parts[0])
			if(len(parts)==2):
				config.set_port(parts[1])
		elif opt in ['-d','--database']:
			config.set_database(value)
		elif opt in ['-u','--username']:
			config.set_username(value)
		elif opt in ['-p','--password']:
			if value==".":value = getpass.getpass()
			config.set_password(value)
		elif opt in ['-q','--querydir']:
			parts = value.split("/")
			print parts
			config.set_querydir("/".join(parts[:-1]) if value.endswith(".xml") else value)
			config.set_queryfile(parts[-1] if value.endswith(".xml") else None)
			print config
		elif opt in ['-r','--resultsdir']:
			config.set_resultsdir(value)
		elif opt in ['-e','--expecteddir']:
			config.set_expecteddir(value)
	if not hasattr(config,'mode'):
		help_and_exit('No operation mode specified')
	if not hasattr(config,'odbcdriver'):
		help_and_exit('No odbcdriver specified.')
	if not hasattr(config,'server'):
		help_and_exit('No server specified')
	if not hasattr(config,'port'):
		help_and_exit('No server port specified')
	if not hasattr(config,'database'):
		help_and_exit('No database name specified')
	if not hasattr(config,'username'):
		help_and_exit('No username specified')
	if not hasattr(config,'password'):
		help_and_exit('No password specified')
	if not hasattr(config,'querydir'):
		help_and_exit('You should specify an xml or a directory containing queries.')
	if not hasattr(config,'resultsdir'):
		help_and_exit('You should specify a directory to save results')
	if config.mode==COMPARE_MODE and not hasattr(config,'expecteddir'):
		help_and_exit('For '+COMPARE_MODE+' mode there has to be expecteddir specified')


	db_conn = DatabaseConnection(config.odbcdriver,config.server,config.port,config.database,config.username,config.password)
	tester = QueryTester(db_conn,config.querydir,config.resultsdir,config.expecteddir if hasattr(config,"expecteddir") else None)
	if config.mode==GENERATE_MODE:
		tester.generate_results(config.queryfile if hasattr(config,"queryfile")else None)
	elif config.mode==COMPARE_MODE:
		tester.compare_results(config.queryfile if hasattr(config,"queryfile")else None)

def help_and_exit(message):
	print message
	print_help()
	sys.exit(2)



def print_help():
	print "To use this script:"
	print "#####"
	print "GENERATE mode:"
	print "odbctest.py -m GENERATE -o <odbcdriver> -s <server>:<port> -d <database> -u <username> -p <password> -q <queryfile> -r <resultsdir>"
	print "or"
	print "odbctest.py" 
	print " --mode GENERATE"
	print " --odbcdriver <odbcdriver>"
	print " --server <server>:<port>"
	print " --database <database>"
	print " --username <username>"
	print " --password <password>"
	print " --queryfile <queryfile>"
	print " --resultsdir <resultsdir>"
	print "#####"
	print "COMPARE mode:"
	print "odbctest.py -m COMPARE -o <odbcdriver> -s <server>:<port> -d <database> -u <username> -p <password> -q <queryfile> -r <resultsdir> -e <expectedresultsdir>"
	print "or"
	print "odbctest.py"
	print " --mode COMPARE"
	print " --odbcdriver <odbcdriver>"
	print " --server <server>:<port>"
	print " --database <database>"
	print " --username <username>"
	print " --password <password>"
	print " --queryfile <queryfile>"
	print " --resultsdir <resultsdir>"
	print " --expecteddir <expectedresultsdir>"
	
if __name__ == "__main__":
   main(sys.argv[1:])



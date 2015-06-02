import pyodbc
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

    def __init__(self,driver, server, port,database,user,password):
        self.driver=driver
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def connect(self):
        self.connection = pyodbc.connect('DRIVER={'+self.driver+'};SERVER='+self.server+';PORT='+self.port+';DATABASE='+self.database+';UID='+self.user+';PWD='+self.password)
        self.cursor = self.connection.cursor()
        self.db_tables = deepcopy([(x[2],x[1],x[0]) for x in self.cursor.tables()])

    def disconnect(self):
        self.clean()
        self.connection.close()

    def clean(self):
        self.cursor.close()
        self.rows = None
        self.tables = None
        self.db_tables = None
        
    def clear_cache(self):
	self.db_columns = {}   

    def destroy(self):
	self.clear_cache()
 
    def execute_query(self, query):
        self.cursor.execute(query)
        self.rows = self.cursor.fetchall()
        return map(lambda x:[data.encode('utf-8') if isinstance(data,unicode) else str(data) for data in [(x[cell] if not x[cell] is None else "null") for cell in range(0,len(x))]],[row for row in self.rows]);

    def parse_columns_from_query(self,query):
       
	columns_retreived = [col[0] for col in self.cursor.description]
	columns = []
        parsed = sqlparse.parse(query)
        stmt = parsed[0]
        cast_type = False
        for token in stmt.tokens:
	    num_of_cols = len(columns)
            if token.ttype==sqlparse.tokens.Keyword and str(token).lower()=="from":
                break
            if token.ttype==None and cast_type:
                columns.append(Column(columns_retreived[num_of_cols],"CAST("+str(token)+")",self.decide_type_cast(query,num_of_cols)))
                cast_type=False
	    elif token.ttype==sqlparse.tokens.Wildcard and str(token)=="*":
		for col in self.cursor.description:
		    columns.append(Column(col[0],None,self.get_type_of_column(None,col[0],query)))
            elif token.ttype==None:
		# maps to columns='table.column', functions('string','string',column,...) and AS following them; applies in any order
		select_cols = re.findall("\s*([\w\.]+\((([\w\.]+|'.+'),\s*)*([\w\.]+|'.+')+\)\s*(AS\s+\w+)*,?|([\w\.]+\s*(AS\s+\w+)*))+\s*",str(token),re.IGNORECASE)
                for col in map(lambda x:x[0].strip(),select_cols):
		    if col.endswith(","):
			col = col[:-1]
		    parts = col.split(".")
                    alias = parts[0].strip() if re.search("FROM\s+.*\s+AS\s+\w*",query,re.IGNORECASE) and len(parts)==2 else None
                    col_name = parts[-1].split(".")[-1].strip()
		    columns.append(Column(columns_retreived[num_of_cols],col,self.get_type_of_column(alias,col_name,query)))
                    num_of_cols = len(columns)
            elif token.ttype==sqlparse.tokens.Keyword and str(token).lower()=="cast":
                cast_type=True
	return columns
    
    def decide_type_cast(self, query, column_offset):
        castedType = "undefined"
        parsed = sqlparse.parse(query)
        stmt = parsed[0]
        tokenList = sqlparse.sql.TokenList(stmt.tokens)
        highest_idx = -1
        lowest_idx = -1
        for token in stmt.tokens:
            if(token.ttype in [sqlparse.tokens.Keyword,None] and lowest_idx<0):
                lowest_idx = token 
            if(token.ttype == sqlparse.tokens.Keyword and str(token).lower()=="from"):
                highest_idx =  token
        
        selectColumnsList = tokenList.tokens_between(lowest_idx,highest_idx)
        col_count = 0
        for tok in selectColumnsList:
            if(col_count>column_offset): break
            old_count = col_count
            if(tok.ttype == None):
                col_count += len(str(tok).split(","))
                if(col_count>=column_offset):
                    neededOffset = column_offset-old_count-1
                    castBody = str(tok).split(",")[neededOffset]
                    castParsed=castBody.split(" ")
                    if(len(castParsed)==3):
                        castedType = castParsed[2].split(")")[0]
        return castedType

    def decide_column_type(self, table,col_name):
	# catalog.schema.table
	parsed_table = re.match("\s*((((\w+)\.)*(\w+)\.)*(\"([\w\.]+)\"|\w+))\s*",table)

        table_qualified = [parsed_table.group(7) or parsed_table.group(6),parsed_table.group(5),parsed_table.group(4)]
        table_qualified_tuple = tuple(table_qualified)
        result_table_tuple = self.get_table_tuple(table_qualified_tuple)
        
	if not table.lower().strip() in self.db_columns:
		self.db_columns[table.lower().strip()]=[x for x in self.cursor.columns(table=result_table_tuple[0],schema=result_table_tuple[1],catalog=result_table_tuple[2])]
	column_type_list = map(lambda x:x.type_name.lower(),filter(lambda y:y.column_name.lower()==col_name.lower(),self.db_columns[table.lower().strip()]))
	column_type = column_type_list[0] if len(column_type_list)>0 else "undefined"
        return column_type

    def get_table_tuple(self,table_qualified_tuple):
	result_table_tuple = None
	for table_tuple in self.db_tables:
	    result = table_qualified_tuple[0].lower()==table_tuple[0].lower()
            if table_qualified_tuple[1]!=None:
                result = result and (table_qualified_tuple[1].lower()==table_tuple[1].lower())
            if table_qualified_tuple[2]!=None:
                result = result and (table_qualified_tuple[2].lower()==table_tuple[2].lower())
            if result:
                result_table_tuple=table_tuple
                break
	return result_table_tuple
	


    def get_type_of_column(self,alias,col_name,query):
	type="undefined"
	as_in_colname = re.match("(.*)\s+AS.*",col_name,re.IGNORECASE)
	if as_in_colname:
	    col_name=as_in_colname.group(1)
        table_array=[]
	if(re.match("SELECT.*"+((re.escape(alias)+"\.") if alias else "")+re.escape(col_name)+".*FROM.*",query,re.IGNORECASE) or re.match("SELECT\s+\*\s+FROM.*",query,re.IGNORECASE)):
            tables = self.get_queried_tables(query)
	    table_array = self.search_queried_tables(alias, col_name,query, tables)
	if len(table_array)==1:
	    type = self.decide_column_type(table_array[0],col_name)
        return type
#
#   Method get_query_with_alias retreives query which is denoted by given alias
#   For query 'SELECT * FROM TABLE schema.table as a' 
#   and given alias 'a' it returns 'schema.table as a'
#
    def get_query_with_alias(self, alias, tables):
        for tuple in tables:
            if(tuple[0].lower()==alias.lower()):
                return tuple[2]
            elif not isinstance(tuple[1],basestring):
                returned = self.get_query_with_alias(alias, tuple[1])
                if returned != None: return returned
#
#   Method search_queried_tables returns table identifiers from given structure
#   which are assigned to given alias.      
#
    def search_queried_tables(self, alias,col_name, query, queried_tables):
        found_tables = []
        found = []
        for table in queried_tables:
            if alias!=None and table[0]!=None and table[0].lower()==alias.lower():
                if isinstance(table[1],basestring):
                    found.append(table[1])
                else:
                    inner_parsed = re.split(".*\((.*)\).*",table[2],re.IGNORECASE)
                    inner_query = inner_parsed if len(inner_parsed)<3 else inner_parsed[1:len(inner_parsed)-1][0]
		    inner_split = re.split("\s*(\w*)\."+col_name,inner_query,re.IGNORECASE)
                    inner_alias = None if len(inner_split)<3 else inner_split[1]
                    found += self.search_queried_tables(inner_alias,col_name,inner_query,table[1])   
                break;
            elif alias!=None and not isinstance(table[1],basestring):
             found += self.search_queried_tables(alias,col_name,query,table[1])
            elif alias==None and not isinstance(table[1],basestring):
             found += self.search_queried_tables(None,col_name,query,table[1])
            elif alias==None:
                found.append(table[1])
            if found !=None and len(found)>0:
                return found
        return found;       
    
    def get_queried_tables(self,query):
        if self.tables == None:
            self.tables = self.queried_tables(query)
	return self.tables
#
#   Method queried_tables parses given query and creates structure of aliases and table names
#   Format of structure is:
#       [
#           (
#               alias1,
#               [(,[],),..],
#               inner_query1
#           )
#       ,...]
#   While lowest level nodes are (aliasN, schema.tableN, schema.tableN as aliasN)
#   For query: 'SELECT * FROM schema.table1 as a INNER JOIN schema.table2 as b ...' it forms
#   [(a,'schema.table1',schema.table1 as a),(b,schema.table2,schema.table2 as b)]
#   For query: 'SELECT * FROM (SELECT * from schema.table1 as inner_a INNER JOIN schema.table2 as inner_b ON ...) as a INNER JOIN schema.table3 as b on ....'
#   [(a,[(inner_a,'schema.table1',schema.table1 as inner_a),(inner_b,'schema.table2',schema.table2 as inner_b)],(SELECT * from schema.table1 as inner_a INNER JOIN schema.table2 as inner_b)),
#   (b,'schema.table3',schema.table3 as b)
#   ]
#
    def queried_tables(self, query):
        defined_tables = []
	in_brackets = re.search("^\((.*)\)$",query);
	if in_brackets:
		query = in_brackets.group(1)
        found = re.search("^\((.*)\).*AS\s+(\w+).*$",query,re.IGNORECASE)
	if found:
		defined_tables.append((found.group(2),self.queried_tables(found.group(1)),found.group(1)))
		return defined_tables
        	
	parsed = sqlparse.parse(query)
        stmt = parsed[0]
        tokenList = sqlparse.sql.TokenList(stmt.tokens)
        from_idxs = []
        inner_selects_idxs = []
        # loop through all parsed tokens
        for token in stmt.tokens:
            token_string = unicode(token)
	    if(token.ttype ==sqlparse.tokens.Keyword and token_string.lower() in ["from","inner join","full outer join","right outer join","left outer join","union all"]):
                from_idxs.append(stmt.tokens.index(token))
            if(token.ttype==None and ( token_string.lower().startswith("select") or token_string.lower().startswith("(select"))):
                inner_select = token_string
                inner_selects_idxs.append(stmt.tokens.index(token))
                #parse table alias
                table_alias = self.parse_relation_alias(inner_select)[0]
                children = self.queried_tables(inner_select.strip())
                # if inner_select contains only simple select (len(children)==1) get only string value
                # if is join, work with whole structure
                child = children[0][1] if len(children)==1 and isinstance(children[0][1],basestring) else children
                defined_tables.append((table_alias,child,inner_select))
        top_level_from_idxs = filter(lambda x:x+2 not in inner_selects_idxs, from_idxs) # indexes of top-level 'FROM' query tokens
        # return tokens which follows 'FROM' clauses denoted by top_level_from_idxs 
	with_possible_commas = filter(lambda x: (stmt.tokens.index(x)-2) in top_level_from_idxs ,stmt.tokens)
	without_commas = []
	for entry in with_possible_commas:
		if not "," in str(entry):
			without_commas.append(str(entry))
		else:
			without_commas+=str(entry).split(",")
	return defined_tables + map(lambda y: self.parse_relation_alias(str(y)),without_commas)

    def parse_relation_alias(self, query):
        # parse the 'as' part of query
        parsed = re.split("\W+(as|AS|As|aS)\W+",query) # can't set re.ignorecase!!
        return (None if len(parsed)<3 else parsed[len(parsed)-1],query if len(parsed)<3 else parsed[0:len(parsed)-2][0],query)

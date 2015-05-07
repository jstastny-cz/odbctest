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
        return map(lambda x:[str(x[cell] if not x[cell] is None else "null") for cell in range(0,len(x))],[row for row in self.rows]);

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
            elif token.ttype==None:
                for col in map(lambda x:x.strip(),str(token).split(",")):
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
	table_parts = table.split(".")
        table_parts.reverse()
        table_qualified = [None,None,None]
        for i in range(0,len(table_parts)):
            table_qualified[i]=table_parts[i]
        table_qualified_tuple = tuple(table_qualified)
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
        if not table.lower() in self.db_columns:
            self.db_columns[table.lower()]=[x for x in self.cursor.columns(table=result_table_tuple[0],schema=result_table_tuple[1],catalog=result_table_tuple[2])]
	column_type_list = map(lambda x:x.type_name.lower(),filter(lambda y:y.column_name.lower()==col_name.lower(),self.db_columns[table.lower()]))
	column_type = column_type_list[0] if len(column_type_list)>0 else "undefined"
        return column_type

    def get_type_of_column(self,alias,col_name,query):
	type=None
        table_array = []
        if(re.match("SELECT.*"+((alias+"\.") if alias else "")+col_name+".*FROM.*",query,re.IGNORECASE)):
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
                    inner_alias = None if inner_split<3 else inner_split[1]
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
        if re.match("^\(.*\).*$",query,re.IGNORECASE):
            query = query.split(")")[0][1:]
        parsed = sqlparse.parse(query)
        stmt = parsed[0]
        tokenList = sqlparse.sql.TokenList(stmt.tokens)
        from_idxs = []
        inner_selects_idxs = []
        defined_tables = []
        # loop through all parsed tokens
        for token in stmt.tokens:
            if(token.ttype ==sqlparse.tokens.Keyword and str(token).lower() in ["from","inner join"]):
                from_idxs.append(stmt.tokens.index(token))
            if(len(stmt.tokens)>1 and token.ttype==None and ( str(token).lower().startswith("select") or str(token).lower().startswith("(select"))):
                inner_select = str(token)
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
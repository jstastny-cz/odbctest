from collections import namedtuple
Column = namedtuple('Column', 'name name_in_query col_type')


class ResultsContainer(object):
    query_name = None
    query = None
    query_type = None
    update_count = None
    columns = []
    rows = []
    is_exception = False
    exception_type = None
    exception_message = None
    exception_regex = None
    exception_class = None

    def __init__(self,
                 query_name,
                 query,
                 query_type,
                 update_count,
                 columns,
                 rows,
                 exception_type=None,
                 exception_message=None,
                 exception_regex=None,
                 exception_class=None):
        self.query_name = query_name
        self.query = query
        self.query_type = query_type
        self.update_count = update_count
        self.columns = columns
        self.rows = rows
        self.is_exception = exception_type or exception_message or exception_class
        self.exception_type = exception_type
        self.exception_message = exception_message
        self.exception_regex = exception_regex
        self.exception_class = exception_class
        self.is_sorted = "order by" in query.lower(
        ) if not query is None else None

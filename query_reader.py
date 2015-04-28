from lxml import etree

class QueryReader(object):
    def read(self,filename):
        xml = etree.parse(filename).getroot()
        queries = []
        for query in xml.findall("query"):
            queries.append((query.get("name"),query.text))
        return queries

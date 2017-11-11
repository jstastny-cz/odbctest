from xml.etree import ElementTree as etree
from results_container import ResultsContainer, Column


class TypeEquality(object):
    '''
    In defitions there are equality classes defined. First row is always the type which is common to whipper test expected results.
    Any output will containt these types, not the odbc specific types.
    '''
    definitions = [["byte", "int2"], ["short", "int2"], [
        "integer", "int4", "int"
    ], ["long", "int8"], ["string", "varchar", "text"], ["float", "float4"],
                   ["double", "float8"], ["timestamp", "timestamptz"],
                   ["bigdecimal", "numeric"], ["biginteger", "numeric"]]

    def is_equal(self, fst, snd):
        if fst == snd:
            return True
        if self.get_equality_class(fst, snd) is not None:
            return True
        return False

    def get_equality_class(self, fst, snd):
        lists = [l for l in self.definitions if fst in l and snd in l]
        if len(lists) > 0:
            return lists[0]
        return None

    def get_referential_type(self, fst, snd):
        cl = self.get_equality_class(fst, snd)
        if cl is not None and len(cl) > 0:
            return cl[0]
        return fst

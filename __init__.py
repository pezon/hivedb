"""hivedb - A DB API v2.0 compatible interface for HIVE CLI

This package is a wrapper around HIVE CLI.  Until HiveThrift
stops sucking, it will continue to use HIVE CLI.

connect() -- connects to server
"""

__author__ = "Peter Pezon <peter.pezon@escapemg.com>"
version_info = (0,1,0)
__version__ = '0.1.0'

threadsafety = 1
apilevle = '2.0'
paramstyle = 'format'

try:
    frozenset
except NameError:
    from sets import ImmutableSet as frozenset

class DBAPISet(frozenset):
    """A special type of set for which A == x is true if A is a 
    DBAPISet and x is a member of that set."""

    def __eq__(self, other):
        if isinstance(other, DBAPISet):
            return not self.difference(other)
        return other in self

def connect(*args, **kwargs):
    """ Factory function for connections.Connection. """
    from connections import Connection
    return Connection(*args, **kwargs)

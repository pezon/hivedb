"""hivedb - A DB API v2.0 compatible interface to MySQL.

This package is a wrapper around hive streaming.

connect() -- connects to server
"""

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

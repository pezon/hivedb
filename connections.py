"""
This module implements connections for hivedb. There is only
one class: Connection.  You may override Connection.default_cursor
with a non-standard Cursor class.
"""

import cursors
from errors import Warning, Error, InterfaceError, DataError, \
    DatabaseError, OperationalError, IntegrityError, InternalError, \
    NotSupportedError, ProgrammingError
import os

def defaulterrorhandler(connection, cursor, errorclass, errorvalue):
    """
    If cursor is not None, (errorclass, errorvalue) is appended to
    cursor.messages; otherwise it is appended to connection.messages.
    Then errorclass is raised with errorvalue as the value.

    You can override this with your own error handler by assigning it
    to the instance.
    """
    error = errorclass, errorvalue
    if cursor:
        cursor.messages.append(error)
    else:
        connection.messages.append(error)
    del cursor
    del connection
    raise errorclass, errorvalue

class Connection(object):
    """Hive database Connection Object"""
    default_cursor = cursors.Cursor

    def __init__(self, *args, **kwargs):
        """
        Create a configuration of hive.  It is strongly recommende
        that you only use keyword parameters.
        
        jar
            string, HIVE jar to use.  defaults to $HIVE_HOME

        port
            integer, TCP/IP port to connect to

        user
            string, user to connect as

        verbose
            bool, default true.  displays hive progress messages.

        init_command
            command which is run once the connection is created
            (not implemented)

        write_access
            bool, issues hive queries as sudo

        cursorclass
            class object, used to create cursors (keyword only)
        """

        self.cursorclass = kwargs.pop('cursorclass', self.default_cursor)
        self.user = kwargs.pop('user', os.environ.get('USER', None))
        self.port = kwargs.pop('port', None)
        self.write_access = kwargs.pop('write_access', False)
        self.verbose = kwargs.pop('verbose', True)
        self.closed = False
        self.messages = []

    def cursor(self, cursorclass=None):
        """
        Create a cursor on which queries may be performed. The
        optional cursorclass parameter is used to create the
        Cursor. By default, self.cursorclass=cursors.Cursor 
        is used.
        """
        if self.closed:
            raise Error("Connection is closed.")
        return (cursorclass or self.cursorclass)(self)

    def __enter__(self):
        return self.cursor()
    
    def close(self):
        self.closed = True

    def show_warnings(self):
        """
        Return detailed information about warnigns as a sequence
        of tuples of (Level, Code, Message).
        """

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError

    errorhandler = defaulterrorhandler

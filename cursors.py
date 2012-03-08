"""
Hive db cursors
This module implements cursors of various types for hive db.
By default, hivedb uses Cursor class.
"""

import sys
from query import Query
from errors import Warning, Error, InterfaceError, DataError, \
    DatabaseError, OperationalError, IntegrityError, InternalError, \
    NotSupportedError, ProgrammingError

class BaseCursor(object):

    """A base for Cursor classes. Useful attributes:

    description
        A tuple of DB API 7-tuples describing the columns in
        the last executed query; see PEP-249 for details.
        (Probably not supported for HIVE without hacking).

    arraysize
        default number of rows fetchmany() will fetch
    """
    
    def __init__(self, connection):
        self.connection = connection
        self.description = None # shit doesn't really exist for hive
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self.lastrowid = None
        self.messages = []
        self.errorhandler = connection.errorhandler
        self._result = None
        self._info = None
        self.rownumber = None

    def __del__(self):
        self.close()
        self.errorhandler = None
        self._result = None

    def close(self):
        if not self.connection:
            return
        while self.nextset():
            pass
        self.connection = None

    def _check_executed(self):
        if not self._executed:
            self.errorhandler(self, ProgrammingError, "execute() first")

    def nextset(self):
        """Advance to the next result set
        Returns None if there are no more result sets."""
        if self._executed:
            self.fetchall()
        del self.messages[:]

    def _do_get_result(self):
        """ @??? WHAT DOES THIS DO??? """
        # execute query, reset params
        self._result = self._get_result()
        self.rowcount = -1 # affected rows
        self.rownumber = 0
        self.description = self._result and self._result.description() or None
        self.lastrowid = 0 # none
        self._info = None

    def setinputsizes(self, *args):
        """ Does nothing, required by DB API. """

    def setoutputsizes(self, *args):
        """ Does nothing, required by DB API. """

    def _get_db(self):
        if not self.connection:
            self.errorhandler(self, ProgrammingError, "cursor closed")
        return self.connection

    def execute(self, query, args=None):

        """Execute a query.

        query -- string, query to execute on server
        args -- optional sequence or mapping, parameters to use with query.

        Note:  If args is a sequence, then %s must be used as the
        parameter placeholder in the query.  If a mapping is used,
        %(key)s must be used as the placeholder.

        Returns long integer rows affected, if any

        """
        del self.messages[:]
        db = self._get_db()
        if args is not None:
            query = query % args
        try:
            r = self._query(query)
        except TypeError, m:
            if m.args[0] in ("not enough arguments for format string",
                             "not all arguments converted"):
                self.messages.append((ProgrammingError, m.args[0]))
                self.errorhandler(self, ProgrammingError, m.args[0])
            else:
                self.messages.append((TypeError, m))
                self.errorhandler(self, TypeError, m)
        except:
            exc, value, tb = sys.exc_info()
            del tb
            self.messages.append((exc, value))
            self.errorhandler(self, exc, value)
        self._executed = query
        return r

    def executemany(self, query, sequence_of_args):
        """ Execute a multi-row query.

        query -- string, query to execute on server

        args

            Sequence of sequences or mappings, parameters to use with
            query.

        Returns long integer rows affected, if any.
         
        This method improves performance on multiple, 
        non-dependent queries.

        Currently not supported.
        """
        del self.messages[:]
        db = self._get_db()
        try:
            queries = []
            for args in sequence_of_args:
                r = self._query(query % args, False)
                queries.append(r)
        except:
            exc, value, tb = sys.exc_info()
            del tb
            self.messages.append((exc, value))
            self.errorhandler(self, exc, value)
    
    def _command_output_handler(self, output):
        self._result = output

    def _command_error_handler(self, error):
        self.messages.append((ProgrammingError, error))
        self.errorhandler(self, ProgrammingError, error)

    def _do_query(self, q, wait=True):
        db = self._get_db()
        self._last_executed = q
        command = []
        if db.user:
            q = 'SET mapred.fairscheduler.pool=%s; %s' % (db.user, q) 
        q = q.replace('"', '\"')
        if db.write_access:
            command = ['sudo', '-uhdfs', 'hive', '-e', '"%s"' % q]
        else:
            command = ['hive', '-e', '"%s"' % q]
        query = Query(command, output=self._command_output_handler, error=self._command_error_handler)
        query.start()
        if wait:
            query.wait()
        return self.rowcount

    def _query(self, q, wait=True):
        return self._do_query(q, wait)

    def _fetch_row(self, size=1):
        raw = self._result.readline()
        if not raw:
            return None
        return tuple(raw.replace('\n', '').split('\t'))

    def __iter__(self):
        return iter(self.fetchone, None)

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

"""
CursorMixIn options for result handling
"""

class CursorStreamResultMixIn(object):

    """This is a MixIn class that streams all rows from HIVE
    using STDIN. Random access is not possible, but for 
    performance reasons this is preferred for large datasets.
    """
 
    def _fetchone(self):
        """Fetch a single row from the cursor.
        None indicates that no more rows are available. """
        self._check_executed()
        row = self._fetch_row(1)
        return row

    def _fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be
        smaller than size.  If size is not defined, cursor.arraysize
        is used."""
        self._check_executed()
        result = []
        i = 0
        if not size:
            size = self.arraysize
        while i < size:
            i += 1
            row = self._fetch_row(1)
            result.append(row)
        return result

    def _fetchall(self): 
        """Fetches everything.  This is probably a really bad idea
        for large datasets."""
        result = []
        while True:
            row = self._fetch_row(1)
            if not row:
                break
            result.append(row)
        return result

    def _scroll(self, value, mode='relative'):
        """Scroll the cursor in the result set to a new position
        according to mode.

        If mode is 'relative' (default), value is takne as offset
        to the current positionin the result set, if set to 'absolute',
        value states an absolute target position.
        
        Only relative forward scrolls are supported in Streaming.
        """
        self._check_executed()
        if mode == 'relative':
            if value < 0:
                self.messages.append((NotSupportedError, 'backward scrolling not supported'))
                self.errorhandler(NotSupportedError, 'backward scrolling not supported')
                return
        elif mode == 'absolute':
            self.messages.append((NotSupportedError, "absolute scrolling not supported")) 
            self.errorhandler(NotSupportedError, 'absolute scrolling not supported')
            return
        i = 0
        while i < size:
            i += 1
            self._fetch_row(1)


class CursorStoreResultMixIn(object):

    """This is a MixIn class that copies hive output and stores
    it locally until the cursor is closed.  Allows improved
    traversal of dataset, but not recommended for large datasets.

    NOT YET IMPLEMENTED.
    """

    def _fetchone(self):
        pass

    def _fetchmany(self, size=None):
        pass

    def _fetchall(self):
        pass

    def _scroll(self, value, mode='relative'):
        pass

"""
CursorMixIn options to display rows
"""

class CursorTupleRowsMixIn(object):

    """This is a MixIn class that causes all rows to be returned
    as tuples, which is the standard form required by DB API."""

    def fetchone(self):
        return self._fetchone()

    def fetchmany(self, size=None):
        return self._fetchman(size)

    def fetchall(self):
        return self._fetchall()

    def scroll(self, value, mode='relative'):
        return self._scroll(value, mode)
 
class CursorDictRowsMixIn(object):
    """This is a MixIn class that causes all rows to be returned
    as dictionaries.  T his is a non-standard feature."""


"""
Default Cursors for use
"""

class Cursor(CursorTupleRowsMixIn, CursorStreamResultMixIn,
             BaseCursor):

    """This is the standard Cursor class that returns rows as tuples
    and stores the result set in the client."""

class DictCursor(CursorDictRowsMixIn, CursorStreamResultMixIn,
                 BaseCursor):

    """This is a Cusor class that returns rows as dictionaries and
    stores the result set in the client.
    
    It is the user's responsibility to implement 
    DictCursor.description to return dictionaries, otherwise dict
    keys will be indices."""

"""
@TODO:
Need a cursor that allows importation of column names.
Need cursor that allows differentiation of result sets by examination of a column 
"""


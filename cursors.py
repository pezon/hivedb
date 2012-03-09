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
import simplejson as json
import logging

def infer_type(value):
    try:
        float(value)
        try:
            int(value)
            return 'int'
        except ValueError:
            return 'float'
    except ValueError:
        try:
            json.loads(value)
            return 'json'
        except:
            return 'str'

def force_type(type, value):
    if type == 'int':
        if value == 'NULL':
            return 0
        return int(float(value))
    if type == 'float':
        if value == 'NULL':
            return 0.0
        return float(value)
    if type == 'str':
        if value == 'NULL':
            return ''
        return str(value)
    if type == 'json':
        if value == 'NULL':
            return None
        return json.loads(value)

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
        self._descriptions = {}
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self.lastrowid = None
        self.messages = []
        self.errorhandler = connection.errorhandler
        self._result = {}
        self._result_index = 0
        self._info = None
        self.rownumber = None
        self._buffer = {}

    def __del__(self):
        self.close()
        self.errorhandler = None
        self._result = None

    def close(self):
        if not self.connection:
            return
        self.connection = None

    def _check_executed(self):
        if not self._executed:
            self.errorhandler(self, ProgrammingError, "execute() first")

    def nextset(self):
        """Advance to the next result set
        Returns None if there are no more result sets."""
        self._check_executed()
        if self._executed:
            try:
                while self._read_buffer():
                    pass
            except KeyError: # already shifted?  via (TriggerCursor)
                pass
        del self.messages[:]
        self._result_index += 1
        self.description = self._descriptions[self._result_index]
        if not self._result.has_key(self._result_index) \
            and self._buffer[self._result_index] is not None:
            return None
        return True

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
        self._pre_execute()
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
        self._post_execute()

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
        self._pre_execute()
        try:
            queries = []
            for args in sequence_of_args:
                q = self._query(query % args, False)
                queries.append(q)
            for query in queries:
                query.wait()
        except:
            exc, value, tb = sys.exc_info()
            del tb
            self.messages.append((exc, value))
            self.errorhandler(self, exc, value)
        self._post_execute()
    
    def _command_output_handler(self, id, output):
        description = []
        columns = output.readline().replace('\n', '').split('\t')
        _buffer = output.readline()
        buffer = _buffer.replace('\n', '').split('\t')
        index = 0
        for column in columns:
            description.append((column, infer_type(buffer[index])))
            index += 1
        self._descriptions[id] = tuple(description)
        self._result[id] = output
        self._buffer[id] = _buffer

    def _command_error_handler(self, id, error):
        self.messages.append((ProgrammingError, error))
        self.errorhandler(self, ProgrammingError, error)
 
    def _command_info_handler(self, id, info):
        db = self._get_db()
        if db.verbose: 
            try:
                info = 'Stage' + info.split('Stage')[1]
            except:
                pass
            logging.info("Query(%s): %s" % (id, info))

    def _pre_execute(self):
        del self.messages[:]
        self._result_index = 0
        self._result = {}
        self.description = None

    def _post_execute(self):
        self._result_index = 0
        self.description = self._descriptions[0]

    def _do_query(self, q, wait=True):
        db = self._get_db()
        self._executed = q
        if db.verbose:
            logging.info("Query(%s)=%s" % (self._result_index, q))
        command = []
        q = "set hive.cli.print.header=true; %s" % q
        if db.user:
            q = 'SET mapred.fairscheduler.pool=%s; %s' % (db.user, q) 
        q = q.replace('"', '\"')
        if db.write_access:
            command = ['sudo', '-uhdfs', 'hive', '-e', '"%s"' % q]
        else:
            command = ['hive', '-e', '"%s"' % q]
        query = Query(self._result_index, command, 
                      output=self._command_output_handler,
                      error=self._command_error_handler,
                      info=self._command_info_handler)
        self._result_index += 1
        query.start()
        if wait:
            query.wait()
        return query

    def _query(self, q, wait=True):
        return self._do_query(q, wait)

    def _read_buffer(self):
        return self._result[self._result_index].readline()

    def _decorate_row(self, row):
        return row

    def _fetch_row(self, size=1):
        raw = None
        if self._buffer[self._result_index]:
            raw = self._buffer[self._result_index]
            self._buffer[self._result_index] = None
        else:
            try:
                raw = self._read_buffer()
            except KeyError:
                return None
        if not raw:
            return None
        row = raw.replace('\n', '').split('\t')
        index = 0
        for r in row:
            type = self.description[index][1]
            row[index] = force_type(type, row[index])
            index += 1
        return self._decorate_row(row)

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
 
    def fetchone(self):
        """Fetch a single row from the cursor.
        None indicates that no more rows are available. """
        self._check_executed()
        row = self._fetch_row(1)
        return row

    def fetchmany(self, size=None):
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

    def fetchall(self): 
        """Fetches everything.  This is probably a really bad idea
        for large datasets."""
        result = []
        while True:
            row = self._fetch_row(1)
            if not row:
                break
            result.append(row)
        return result

    def scroll(self, value, mode='relative'):
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

    def fetchone(self):
        pass

    def fetchmany(self, size=None):
        pass

    def fetchall(self):
        pass

    def scroll(self, value, mode='relative'):
        pass

"""
CursorMixIn options to display rows
"""

class CursorTupleRowsMixIn(object):

    """This is a MixIn class that causes all rows to be returned
    as tuples, which is the standard form required by DB API."""

    def _decorate_row(self, row):
        return tuple(row)

class CursorDictRowsMixIn(object):
    """This is a MixIn class that causes all rows to be returned
    as dictionaries.  T his is a non-standard feature."""

    def _decorate_row(self, row):
        dictrow = {}
        index = 0
        for r in row:
            key = self.description[index][0]
            dictrow[key] = r
            index += 1
        return dictrow

"""
More Cursor options
"""

class CursorTriggeredSetMixIn(object):
    
    """This is a Cursor class that separates result sets when a given
    trigger column(s) change value."""

    _trigger_init = False
    _trigger_columns = ()
    _trigger_column_values = {}

    def _command_output_handler(self, id, output):
        description = []
        columns = output.readline().replace('\n', '').split('\t')
        _buffer = output.readline()
        buffer = _buffer.replace('\n', '').split('\t')
        index = 0
        for column in columns:
            description.append((column, infer_type(buffer[index])))
            index += 1
        self._descriptions[id] = tuple(description)
        self._result[id] = output
        self._buffer[id] = _buffer
        # new stuff: initialzie the _trigger_columns and _trigger_column_values
        # make sure _trigger_columns are indices
        columns = []
        index = 0
        for column in description:
            if column[0] in self._trigger_columns and type(column[0]) == str:
                columns.append(index)
            index += 1               
        self._trigger_init = True
        self._trigger_columns = tuple(columns) 
        # initialzie _trigger_column_values
        index = 0
        for column in self._trigger_columns:
            self._trigger_column_values[index] = buffer[column]
            index += 1

    def _read_buffer(self):
        _buffer = self._result[self._result_index].readline()
        if self._trigger_columns == ():
            return _buffer
        buffer = _buffer.replace('\n', '').split('\t')
        # detect changes in trigger columns
        trigger = False
        index = 0
        for column in self._trigger_columns:
            if buffer[column] != self._trigger_column_values[index]:
                trigger = True
                break
            index += 1
        if not trigger:
            return _buffer
        # record new trigger column values
        index = 0
        for column in self._trigger_columns:
            self._trigger_column_values[index] = buffer[column]
            index += 1
        self._result[self._result_index + 1] = self._result[self._result_index]
        self._buffer[self._result_index + 1] = _buffer
        del self._result[self._result_index]
        self._descriptions[self._result_index + 1] = self._descriptions[self._result_index]
        del self._descriptions[self._result_index]
        del self._buffer[self._result_index]
        return None

    def settriggers(self, columns):
        self._trigger_columns = columns

        
"""
Default Cursors for use
"""

class Cursor(CursorTupleRowsMixIn, CursorStreamResultMixIn,
             BaseCursor):

    """This is the standard Cursor class that returns rows as tuples
    and streams the result set in the client."""

class TriggeredCursor(CursorTriggeredSetMixIn, CursorTupleRowsMixIn,
                      CursorStreamResultMixIn, BaseCursor):

    """This is a triggered cursor that returns rows as tuples and
    streams the result set.  A trigger can be set to create a new
    result set when a column's value changes."""

class DictCursor(CursorDictRowsMixIn, CursorStreamResultMixIn,
                 BaseCursor):

    """This is a Cursor class that returns rows as dictionaries and
    streams the result set in the client."""
    
class TriggeredDictCursor(CursorTriggeredSetMixIn, CursorDictRowsMixIn,
                          CursorStreamResultMixIn, BaseCursor):

    """This is a triggered cursor that returns rows as dicts and
    streams the result set.  A trigger can be set to create a new
    result set when a column's value changes."""


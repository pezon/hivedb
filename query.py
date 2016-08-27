"""
Hive db query
This module sends commands or queries to hive db.
Cursor objects handle this query object.

@ TODO Im sure theres a better way to refactor this.
"""

import logging
from threading import Thread
from subprocess import PIPE, Popen

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Query(Thread):
    def __init__(self, id, command, info=None, error=None, output=None):
        threading.Thread.__init__(self)
        self.id = id
        self.command = command
        self.info_cb, self.error_cb, self.output_cb = info, error, output
        logger.info('Init query id=%s command=%s', self.id, self.command)
        self.ready = False
        self.result = None

    def run(self):
        logger.info('Run query id=%s command=%s', self.id, self.command)
        process = Popen(self.command, stdout=PIPE, stderr=PIPE)
        while process.poll() == None:
            message = process.stderr.readline().replace('\n', '')
            if message != '' and self.info_cb:
                self.info_cb(self.id, message)
            if 'OK' in message:
                break
            if 'FAILED' in message and self.error_cb:
                self.error_cb(self.id, message)
                break
        self.result = process.stdout
        self.output_cb(self.id, self.result)
        self.ready = True

    def wait(self):
        while not self.ready:
            continue

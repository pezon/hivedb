"""
Hive db query
This module sends commands or queries to hive db.
Cursor objects handle this query object.
"""

import threading 
import subprocess as sp

class Query(threading.Thread):
    def __init__(self, id, command, output=None, error=None, info=None):
        threading.Thread.__init__(self)
        self.id = id
        self.command = command
        self.infoCallback = info
        self.errorCallback = error
        self.outputCallback = output
        self.ready = False

    def run(self):
        process = sp.Popen(self.command, stdout=sp.PIPE, stderr=sp.PIPE)
        while process.poll() == None:
            message = process.stderr.readline().replace('\n', '')
            if message != '' and self.infoCallback:
                self.infoCallback(self.id, message)
            if 'OK' in message:
                break
            if 'FAILED' in message and self.errorCallback:
                self.errorCallback(self.id, message)
                break
        self.result = process.stdout
        self.outputCallback(self.id, process.stdout)
        self.ready = True

    def wait(self):
        while not self.ready:
            pass

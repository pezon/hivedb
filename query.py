"""
Hive db query
This module sends commands or queries to hive db.
Cursor objects handle this query object.
"""

import threading 
import subprocess as sp

class Query(threading.Thread):
    def __init__(self, command, output=None, error=None, info=None):
        threading.Thread.__init__(self)
        self.command = command
        self.kill_received = False
        self.process = None
        self.infoCallback = info
        self.errorCallback = error
        self.outputCallback = output

    def run(self):
        while not self.kill_received:
            self.process = sp.Popen(self.command, stdout=sp.PIPE, stderr=sp.PIPE)
            while self.process.poll() == None:
                message = self.process.stderr.readline().replace('\n', '')
                if message != '' and self.infoCallback:
                    self.infoCallback(message)
                if 'OK' in message:
                    break
                if 'FAILED' in message and self.errorCallback:
                    self.errorCallback(message)
                    break
            self.outputCallback(self.process.stdout)
            break

    def wait(self):
        while not self.ready():
            pass
    
    def kill(self):
        self.kill_received = True

    def ready(self):
        return self.process and (not self.isAlive() or self.process.poll() != None)


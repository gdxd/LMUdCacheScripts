import time
class timing(object):
    " a simple class for timing "
    def __init__(self):
        self.begin=0.
        self.last=0.
        self.now=0.
        self.run = False
        
    def start(self):
        "start the timing"
        self.begin=time.time()
        self.last=self.begin
        self.run = True

    def stop(self):
        "stop the timing"
        self.now=time.time()
        self.last=self.now
        self.run = False

    def getdiff(self):
        "get the time diff since start or last call and update last reading"
        self.now=time.time()
        diff=self.now-self.last
        self.last=self.now
        return diff

    def readdiff(self):
        "get the time diff since start or last call w/o update of last reading"
        self.now=time.time()
        diff=self.now-self.last
        return diff

    def gettot(self):
        "get the time diff since start"
        if self.run :
            self.now=time.time()
        diff=self.now-self.begin
        return diff

#



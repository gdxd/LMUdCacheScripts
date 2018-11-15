#!/usr/bin/python
# -*- coding: iso-8859-15 -*-
import time

from xml.sax.handler import ContentHandler

proto = "xrootd"

DEBUG=0

def checkQName( qname ):
    if qname == proto :
        return True
    else: 
        return False

class QPar(object):
    maxactive=0
    active=0
    queued=0
    def __init__( self, qinfo ):
        try:
            self.maxactive = qinfo["max-active"]
            self.active = qinfo["active"]
            self.queued = qinfo["queued"]
        except:
            pass


        if DEBUG>0: print "QPar:", self.maxactive, self.active, self.queued, qinfo


    pass



class PoolHandler(ContentHandler):
    #A handler to deal with articles in XML
    #    def startElement(self, name, attrs):
    #    print Start element:, name
    pname = ""
    qname = ""
    aname = ""
    inQueue = False
    
    qinfo = {}
    pinfo = {}

    def startElement(self, name, attrs):
        # print "startElement", name
        if name == "pool":
            self.pname = attrs.get("name","")
        elif name == "queue":
            self.qname = attrs.get("name","")
            self.inQueue = checkQName( self.qname )

        elif self.inQueue and name == "metric":
            self.aname  = attrs.get("name","")
            self.inMetric = True
#            print self.pname, self.qname, self.aname


    def characters(self, characters):
        # print "characters", characters

        if self.inQueue and self.inMetric:
            self.qinfo[str(self.aname)] = int(characters)

            if DEBUG>0: print self.pname, self.qname, self.aname, characters
            
            # print self.name, characters

    def endElement(self, name):
        if self.inQueue and name == "queue":
            tag = self.pname +":"+self.qname
            if DEBUG>0: print self.pname, self.qname, self.aname, self.qinfo

            self.pinfo[tag] = QPar( self.qinfo )
            self.inQueue = False

        elif name == "pool":
            self.qinfo = {}
        elif name == "metric":
            self.inMetric = False
            
# 


import sys
from xml.sax import make_parser
# from simplehandler import ArticleHandler
from urllib import urlopen

def doParse( url ):
    ch = PoolHandler()
    saxparser = make_parser()
    saxparser.setContentHandler(ch)

    inp = urlopen( url )
    saxparser.parse(inp)

    return ch.pinfo

#    saxparser.parse("pools.xml")
#saxparser.parse(sys.stdin)



if __name__ == '__main__':

    poolinfo = doParse("http://lcg-lrz-dcache0.grid.lrz.de:59998/info/pools")

    for ptag in sorted(poolinfo.keys()):
        qp =  poolinfo[ptag]
        print ptag, qp.maxactive, qp.active, qp.queued



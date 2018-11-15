#!/usr/bin/env python

#GD 14/06/11: fixed fail_ids() 

import sys, time, os, re
from dCacheAdmin import Admin, parseOpts, parse_DBParam

#    "replicas exist on dCache system for logical file.  All future attempts " \
#    "will also fail without manual operator intervention."

def lookup_stuck_movers( a, pool, tmin = 1000 ):

    r = re.compile('RUNNING')
    results = a.execute( pool, 'mover ls' )
    stuck_ids = []
# new format , to be implemented
# 100678792 : RUNNING : 0000B822F235551A4A7D8320789122487ABF IoMode=[READ, WRITE, CREATE] h={SU=0;SA=0;S=None} bytes=0 time/sec=135510 LM=135510 si={atlas:DataDisk}

    for line in results.split('\n'):
        if r.search( line ):
            try:
                fields = line.split(':')[2].split()
                bytes=int(fields[3].split('=')[1])
                time=int(fields[4].split('=')[1])

                if bytes <= 0 and time>tmin:
                    stuck_ids.append( line )
            except:
                print 'Parsing trouble: ', line
                pass
    return stuck_ids

def kill_ids( a, pool, ids ):
    for sid in ids:
	id = sid.split(':')[0]
        a.execute( pool, 'mover kill ', [id] )


def print_ids( ids ):
    print "Movers to be killed:"
    for id in ids:
        print " * %s" % id

def printHelp():
    help = """

    dcache_kill_hanging_movers.py pool [-k] [-config <filename>:<section>]

    The dcache_kill_hanging_movers will check stuck movers and eventually kill them

    Arguments:
      -config <filename>:<section>  Config file listing parameters needed
                                    to connect to dCache.

      -k                            actually kill transfers (default print only)

    """
    print help

if __name__ == '__main__':
  kwOpts, passedOpts, givenOpts = parseOpts( sys.argv[1:] )

  #print 'sys.argv[1:]', sys.argv[1:]
  #print 'kwOpts:', kwOpts
  #print 'passedOpts: ', passedOpts
  #print 'givenOpts:', givenOpts

  if 'h' in passedOpts or len(givenOpts)<1:
      printHelp()
      sys.exit(-1)

  pool = givenOpts[0]
  tmin = 1000

  if 'config' in kwOpts:
      config = kwOpts['config']
      try:
        config, section = config.split(':',1)
      except:
        section = None
  else:
      config = None
      section = None

  if 'tmin' in kwOpts:
     tmin = int(kwOpts['tmin'])

  info = parse_DBParam( config, section )

  try:
      a = Admin( info )
  except Exception, e:
      print "The following error occurred while trying to connect to the " \
            "admin interface:"
      print e
      sys.exit(3)

  stuck_ids = lookup_stuck_movers( a, pool, tmin )
 
  print_ids( stuck_ids )
  if 'k' in passedOpts:
      kill_ids( a, pool, stuck_ids )

  # decent ending
  a.logoff()


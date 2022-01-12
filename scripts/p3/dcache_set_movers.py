#!/usr/bin/env python


import sys, time, os, re
from dCacheAdmin import Admin, parseOpts, parse_DBParam



def check_pool_movers( pool, nmover, queue ):

    # sanity check
    minmover=2
    maxmover=300

    # LRZ specific 
    pref='lcg-lrz-dc'
    if pool.index(pref) != 0 :
        print('check_pool_movers invalid pool name ', pool)
        return False

    if nmover<minmover or nmover>maxmover:
        print('check_pool_movers nmover outside range: ', nmover, minmover, maxmover)
        return False
    
    return True


def set_movers( a, pool, nmover, queue, test=False ):


    if check_pool_movers( pool, nmover, queue ):
        command = 'mover set max active %d -queue=%s' % ( nmover, queue )
        
        if test :
            print("set_movers test-mode: ", pool, command)
        else:
            print("set_movers execute: ", pool, command)
            results = a.execute( pool, command )

    else:
        print("set_movers: outside range, no action for ", pool, nmover, queue)


def adjust_pools( plist, test=False ):

    a = setup_admin()

    for ptag in list(plist.keys()):
        pool = ptag.split(':')[0]
        queue = ptag.split(':')[1]
        set_movers( a, pool, plist[ptag], queue, test )




def printHelp():
    help = """

    dcache_set_movers.py [-queue=qname] -nmover=N -pool=pname[,p2,...] [-config <filename>:<section>]

    The dcache_set_movers.py changes max mover settings

    """
    print(help)

def setup_admin( opts={} ):
  if 'config' in opts:
      config = opts['config']
      try:
        config, section = config.split(':',1)
      except:
        section = None
  else:
      config = None
      section = None

  info = parse_DBParam( config, section )

  try:
      a = Admin( info )
  except Exception as e:
      print("The following error occurred while trying to connect to the " \
            "admin interface:")
      print(e)
      sys.exit(3)

  return a



if __name__ == '__main__':
  kwOpts, passedOpts, givenOpts = parseOpts( sys.argv[1:] )



  if 'h' in passedOpts:
      printHelp()
      sys.exit(-1)


  try:
      pools = kwOpts['pool'].split(',')
      nmover= int(kwOpts['nmover'])
  except:
      printHelp()
      sys.exit(-1)
      
  try:
      qname = kwOpts['queue']
  except:
      qname = 'xrootd'


  a = setup_admin( kwOpts )


  for pool  in pools:
      set_movers( a, pool, nmover, qname )



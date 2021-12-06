#!/usr/bin/env python
#
# GD 18-Jan-2011 
# added function to get list of transfer volume per hour
# 



import sys, os, glob, datetime, time, gzip, pickle
import simpleTiming
from optparse import OptionParser

from dCacheTransferParseUtil import *
from io import StringIO

def setupParser():

  parser = OptionParser()
  parser.add_option('-p','--transfer-protocol',help="Transfer protocol: gftp or dcap.", dest="protocol", default='ALL')
  parser.add_option('-m','--month', help="Month: 2010-06. If this option is specified, then it has the highest priority and the --start-date and --end-date are not taken into account",dest="month",default="empty")
  parser.add_option('-H','--hour', help="Hour 09. If this option is specified, only requests for this hour are processed",dest="hour",default="empty")
  parser.add_option('-s','--start-date',help="The starting date: 2010-06-01", dest="dateStart", default=yesterday() )
                    #datetime.date.today().isoformat())
  parser.add_option('-e','--end-date', help="The ending date", dest="dateEnd", default=yesterday())
  parser.add_option('-d','--debuglevel',help="Debug level", dest="debug",default=0)
  parser.add_option('-i','--pickle-in-dir',help="Dir to read decoded transfers from pickle file", dest="pickleInDir",default='./')
  parser.add_option('-S','--summary',help="Summary type: byprotocol, bycode, bypool, byerrmess", dest="summary",default='byprotocol,bypool,byerrmess')

  return parser

     

if __name__ == "__main__" :

  BDIR = '/var/lib/dcache/billing/'

  parser = setupParser()

  try:
    #option parsing
    (options, args) = parser.parse_args()
    protocol=options.protocol.upper()
    if protocol == 'ALL' :
      protocol=None


    debug=int(options.debug)


    summaries = options.summary.split(',')

    print(summaries)
    
    if options.month!='empty':
      monthEntry=options.month.split('-')
      dateStart=datetime.date(int(monthEntry[0]), int(monthEntry[1]), 1)
      one_day = datetime.timedelta(days=1)
      if int(monthEntry[1]) < 12 :  #GD
        dateEnd=datetime.date(int(monthEntry[0]), int(monthEntry[1])+1, 1)-one_day
      else:
        dateEnd=datetime.date(int(monthEntry[0]), int(monthEntry[1]), 31)
    else:
    
    
#    dateStart=options.dateStart
#    dateEnd=options.dateEnd
#    dateStart=datetime.datetime.strptime(options.dateStart,'%Y-%m-%d')
#    dateEnd  =datetime.datetime.strptime(options.dateEnd,'%Y-%m-%d')
#GD old python needs hickup
      dateStart=datetime.date.fromtimestamp(time.mktime(time.strptime(options.dateStart,'%Y-%m-%d')))
      dateEnd  =datetime.date.fromtimestamp(time.mktime(time.strptime(options.dateEnd,'%Y-%m-%d')))
      monthEntry=[str(dateStart.year),str(dateStart.month)]
  
  except ImportError:
    sys.exit("Error: wrong options")


  mytimer = simpleTiming.timing()
  mytimer.start()



  nfprocess = 0
  flist = []

  daydicts={}

#  for day in range( int(dateStart.day),int(dateEnd.day)+1):
  for day in range( dateStart.toordinal(),dateEnd.toordinal()+1):
     dateDayToProcess=datetime.date.fromordinal(day)
#     dateDayToProcess=datetime.date(int(monthEntry[0]), int(monthEntry[1]), day)

     if options.pickleInDir=='BILLINGDIR':
       indir=BDIR + dateDayToProcess.strftime('%Y/%m')
     else:
       indir = options.pickleInDir
       

     filename=indir+'/dctr-error-'+dateDayToProcess.strftime('%Y-%m-%d')+'.cpickle.gz'



     if os.access( filename, os.R_OK ):
       flist.append(filename)
     else:
       print('trouble accessing ', filename)
       sys.exit("Give up ...")

  # end day loop

  # overall summary


  for type in ['transfer','request']:
    for tag in summaries:

      results, resultsdict = getErrorSumbyTag( tag, getReqTrObjs( flist, protocol) , type  )

      print('++++ ' + type + ' ++++ ' +tag+ ' +++++++ summary for '+dateStart.strftime('%Y-%m-%d')+' to '+dateEnd.strftime('%Y-%m-%d') +' [sorted by frequency] +++++++++++++++')
      PrintErrorResult(results, options.protocol, tag)
#      print(format('Time for processing: ', 65) + format(mytimer.getdiff(), -12))
      

# end __main__

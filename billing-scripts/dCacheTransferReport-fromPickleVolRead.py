#!/usr/bin/env python
#
# GD 18-Jan-2011 
# added function to get list of transfer volume per hour
# 



import sys, os, glob, datetime, time, gzip, cPickle, math
import simpleTiming
from optparse import OptionParser

from dCacheTransferParseUtil import *
from StringIO import StringIO

from ROOT import *

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
  parser.add_option('-S','--summary',help="Summary type", dest="summary",default='bydomain,bypool,byprotocol,byspacetoken')
  parser.add_option('-t','--tag',help="File tag", dest="tag",default=None)

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

    print summaries
    
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
       indir=BDIR+dateDayToProcess.strftime('%Y/%m')
     else:
       indir = options.pickleInDir
       

     filename=indir+'/dctr-'+dateDayToProcess.strftime('%Y-%m-%d')+'.cpickle.gz'



     if os.access( filename, os.R_OK ):
       flist.append(filename)
     else:
       print 'trouble accessing ', filename
       sys.exit("Give up ...")



  hdict = {}

  hdict["fsize"]  = TH1F("fsize","File-size ",200, 0, 10000)
  hdict["fsize-log"] = TH1F("fsizel","log File-size ",200, -6, 10)
  hdict["tsize-log"] = TH1F("tsizel","log Transfer-size ",200, -6, 10)
  hdict["fsrs1"]  = TH2F("fsrs1","File-size vs Read-size ",200, 0, 5000, 200, 0, 5000)
  hdict["fsrs-log"]  = TH2F("fsrs-log","log File-size vs log Read-size ",200, -1, 4, 200, -1, 4)
  hdict["fsize-rat1"] = TH1F("fsize-rat1","Read/File-size ratio ",400, 0, 10)
  hdict["fsize-rat2"] = TH1F("fsize-rat2","Read/File-size ratio tr>3",400, 0, 10)
  hdict["fsrs2"]  = TH2F("fsrs2","File-size vs Read/File-size ratio ",200, 0, 5000, 200, 0, 3)


  nf = 0

  for tro in getTrObjs( flist, protocol ) :
#    print "%10.1f  %10.1f  %s" % ( tro.datavolume, tro.fsize, tro.fname )

    if tro.fsize<10: continue # at least 10 MB files


    try:
      fnparts = tro.fname.strip('/').split('/')
      fn = fnparts[5]
      if fn.find('atlasdatadisk')<0 and  fn.find('atlaslocalgroupdisk')<0 and  fn.find('atlasgroupdisk')<0 and  fn.find('atlasproddisk')<0   : # only these spacetokens
        continue
      dstags = fnparts[-2].split('.')
      if dstags[0] == 'user':
        key = dstags[0]+'***'
      else:
        key = dstags[0]+'_'+dstags[4]

      if options.tag != None and key.find(options.tag)<0: 
        continue


      hdict["fsize"].Fill(  tro.fsize )
      if tro.fsize > 1e-3 :
        lfs = math.log10(tro.fsize )
      else:
        lfs = -5

      if tro.datavolume > 1e-3 :
        lts = math.log10(tro.datavolume )
      else:
        lts = -5

      rfs = tro.datavolume/tro.fsize
      hdict["fsize-log"].Fill(  lfs )
      hdict["tsize-log"].Fill( lts ) 
      hdict["fsize-rat1"].Fill(  rfs )
      if ( lts > 0.5 ) :
        hdict["fsize-rat2"].Fill(  rfs )
      hdict["fsrs1"].Fill(  tro.fsize,  tro.datavolume )
      hdict["fsrs-log"].Fill(  lfs,  lts )
      hdict["fsrs2"].Fill(  tro.fsize,  rfs )
    
      nf += 1
    except:
      print  'Trouble processing '+tro.fname

  print 'Done filling histos, %d entries for %s' % ( nf, options.tag )
  # for h in hdict.keys():
  #   print 'Histo '+ h
  #   hdict[h].Draw()
  #   aus = raw_input()
    
  # output root file
  if options.tag == None:
    outfile = './out.root'
  else:
    outfile = './out_'+options.tag+'.root'


  

  print "Storing in ", outfile
  tf1 = TFile(outfile,"recreate")
    # store histos in Root file
  for h in hdict.keys():
    hdict[h].Write()

# end __main__

#!/usr/bin/env python
#
# GD 18-Jan-2011 
# added function to get list of transfer volume per hour
# 

# xroot request/transfer:
# 10.03 12:13:26 [door:Xrootd-lcg-lrz-dc15@xrootd-lcg-lrz-dc15Domain:request] ["unknown":-1:-1:lxe26.cos.lrz-muenchen.de] [00008B11C151C45349B582D84E8EE6B16024,0] [pnfs/lrz-muenchen.de/data/atlas/dq2/atlaslocalgroupdisk/mc10_7TeV/NTUP_SMWZ/e574_s933_s946_r2302_r2300_p605/mc10_7TeV.106051.PythiaZmumu_1Lepton.merge.NTUP_SMWZ.e574_s933_s946_r2302_r2300_p605_tid441300_00/NTUP_SMWZ.441300._000026.root.2] <unknown> 1317636806718 0 {0:""}
# 10.03 12:13:35 [pool:lcg-lrz-dc50_6@lcg-lrz-dc50_6Domain:transfer] [000037A050A2295643708A5C299CE57A5B38,527715714] [Unknown] atlas:LocalGroupDisk@osm 165988346 82292 false {Xrootd-2.7,lxe26.cos.lrz-muenchen.de:35011} [door:Xrootd-lcg-lrz-dc15@xrootd-lcg-lrz-dc15Domain:12987] {0:""}


# dcap-gsi request/transfer
# 10.03 00:00:06 [door:DCap-gsi-lcg-lrz-dcache-/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker-8631@gsidcap-lcg-lrz-dcacheDomain:request] ["/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker":10761:1307:unknown] [0000959ECEC68A824DCA963F587E8CBEDFC7,0] [//pnfs/lrz-muenchen.de/data/atlas/dq2/atlasscratchdisk/user.elmsheus/user.elmsheus.hc10006805.ANALY_LRZ11.69.6617.ANALY_LRZ11.lib/user.elmsheus.hc10006805.ANALY_LRZ11.69.6617.ANALY_LRZ11.lib.tgz] <unknown> 75 0 {0:""}
# 10.03 00:00:06 [pool:lcg-lrz-dc21_1@lcg-lrz-dc21Domain:transfer] [0000959ECEC68A824DCA963F587E8CBEDFC7,1771] [Unknown] atlas:ScratchDisk@osm 1771 55 false {DCap-3.0,lxe15.cos.lrz-muenchen.de:40181} [door:DCap-gsi-lcg-lrz-dcache-/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker-8631@gsidcap-lcg-lrz-dcacheDomain:1317592806081-24538] {0:""}
# dcap request/transfer
#10.03 00:21:26 [door:DCap-lcg-lrz-dc43-Unknown-10344@dcap-lcg-lrz-dc43Domain:request] ["Unknown":-1:-1:unknown] [000038679099193A4227B614C1979EEA1D09,0] [//pnfs/lrz-muenchen.de/data/atlas/dq2/atlashotdisk/cond11_data/000002/gen/cond11_data.000002.gen.COND/cond11_data.000002.gen.COND._0001.pool.root] <unknown> 11619 0 {0:""}
#10.03 00:03:07 [pool:lcg-lrz-dc24_2@lcg-lrz-dc24Domain:transfer] [000008B446AE297B4B029507C4E9A318970D,663904356] [Unknown] atlas:LocalGroupDisk@osm 663904356 2458 false {DCap-3.0,129.187.131.51:33131} [<undefined>] {0:""}

# GFTP request/transfer
# 10.03 00:00:09 [door:GFTP-lcg-lrz-dc12-Unknown-91599@gridftp-lcg-lrz-dc12Domain:request] ["/DC=org/DC=doegrids/OU=People/CN=Louis Bianchini 910142":10761:1307:brndt3int0.hep.brandeis.edu] [0000712334C996024AE4857F7F69774414B8,0] [/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasscratchdisk/user.lbianchi/user.lbianchi.periodK.NoGRL.physics_Egamma.NTUP_SMWZ.p605.111001.v2.111002144049_sub031165743/user.lbianchi.002696._1325248635.log.tgz] <unknown> 1317592809019 0 {0:""}
# 10.03 00:00:08 [pool:lcg-lrz-dc21_1@lcg-lrz-dc21Domain:transfer] [0000E482AFCAD01540399A85E67BEEF899E2,890264] [Unknown] atlas:ScratchDisk@osm 890264 1013 false {GFtp-1.0 lcg-lrz-dc40.grid.lrz-muenchen.de 38389} [door:GFTP-lcg-lrz-dc40-Unknown-94637@gridftp-lcg-lrz-dc40Domain:1317592806985-94422] {0:""}



import os, glob, datetime, time, gzip
import simpleTiming
from optparse import OptionParser


class TransferObj( object ):
  " class to contain billing transfer info"
  def __init__( self, line, entry ):
    self.entry = entry
    self.line = line
    self.Write = False 
    self.host = ''
    transfer = line.split(' [')


    try:
  #06.01 01:08:54
      self.Time=transfer[0].split()[1] #GD only time
  #pool:sn03@sn03Domain:transfer]
      #transferPool=transfer[1].split(':')
  #0000E82ABA02230A4421BE206816D83DAF42,468373]
      PNFS=transfer[2].strip(']').split(',')
      self.PNFSid=PNFS[0]
        #transferFilesize=int(transferPNFS[1])/1024/1024

  #Unknown] atlas:SCRATCHDISK@osm 468373 10 true {GFtp-1.0 grid-se.physik.uni-wuppertal.de 40340}
      transferDetails=transfer[3].strip(']').split(' ')

      self.Datavolume=round(float(transferDetails[2])/1024/1024,3)
      self.Duration=float(transferDetails[3])
      if self.Duration>0.  and self.Datavolume>0. :
        self.Rate = self.Datavolume / self.Duration 
      else:
        self.Rate = 0

      Direction=transferDetails[4]
      if Direction.find('true')>=0:
        self.Write = True 
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:1275347334131-58042] {0:""}
      self.Door=transfer[4].split('@')[0]
      

      # works only for dcap
      try:
        self.host = transfer[3].strip(':').strip('{').strip('}').split(',')[1].split(':')[0]
        if self.host.find('.')>=0:
          if self.host.upper()!=self.host.lower():
            self.Domain=self.host.split('.',1)[1]
          else:
            self.Domain=self.host.split('.')[0]+'.'+self.host.split('.')[1]+'.*'
        else:
          self.Domain=self.host

      except:
        pass


      self.hash = (self.Door + self.PNFSid).__hash__() 

      self.ok = True

    except Exception, x:
      print  "Transfer parsing troubles: ", x.__class__.__name__ , ' : ', x, ' Line: ', line
      print self.__dict__
      print 'transferDetails=',transferDetails
      print 'transfer[3:5]=', transfer[3:5]
      self.ok = False



  def __str__( self ):
    return self.line

class RequestObj( object):
  " class to contain billing request info"
  def __init__( self, line, entry ):
    self.entry = entry
    self.line = line
    self.host = ''

    request= line.split(' [')

    try:
      self.Door=request[1].split('@')[0]
      self.PNFSid=request[3].strip(']').split(',')[0]
      self.Time=request[0]
      
  #"/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker":10761:1307:wn092.pleiades.uni-wuppertal.de]
  #"/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gridmon/CN=137254/CN=Robot: Grid Monitoring-Framework/E=grid.monitoring-framework@cern.ch":21000:2031:sam011.cern.ch
      requestCredentials=request[2].strip(']').split(':')
      if len(requestCredentials) >=4 and protocol=='GFTP':
        self.host=requestCredentials[-1]

        if self.host.find('.')>=0:
          if self.host.upper()!=self.host.lower():
            self.Domain=self.host.split('.',1)[1]
          else:
            self.Domain=self.host.split('.')[0]+'.'+self.host.split('.')[1]+'.*'
        else:
          self.Domain=self.host


  #0000E82ABA02230A4421BE206816D83DAF42,0]
  #/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user10.AgnieszkaLeyko/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306_sub07654925/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306._1076473737.log.tgz] <unknown> 1275347334131 0 {0:""}
      requestDetails=request[4].strip(']').split(' ')
      self.File=requestDetails[0]

      #
      self.hash = (self.Door + self.PNFSid).__hash__() 

      self.ok = True
    except Exception, x:
      print  "Request parsing troubles: ", x.__class__.__name__ , ' : ', x,  ' Line: ',line
      self.ok = False

  def __str__( self ):
    return self.line

def setupParser():
  BDIR = '/var/lib/dcache/billing/'
  parser = OptionParser()
  parser.add_option('-p','--transfer-protocol',help="Transfer protocol: gftp or dcap.", dest="protocol", default='GFTP')
  parser.add_option('-m','--month', help="Month: 2010-06. If this option is specified, then it has the highest priority and the --start-date and --end-date are not taken into account",dest="month",default="empty")
  parser.add_option('-H','--hour', help="Hour 09. If this option is specified, only requests for this hour are processed",dest="hour",default="empty")
  parser.add_option('-s','--start-date',help="The starting date: 2010-06-01", dest="dateStart", default=datetime.date.today().isoformat())
  parser.add_option('-e','--end-date', help="The ending date", dest="dateEnd", default=datetime.date.today().isoformat())
  parser.add_option('-d','--debuglevel',help="Debug level", dest="debug",default=0)
  parser.add_option('-f','--directory',help="Path to billing directories, default is " + BDIR, dest="directory",default=BDIR')

  return parser

def format(stringToExtend, length):
  while len(str(stringToExtend)) < abs(int(length)):
    if int(length) < 0:
      stringToExtend= ' ' + str(stringToExtend)
    else:
      stringToExtend= str(stringToExtend) + ' '
  return str(stringToExtend)

def set(ordinaryList):
  uniqueList=[]
  for entry in ordinaryList:
    if not entry in uniqueList:
      uniqueList.append(entry)
  return uniqueList

  

def SumDomains(domainListDetail, datavolumeListDetail, datavolumeWritingListDetail, datavolumeReadingListDetail, transferDurationListDetail):
  domainResults=[]
  domainResultDetail=[]
#  for domain in domainListDetail:
#    if not domain in domainList:
#      domainList.append(domain)
  domainList=set(domainListDetail)
  #print(domainList)
  for domain in domainList:
    i=0
    domainDatavolume=0
    domainDatavolumeWriting=0
    domainDatavolumeReading=0
    domainDuration=0
    domainTransferNumber=0
    for domainDetail in domainListDetail:
      if domain == domainDetail:
        domainDatavolume+= datavolumeListDetail[i]
        domainDatavolumeWriting+= datavolumeWritingListDetail[i]
        domainDatavolumeReading+= datavolumeReadingListDetail[i]
        domainDuration+=int(transferDurationListDetail[i])
        domainTransferNumber+=1
      i+=1
    if domainDuration > 0:
      domainTransferRate=round(domainDatavolume/domainDuration*1024,3)
    else: 
      domainTransferRate=0.0

    domainLine=(domain, domainDatavolume, domainTransferNumber, domainDuration, domainTransferRate, domainDatavolumeWriting, domainDatavolumeReading)
    domainResults.append(domainLine)
  return domainResults

def PrintDomainResult(domainResults, protocol='GFTP', sortKey=1):
  print(format('Domain', '35') + str('DV [MByte]').center(15) + str('Transfers').center(15) + str('Duration [sec]').center(15) +' | '+ str('write [MByte]').center(15) + str('read [MByte]').center(15))
  Datavolume=0
  TransferNumber=0
  Duration=0
  DVwrite=0
  DVread=0
  for domainResultLine in sorted(domainResults, key=lambda entry: entry[sortKey], reverse=True):
    print(format(domainResultLine[0], 35) + format("%12.1f" % domainResultLine[1], -15) + format(domainResultLine[2], -15) + format(domainResultLine[3], -15) +' | '+ format("%12.1f" % domainResultLine[5], -15) + format("%12.1f" % domainResultLine[6], -15))
    Datavolume+=domainResultLine[1]
    TransferNumber+=domainResultLine[2]
    Duration+=domainResultLine[3]
    DVwrite+=domainResultLine[5]
    DVread+=domainResultLine[6]
  print('---------------------------------------------------------------------------------+-------------------------------')
  print(format('Sum '+ protocol +':', -35) + format("%12.1f" % Datavolume, -15) + format(TransferNumber, -15) + format(Duration, -15) +' |'+ format("%12.1f" % DVwrite, -15) + format("%12.1f" % DVread, -15))
  print('=================================================================================+===============================')
  return True


def PrintSumPerHour( datavolumeListDetail, transferTimeListDetail ):
  arr=24*[0.]

  for i in xrange(len(datavolumeListDetail)):
    # '01:08:54'
    try:
      if ( transferDurationListDetail[i] > 0 ): # no p2p transfers
        hour = int(transferTimeListDetail[i].split(':')[0])
        arr[hour] += datavolumeListDetail[i]
    except:
      print 'PrintSumPerHour trouble', i, transferTimeListDetail[i]

  print '### Transfer per hour :'
  for i in range(len(arr)):
    print "%5d   %10.0f" % (i, arr[i])

  print ' '
    
  

if __name__ == "__main__" :
  parser = setupParser()

  try:
    #option parsing
    (options, args) = parser.parse_args()
    protocol=options.protocol.upper()
    if protocol=='DCAP':
      protocol='DCap'
    if protocol=='XROOTD':
      protocol='Xrootd'

    debug=int(options.debug)

    
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
      directory=options.directory+dateStart.strftime('%Y/%m')
  
#  protocol = options.protocol
  except ImportError:
    sys.exit("Error: wrong options")



#month='2010-06'
#if WinTestmode:
#  directory='C:/Users/schultes/Desktop/billing'
#else:
#  directory='/opt/d-cache/billing/'+dateStart.strftime('%Y/%m')

  domainMonthResults=[]
  hostMonthlyListDetail=[]
  domainMonthlyListDetail=[]
  datavolumeMonthlyListDetail=[]
  datavolumeWritingMonthlyListDetail=[]
  datavolumeReadingMonthlyListDetail=[]
  transferDurationMonthlyListDetail=[]


  print('local folder which will be processed:' +directory)

  failedRequestList=[]
  failedTransferList=[]

  mytimer = simpleTiming.timing()
  mytimer.start()

#GD for day in range(1,int(dateEnd.day)+1):
  for day in range( int(dateStart.day),int(dateEnd.day)+1):
    print('')
    print('')
    transferList=[]
    requestList=[]
    p2pList=[]

    dateDayToProcess=datetime.date(int(monthEntry[0]), int(monthEntry[1]), day)
    filename=directory+'/billing-'+dateDayToProcess.strftime('%Y.%m.%d')

    filenamegz = filename + '.gz'     # try compressed
    print('file which is being processed: '+ filename +'[.gz]')
    
#f=open('/opt/d-cache/billing/2010/06/billing-2010.06.30', 'r')

    if os.path.exists(filename):
      f=open(filename)
      linesBilling=f.readlines() 
      f.close()
    elif os.path.exists(filenamegz):
      f=gzip.open(filenamegz)
      linesBilling=f.readlines()
      f.close()
    else:
      print ("ERROR: File "+filename+" does not exist")
      continue

    print(format('Time for reading: ', 65) + format(mytimer.getdiff(), -12))


    hostListDetail=[]
    domainListDetail=[]
    datavolumeListDetail=[]
    datavolumeWritingListDetail=[]
    datavolumeReadingListDetail=[]
    transferDurationListDetail=[]
    transferTimeListDetail=[]




    nentries=0
    numberOfBillingEntries= len(linesBilling)
    print(format('Number of all entries: ', 65) + format(numberOfBillingEntries, -12))
    while len(linesBilling)> 0:#len(linesBilling):
      # deleting entry which is not the requested protoco
      indexBilling = len(linesBilling)-1
      if indexBilling%100000 == 0:
        print('entries found for '+ protocol +': '+ str(len(requestList)) +', entries still to be processed: '+ str(len(linesBilling)) +', sum of entries: '+ str(numberOfBillingEntries))      
      lastLine = linesBilling[-1]
      hourline = (lastLine.split()[1]).split(':')[0]
#01.31 00:00:16 [pool:sn14@sn14Domain:transfer] [000039EF0ACDD2BC42D0A74CC8C9B8AC010E,881569198] [Unknown] atlas:LOCALGROUPDISK@osm 881569198 9584 false {DCap-3.0,132.195.124.212:24045} [<undefined>] {0:""}
      if (protocol=='DCap' or protocol == 'Xrootd') and lastLine.find('door:')<0 and lastLine.find('pool:')>0 and lastLine.find('transfer')>0:
	p2pList.append(lastLine)
	host=lastLine.split()[9].split(',')[1].split(':')[0]
	hostListDetail.append(host)
	hostMonthlyListDetail.append(host)
	domainListDetail.append(host.split('.')[0]+'.'+host.split('.')[1]+'.*')
	domainMonthlyListDetail.append(host.split('.')[0]+'.'+host.split('.')[1]+'.*')
	volume = round(float(lastLine.split()[6])/(1024.*1024),3)
	datavolumeMonthlyListDetail.append(volume)
	datavolumeListDetail.append(volume)
	datavolumeWritingMonthlyListDetail.append(0)
	datavolumeWritingListDetail.append(0)
	datavolumeReadingMonthlyListDetail.append(volume)
	datavolumeReadingListDetail.append(volume)
	transferDurationListDetail.append(0)
	transferDurationMonthlyListDetail.append(0)
        transferTimeListDetail.append(0)

      if lastLine.find('door:'+protocol)<0:
        if debug >=5:
          print('deleting entry: ' + linesBilling[-1])
    # classifying the entry
      else:
        nentries += 1
        if lastLine.find(':request]')>=0:
# example for a failed request:
# 07.04 01:06:16
# door:GFTP-grid-se-Unknown-276078@gridftp-grid-seDomain:request]
# "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=girolamo/CN=614260/CN=Alessandro Di Girolamo":10761:1307:samnag013.cern.ch]
# 00009E4E45AA4F0349BBA28B3D2A9DD692AB,0]
# /pnfs/physik.uni-wuppertal.de/data/atlas/generated/testfile-put-1278198362-38fa2b40d5e7.txt] <unknown> 1278198376014 0 {451:"Operation failed: FTP Door: got response from '[>PoolManager@dCacheDomain:*@dCacheDomain:SrmSpaceManager@srm-grid-seDomain:*@srm-grid-seDomain:*@dCacheDomain]' with error No write pools configured for <atlas:GENERATED@osm> for linkGroup: atlas_scratchdisk-LinkGroup"}
          requestExitCode = int(lastLine.split(' {')[1].split(':')[0])
          if requestExitCode>0:
            failedRequestList.append(lastLine)
            if debug >= 1: print('INFO: request failed: ' + lastLine)
          else:
#            if options.hour!='empty' and options.hour == hourline:
            if options.hour == 'empty' or options.hour == hourline:

# parse info and store in object
              reqObj = RequestObj( lastLine, indexBilling )
              if reqObj.ok:
                requestList.append(reqObj)
              if debug >=3:
                print('Found request: ' + str(reqObj.ok) + lastLine)

        if lastLine.find(':transfer]')>=0:
# example for a failed request:
# 07.05 07:04:07
# pool:sn14@sn14Domain:transfer]
# 0000D065DD1A98594BDE927F8E7869AD899E,0]
# Unknown] atlas:DATADISK@osm 3429498880 3598854 true {GFtp-2.0 grid-se.physik.uni-wuppertal.de 54099}
# door:GFTP-grid-se-Unknown-281222@gridftp-grid-seDomain:1278302632625-280160] {10001:"No such file or directory: 0000D065DD1A98594BDE927F8E7869AD899E"}
          requestExitCode = int(lastLine.split(' {')[2].split(':')[0])
          if requestExitCode>0:
            failedTransferList.append(lastLine)
            if debug >= 1: print('INFO: transfer failed: ' + lastLine)
          else:
            traObj = TransferObj( lastLine, indexBilling )

            if traObj.ok:
              transferList.append(traObj)
            if debug >=3:
              print('Found transfer: ' + str(traObj.ok) + lastLine)

#      del linesBilling[-1]
      linesBilling.pop()
    # end loop billing entries
    print(format('Number of of triggered Pool2pool replications:',65) + format(len(p2pList),-12))
    print(format('Number of entries with door: '+protocol+':', 65) + format(nentries, -12))
    print(format('Number of entries with door: '+protocol+' and :request]', 65) + format(len(requestList), -12))
    print(format('Number of entries with door: '+protocol+' and :transfer]', 65) + format(len(transferList), -12))
    print(format('overall number of entries with door: '+protocol+' and failed request:', 65) + format(len(failedRequestList), -12))
    print(format('overall number of entries with door: '+protocol+' and failed transfer:', 65) + format(len(failedTransferList), -12))

    print(format('Time for parsing: ', 65) + format(mytimer.getdiff(), -12))


    # loop over transfers
    trreqList=[]
    trorphanList=[]
    indexTransfer=-1
    for trobj in transferList:
      indexTransfer+=1
      if indexTransfer%5000 ==0:
        print('transfer entries still to be processed: '+ str(len(transferList)-indexTransfer))

      indexRequest=-1
      noRequestEntry=False

      # loop over requests
      for reqobj in requestList:
        indexRequest+=1
        

        # testing request entries for fiting to transfer entry.
        # if a fitting pair is found, the request entry has not to be tested for the next transfers
        if reqobj.hash == trobj.hash and reqobj.Door == trobj.Door and reqobj.PNFSid == trobj.PNFSid:

          # for dcap request host in transfer info, copy over
          if protocol == 'DCap' or protocol == 'Xrootd': 
            reqobj.host = trobj.host
            reqobj.Domain = trobj.Domain
          # put it in combined list
          trreqList.append( ( trobj, reqobj ))
          del requestList[indexRequest] # remove from list
          break

      else:
        noRequestEntry=True
        requestEntryInFailed=False
        for failedRequest in failedRequestList:
          if failedRequest.find(trobj.Door)>=0:
            requestEntryInFailed=True
            break
        else:
          trorphanList.append(trobj)
          if options.hour == 'empty':
            print('ERROR: no request entry found for', trobj.line)

    # end transfer loop


    print(format('Time for matching: ', 65) + format(mytimer.getdiff(), -12))


    print(format('Overall number of matching transfers/requests: ', 65) + format(len(trreqList), -12 ))
    
    for trobj, reqobj in trreqList:
      hostListDetail.append(reqobj.host)
      hostMonthlyListDetail.append(reqobj.host)
      domainListDetail.append(reqobj.Domain)
      domainMonthlyListDetail.append(reqobj.Domain)

      datavolumeListDetail.append(trobj.Datavolume)
      datavolumeMonthlyListDetail.append(trobj.Datavolume)
      if trobj.Write:
        datavolumeWritingListDetail.append(trobj.Datavolume)
        datavolumeWritingMonthlyListDetail.append(trobj.Datavolume)
        datavolumeReadingListDetail.append(0)
        datavolumeReadingMonthlyListDetail.append(0)
      else:
        datavolumeReadingListDetail.append(trobj.Datavolume)
        datavolumeReadingMonthlyListDetail.append(trobj.Datavolume)
        datavolumeWritingListDetail.append(0)
        datavolumeWritingMonthlyListDetail.append(0)

      transferDurationListDetail.append(trobj.Duration)
      transferDurationMonthlyListDetail.append(trobj.Duration)
      transferTimeListDetail.append(trobj.Time)

    #

    if debug>=2:
      print(hostListDetail)
      print(domainListDetail)
      print(datavolumeListDetail)
      print(datavolumeWritingListDetail)
      print(datavolumeReadingListDetail)
      print(transferDurationListDetail)

    hostList=[]
    domainList=[]

  #print('++++++++++++++++++++++++++++++ hosts ++++++++++++++++++++++++++++++')

    doHostList = False

    if doHostList:
      hostList=set(hostListDetail)
  #print(hostList)
      for host in hostList:
        i=0
        hostDatavolume=0
        hostDatavolumeWriting=0
        hostDatavolumeReading=0
        hostTransferDuration=0
        hostTransferNumber=0
        for hostDetail in hostListDetail:
          if host == hostDetail:
            hostDatavolume+= datavolumeListDetail[i]
            hostDatavolumeWriting+= datavolumeWritingListDetail[i]
            hostDatavolumeReading+= datavolumeReadingListDetail[i]
            hostTransferDuration+=int(transferDurationListDetail[i])
            hostTransferNumber+=1
          i+=1
     
        if hostTransferDuration > 0:
          hostTransferRate=round(hostDatavolume/hostTransferDuration,3)
        else: 
          hostTransferRate=0.0
        print("host: " +host, "; Datavolume:" +str(hostDatavolume) + " MByte", "; Number of Transfers: " +str(hostTransferNumber), "; Transferrate: " +str(hostTransferRate)+ " MByte/sec", " Datavolume writing: ", str(hostDatavolumeWriting) + " MByte", " Datavolume reading: ", str(hostDatavolumeReading) + " MByte")
      # end host detail
#  for entryGFTPtransfer in transferList:
#    print('INFO: A transfer without request '+entryGFTPtransfer)
#  print(str(len(requestList)) + ' '+protocol+' requests not resolved:')
#  for entryGFTPrequest in requestList:
#    print('INFO: '+entryGFTPrequest)
    print('+++++++++++++++ daily '+protocol+' summary for domains for '+str(dateDayToProcess)+' [sorted by Datavolume] +++++++++++++++')
    domainResults = SumDomains(domainListDetail, datavolumeListDetail, datavolumeWritingListDetail, datavolumeReadingListDetail, transferDurationListDetail)
    PrintDomainResult(domainResults, protocol)
#GD
    PrintSumPerHour( datavolumeListDetail, transferTimeListDetail )


  domainResults = SumDomains(domainMonthlyListDetail, datavolumeMonthlyListDetail, datavolumeWritingMonthlyListDetail, datavolumeReadingMonthlyListDetail, transferDurationMonthlyListDetail)

  print(format('Time for adding up: ', 65) + format(mytimer.getdiff(), -12))


  print('')
  print('')
  print('')
  print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
  print(str(len(trorphanList)) + ' '+protocol+' transfers not resolved:')
  if ( debug > 0 ):
    for entryGFTPtransfer in trorphanList:
      print('INFO: A transfer without request ', entryGFTPtransfer.line)
  print(str(len(requestList)) + ' '+protocol+' requests not resolved:')
  if ( debug > 0 ):
    for entryGFTPrequest in requestList:
      print('INFO: A request without transfer', entryGFTPrequest.line)

  print(format('overall number of entries with door:'+protocol+' and failed request:', 65) + format(len(failedRequestList), -12))
  print(format('overall number of entries with door:'+protocol+' and failed transfer:', 65) + format(len(failedTransferList), -12))

#print('+++++++++++++++ monthly '+protocol+' summary for domains for '+dateStart.strftime('%Y-%m')+' [sorted by Duration] +++++++++++++++')
#PrintDomainResult(domainResults, protocol, 3)
  print('*** Summary ***')
  PrintDomainResult(domainResults, protocol)



# end __main__

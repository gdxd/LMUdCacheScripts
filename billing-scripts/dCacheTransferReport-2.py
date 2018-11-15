#!/usr/bin/env python
#
# GD 18-Jan-2011 
# added function to get list of transfer volume per hour
# 

import os, glob, datetime, time
from optparse import OptionParser

WinTestmode=False

BDIR = '/var/lib/dcache/billing/'

parser = OptionParser()
parser.add_option('-p','--transfer-protocol',help="Transfer protocol: gftp or dcap.", dest="protocol", default='GFTP')
parser.add_option('-m','--month', help="Month: 2010-06. If this option is specified, then it has the highest priority and the --start-date and --end-date are not taken into account",dest="month",default="empty")
parser.add_option('-s','--start-date',help="The starting date: 2010-06-01", dest="dateStart", default=datetime.date.today().isoformat())
parser.add_option('-e','--end-date', help="The ending date", dest="dateEnd", default=datetime.date.today().isoformat())
parser.add_option('-d','--debuglevel',help="Debug level", dest="debug",default=0)
parser.add_option('-f','--directory',help="Path to billing directories, default is ' + BDIR", dest="directory",default=BDIR)

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
    print(format(domainResultLine[0], 35) + format(domainResultLine[1], -15) + format(domainResultLine[2], -15) + format(domainResultLine[3], -15) +' | '+ format(domainResultLine[5], -15) + format(domainResultLine[6], -15))
    Datavolume+=domainResultLine[1]
    TransferNumber+=domainResultLine[2]
    Duration+=domainResultLine[3]
    DVwrite+=domainResultLine[5]
    DVread+=domainResultLine[6]
  print('---------------------------------------------------------------------------------+-------------------------------')
  print(format('Sum '+ protocol +':', -35) + format(Datavolume, -15) + format(TransferNumber, -15) + format(Duration, -15) +' |'+ format(DVwrite, -15) + format(DVread, -15))
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

  print '### Transfer per hour :',
  for a in arr:
    print a,

  print ' '
    
  


try:
  (options, args) = parser.parse_args()
  protocol=options.protocol.upper()
  if protocol=='DCAP':
    protocol='DCap'
    debug=options.debug
  if options.month!='empty':
    monthEntry=options.month.split('-')
    dateStart=datetime.date(int(monthEntry[0]), int(monthEntry[1]), 1)
    one_day = datetime.timedelta(days=1)
    if WinTestmode:
      dateEnd=datetime.date(int(monthEntry[0]), int(monthEntry[1]), 3)-one_day
    else:
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

if WinTestmode:
  directory='C:/Users/schultes/Desktop/billing'



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

#GD for day in range(1,int(dateEnd.day)+1):
for day in range( int(dateStart.day),int(dateEnd.day)+1):
  print('')
  print('')
  transferList=[]
  requestList=[]
  p2pList=[]

  dateDayToProcess=datetime.date(int(monthEntry[0]), int(monthEntry[1]), day)
  filename=directory+'/billing-'+dateDayToProcess.strftime('%Y.%m.%d')
  print('file which is being processed: '+ filename)

#f=open('/opt/d-cache/billing/2010/06/billing-2010.06.30', 'r')
#f=open('C:/Users/schultes/Desktop/billing/billing-2010.06.30')
  if os.path.exists(filename):
    f=open(filename)
    linesBilling=f.readlines()
    f.close
  else:
    print ("ERROR: File "+filename+" does not exist")
    continue
  #for transfer in f:

  hostListDetail=[]
  domainListDetail=[]
  datavolumeListDetail=[]
  datavolumeWritingListDetail=[]
  datavolumeReadingListDetail=[]
  transferDurationListDetail=[]
  transferTimeListDetail=[]


  #with open('C:/Users/schultes/Desktop/billing/billing-2010.06.01', 'r') as f:

  debug=options.debug
  indexBilling=0
  numberOfBillingEntries= len(linesBilling)
  indexBilling=numberOfBillingEntries-1
  print(format('Number of all entries: ', 65) + format(numberOfBillingEntries, -12))
  while len(linesBilling)> 0:#len(linesBilling):
    # deleting entry which is not the requested protoco
    if len(linesBilling)%100000 == 0:
      print('entries found for'+ protocol +': '+ str(len(requestList)) +', entries still to be processed: '+ str(len(linesBilling)) +', sum of entires: '+ str(numberOfBillingEntries))      
    lastLine = linesBilling[-1]
#01.31 00:00:16 [pool:sn14@sn14Domain:transfer] [000039EF0ACDD2BC42D0A74CC8C9B8AC010E,881569198] [Unknown] atlas:LOCALGROUPDISK@osm 881569198 9584 false {DCap-3.0,132.195.124.212:24045} [<undefined>] {0:""}
    if protocol=='DCap' and lastLine.find('door:')<0 and lastLine.find('pool:')>0 and lastLine.find('transfer')>0:
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
       if debug >=3:
         print('deleting entry: ' + linesBilling[-1])
    # classifying the entry
    else:
      if linesBilling[-1].find(':request]')>=0:
# example for a failed request:
# 07.04 01:06:16
# door:GFTP-grid-se-Unknown-276078@gridftp-grid-seDomain:request]
# "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=girolamo/CN=614260/CN=Alessandro Di Girolamo":10761:1307:samnag013.cern.ch]
# 00009E4E45AA4F0349BBA28B3D2A9DD692AB,0]
# /pnfs/physik.uni-wuppertal.de/data/atlas/generated/testfile-put-1278198362-38fa2b40d5e7.txt] <unknown> 1278198376014 0 {451:"Operation failed: FTP Door: got response from '[>PoolManager@dCacheDomain:*@dCacheDomain:SrmSpaceManager@srm-grid-seDomain:*@srm-grid-seDomain:*@dCacheDomain]' with error No write pools configured for <atlas:GENERATED@osm> for linkGroup: atlas_scratchdisk-LinkGroup"}
        requestExitCode = int(linesBilling[-1].split(' {')[1].split(':')[0])
        if requestExitCode>0:
          failedRequestList.append(linesBilling[-1])
          if debug >= 1: print('INFO: request failed: ' + linesBilling[-1])
        else:
          requestList.append(linesBilling[-1])

      if linesBilling[-1].find(':transfer]')>=0:
# example for a failed request:
# 07.05 07:04:07
# pool:sn14@sn14Domain:transfer]
# 0000D065DD1A98594BDE927F8E7869AD899E,0]
# Unknown] atlas:DATADISK@osm 3429498880 3598854 true {GFtp-2.0 grid-se.physik.uni-wuppertal.de 54099}
# door:GFTP-grid-se-Unknown-281222@gridftp-grid-seDomain:1278302632625-280160] {10001:"No such file or directory: 0000D065DD1A98594BDE927F8E7869AD899E"}
        requestExitCode = int(linesBilling[-1].split(' {')[2].split(':')[0])
        if requestExitCode>0:
          failedTransferList.append(linesBilling[-1])
          if debug >= 1: print('INFO: transfer failed: ' + linesBilling[-1])
        else:
          transferList.append(linesBilling[-1])

      indexBilling-=1
    del linesBilling[-1]
 
  print(format('Number of of triggered Pool2pool replications:',65) + format(len(p2pList),-12))
  print(format('Number of entries with door:'+protocol+':', 65) + format(len(linesBilling), -12))
  print(format('Number of entries with door:'+protocol+' and :request]', 65) + format(len(requestList), -12))
  print(format('Number of entries with door:'+protocol+' and :transfer]', 65) + format(len(transferList), -12))
  print(format('overall number of entries with door:'+protocol+' and failed request:', 65) + format(len(failedRequestList), -12))
  print(format('overall number of entries with door:'+protocol+' and failed transfer:', 65) + format(len(failedTransferList), -12))


  indexTransfer=0
#  while indexTransfer<len(transferList and requestList)!='':
  while indexTransfer<len(transferList) and requestList!='':
    if len(transferList)%5000 ==0:
      print('transfer entries still to be processed: '+ str(len(transferList)))
  #xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx transfer xxxaxxxxxxxxxxxxxxxxxxxxxxxxxxx
  #06.01 01:08:54
  #pool:sn03@sn03Domain:transfer]
  #0000E82ABA02230A4421BE206816D83DAF42,468373]
  #Unknown] atlas:SCRATCHDISK@osm 468373 10 true {GFtp-1.0 grid-se.physik.uni-wuppertal.de 40340}
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:1275347334131-58042] {0:""}
    transfer=transferList[indexTransfer].split(' [')
    transferPNFSid=''

    if debug>=2: print('++++++++++++++++++++++++++++++')
    if debug>=2: print(transfer)
    if transfer=="": break

    if len(transfer)>=5:
  #06.01 01:08:54
      transferTime=transfer[0].split()[1] #GD only time
  #pool:sn03@sn03Domain:transfer]
      #transferPool=transfer[1].split(':')
  #0000E82ABA02230A4421BE206816D83DAF42,468373]
      transferPNFS=transfer[2].strip(']').split(',')
      if len(transferPNFS)>=2:
        transferPNFSid=transferPNFS[0]
        #transferFilesize=int(transferPNFS[1])/1024/1024

  #Unknown] atlas:SCRATCHDISK@osm 468373 10 true {GFtp-1.0 grid-se.physik.uni-wuppertal.de 40340}
      transferDetails=transfer[3].strip(']').split(' ')
      if len(transferDetails)>=6:
        #transferSpacetoken=transferDetails[1]
        transferDatavolume=round(float(transferDetails[2])/1024/1024,3)
        transferDuration=transferDetails[3]
        transferDirection=transferDetails[4]
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:1275347334131-58042] {0:""}
      transferDoor=transfer[4].split('@')[0]
      
      if protocol=='DCap':
        requesthost = transfer[3].strip(':').strip('{').strip('}').split(',')[1].split(':')[0]

  #xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx request xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  #06.01 01:08:54
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:request]
  #"/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker":10761:1307:wn092.pleiades.uni-wuppertal.de]
  #0000E82ABA02230A4421BE206816D83DAF42,0]
  #/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user10.AgnieszkaLeyko/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306_sub07654925/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306._1076473737.log.tgz] <unknown> 1275347334131 0 {0:""}
    indexRequest=-1
    requestDoor=''
    requestPNFSid=''
    noRequestEntry=False

  # testing request entries for fiting to transfer entry.
  # if a fitting pair is found, the request entry has not to be tested for the next transfers
    while transferDoor != requestDoor and transferPNFSid!=requestPNFSid:
      indexRequest+=1
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:request]
      if requestList!='' and indexRequest<len(requestList):
        request=requestList[indexRequest].split(' [')
        requestDoor=request[1].split('@')[0]
        requestPNFSid=request[3].strip(']').split(',')[0]
      else:
        requestDoor=''
      
      if indexRequest>=len(requestList)-1:
        noRequestEntry=True
        requestEntryInFailed=False
        for failedRequest in failedRequestList:
          if failedRequest.find(transferDoor)>=0:
            requestEntryInFailed=True
            break
        break
    if noRequestEntry:
      if requestEntryInFailed:
        del transferList[indexTransfer]
      else:
        print('ERROR: no request entry found for', transfer)
        print(transferDoor)
        del transferList[indexTransfer]
      continue
    else:
      if debug>=2: print('deleting: indexRequest'+ str(indexRequest)+'/'+str(len(requestList)), 'indexTransfer'+str(indexTransfer)+'/'+str(len(transferList)))
      del transferList[indexTransfer]
      del requestList[indexRequest]
      
    if len(request)>=5:
  #06.01 01:08:54
      requestTime=request[0]
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:request]
      requestDoor=request[1].split('@')[0]
      
  #"/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker":10761:1307:wn092.pleiades.uni-wuppertal.de]
  #"/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gridmon/CN=137254/CN=Robot: Grid Monitoring-Framework/E=grid.monitoring-framework@cern.ch":21000:2031:sam011.cern.ch
      requestCredentials=request[2].strip(']').split(':')
      if len(requestCredentials) >=4 and protocol=='GFTP':
        requesthost=requestCredentials[-1]
  #0000E82ABA02230A4421BE206816D83DAF42,0]
      requestPNFS=request[3].strip(']').split(',')
      if len(requestPNFS)>=2:
        requestPNFSid=requestPNFS[0]
        #requestFilesize=requestPNFS[1]
  #/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user10.AgnieszkaLeyko/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306_sub07654925/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306._1076473737.log.tgz] <unknown> 1275347334131 0 {0:""}
      requestDetails=request[4].strip(']').split(' ')
      requestFile=requestDetails[0]

      if debug>=1: print('------------------------------')
      if requesthost.find('.')>=0 and requesthost.upper()!=requesthost.lower():
        requestDomain=requesthost.split('.',1)[1]
      elif requesthost.find('.')>=0 and requesthost.upper()==requesthost.lower():
        requestDomain=requesthost.split('.')[0]+'.'+requesthost.split('.')[1]+'.*'
      else:
        requestDomain=requesthost
      if float(transferDuration)>0:
          transferSpeed=float(transferDatavolume)/float(transferDuration)/1024
      else: transferSpeed=0.0

      if debug>=1:
        print(protocol+' transfer')
        print('File:'+requestFile)
        print('Datavolume transfered:' + str(transferDatavolume) +' MByte')
        print('Duration:' + transferDuration +' sec')
        print('transfer speed:' + str(transferSpeed) +' kByte/sec')

      if transferDirection.find('true')>=0:
        transferDatavolumeWriting=transferDatavolume
        transferDatavolumeReading=0.0
      if transferDirection.find('false')>=0:
        transferDatavolumeWriting=0.0
        transferDatavolumeReading=transferDatavolume

      hostListDetail.append(requesthost)
      domainListDetail.append(requestDomain)
      datavolumeListDetail.append(round(transferDatavolume,3))
      datavolumeWritingListDetail.append(round(transferDatavolumeWriting,3))
      datavolumeReadingListDetail.append(round(transferDatavolumeReading,3))
      transferDurationListDetail.append(transferDuration)
      transferTimeListDetail.append(transferTime)

      hostMonthlyListDetail.append(requesthost)
      domainMonthlyListDetail.append(requestDomain)
      datavolumeMonthlyListDetail.append(round(transferDatavolume,3))
      datavolumeWritingMonthlyListDetail.append(round(transferDatavolumeWriting,3))
      datavolumeReadingMonthlyListDetail.append(round(transferDatavolumeReading,3))
      transferDurationMonthlyListDetail.append(transferDuration)
          

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
    else: hostTransferRate=0.0
#    print("host: " +host, "; Datavolume:" +str(hostDatavolume) + " MByte", "; Number of Transfers: " +str(hostTransferNumber), "; Transferrate: " +str(hostTransferRate)+ " MByte/sec", " Datavolume writing: ", str(hostDatavolumeWriting) + " MByte", " Datavolume reading: ", str(hostDatavolumeReading) + " MByte")

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

print('')
print('')
print('')
print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
print(str(len(transferList)) + ' '+protocol+' transfers not resolved:')
for entryGFTPtransfer in transferList:
  print('INFO: A transfer without request '+entryGFTPtransfer)
print(str(len(requestList)) + ' '+protocol+' requests not resolved:')
for entryGFTPrequest in requestList:
  print('INFO: '+entryGFTPrequest)

print(format('overall number of entries with door:'+protocol+' and failed request:', 65) + format(len(failedRequestList), -12))
print(format('overall number of entries with door:'+protocol+' and failed transfer:', 65) + format(len(failedTransferList), -12))

#print('+++++++++++++++ monthly '+protocol+' summary for domains for '+dateStart.strftime('%Y-%m')+' [sorted by Duration] +++++++++++++++')
#PrintDomainResult(domainResults, protocol, 3)
print('*** Summary ***')
PrintDomainResult(domainResults, protocol)

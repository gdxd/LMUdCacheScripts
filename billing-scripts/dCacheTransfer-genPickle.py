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



import os, glob, datetime, time, gzip, cPickle
import simpleTiming
from optparse import OptionParser

from dCacheTransferParseUtil import *


def setupParser():


  BDIR = '/var/lib/dcache/billing'
  parser = OptionParser()
  parser.add_option('-m','--month', help="Month: 2010-06. If this option is specified, then it has the highest priority and the --start-date and --end-date are not taken into account",dest="month",default="empty")
  parser.add_option('-s','--start-date',help="The starting date: 2010-06-01", dest="dateStart", default=yesterday())
  parser.add_option('-e','--end-date', help="The ending date", dest="dateEnd", default=yesterday())
  parser.add_option('-d','--debuglevel',help="Debug level", dest="debug",default=0)
  parser.add_option('-f','--directory',help="Path to billing directories, default is " + BDIR, dest="directory",default=BDIR)
  parser.add_option('-o','--pickle-out-dir',help="Dir to store decoded transfers in pickle file", dest="pickleOutDir",default="./")

  return parser


if __name__ == "__main__" :
  parser = setupParser()

  try:
    #option parsing
    (options, args) = parser.parse_args()


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

    directory=options.directory+'/'+dateStart.strftime('%Y/%m')




  
  except ImportError:
    sys.exit("Error: wrong options")


  print('local folder which will be processed:' +directory)

  failedRequestList=[]
  failedTransferList=[]

  # load host dictionary
  hd = getHostDict( '/root/dCache/billing-scripts' )
  if hd :
    host_ip_dict = hd
    
  
  mytimer = simpleTiming.timing()
  mytimer.start()


  nfprocess = 0
#  for day in range( int(dateStart.day),int(dateEnd.day)+1):
  for day in range( dateStart.toordinal(),dateEnd.toordinal()+1):
    print('')
    print('')
    transferList=[]
    requestList=[]
    requestDict={}
    failedRequestDict={}
    p2pList=[]
    rmList=[]
    trsumList=[]
    failedTrReqList=[]

#    dateDayToProcess=datetime.date(int(monthEntry[0]), int(monthEntry[1]), day)
    
    dateDayToProcess=datetime.date.fromordinal(day)
    directory=options.directory+'/'+dateDayToProcess.strftime('%Y/%m')
    filename=directory+'/billing-'+dateDayToProcess.strftime('%Y.%m.%d')

    print('file which is being processed: '+ filename +'[.gz]')

    f=fileOpen(filename)

    if ( f == None ): continue
    
    linesBilling=f.readlines() 
    f.close()

    nfprocess += 1


    print(format('Time for reading: ', 65) + format(mytimer.getdiff(), -12))




    nentries=0
    numberOfBillingEntries= len(linesBilling)
    print(format('Number of all entries: ', 65) + format(numberOfBillingEntries, -12))
    while len(linesBilling)> 0:

      indexBilling = len(linesBilling)-1
      if debug >= 1 and indexBilling%100000 == 0 :
        print('entries found for : '+ str(len(requestDict)) +', entries still to be processed: '+ str(len(linesBilling)) +', sum of entries: '+ str(numberOfBillingEntries))      
      lastLine = linesBilling.pop()

      if lastLine.find('doorDomain:remove]') > 0 : # skip deletion entries
        continue



#01.31 00:00:16 [pool:sn14@sn14Domain:transfer] [000039EF0ACDD2BC42D0A74CC8C9B8AC010E,881569198] [Unknown] atlas:LOCALGROUPDISK@osm 881569198 9584 false {DCap-3.0,132.195.124.212:24045} [<undefined>] {0:""}


      # p2p transfers
      # 2017-04-19 20:20:45+02:00	mMsg:	[pool:lcg-lrz-dc51_2:transfer]	[pool:lcg-lrz-dc51_2:1492626045975-4614]	[0000B4497E9EEC614864887A24E563F5CD4A:Unknown]	940698B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[0]	[0:0]	0ms	[Http-1.1:129.187.131.50:0:lcg-lrz-dc50_2:pool_lcg-lrz-dc50_2:/0000B4497E9EEC614864887A24E563F5CD4A]	[pool:lcg-lrz-dc50_2@pool_lcg-lrz-dc50_2]	p2p	download	940698B	302ms	[Unknown]	[0:""]
      if  lastLine.find('door:')<0 and lastLine.find('pool:')>0 and lastLine.find('transfer')>0:
        trsumObj = decodeP2PLine(lastLine)
        if trsumObj != None:
          p2pList.append( trsumObj )

      if  lastLine.find(':remove')>0 and lastLine.find('drMsg:')>0 :
        rmObj = decodeRmLine(lastLine)
        if rmObj != None:
          rmList.append( rmObj )

      if lastLine.find('door:')<0:
        continue

    # classifying the entry
      nentries += 1
      if lastLine.find(':request]')>=0:
# example for a failed request:
# 07.04 01:06:16
# door:GFTP-grid-se-Unknown-276078@gridftp-grid-seDomain:request]
# "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=girolamo/CN=614260/CN=Alessandro Di Girolamo":10761:1307:samnag013.cern.ch]
# 00009E4E45AA4F0349BBA28B3D2A9DD692AB,0]
# /pnfs/physik.uni-wuppertal.de/data/atlas/generated/testfile-put-1278198362-38fa2b40d5e7.txt] <unknown> 1278198376014 0 {451:"Operation failed: FTP Door: got response from '[>PoolManager@dCacheDomain:*@dCacheDomain:SrmSpaceManager@srm-grid-seDomain:*@srm-grid-seDomain:*@dCacheDomain]' with error No write pools configured for <atlas:GENERATED@osm> for linkGroup: atlas_scratchdisk-LinkGroup"}
#GDold        requestExitCode = int(lastLine.split(' {')[1].split(':')[0])
        try:
          requestExitCode = int(lastLine.split('\t')[-1].strip('[]').split(':')[0])
        except:
          continue
        if requestExitCode>0:
          #          failedRequestList.append(lastLine)
          # parse info and store in object
          reqObj = RequestObj( lastLine, indexBilling )
          failedRequestList.append( (reqObj, None) ) # dummy tuple
          failedRequestDict[reqObj.hash]=reqObj
          if debug >= 3: print('INFO: request failed: ' + lastLine)
        else:

          # parse info and store in object
          reqObj = RequestObj( lastLine, indexBilling )
          if reqObj.ok:
#            requestList.append(reqObj)
            if reqObj.hash in requestDict:
              print 'found request already in requestDict : ' + reqObj.hash
            else:
              requestDict[reqObj.hash]=reqObj
          if debug >=3:
            print('Found request: ' + str(reqObj.ok) + lastLine)

      if lastLine.find(':transfer]')>=0:
# example for a failed transfer:
# 07.05 07:04:07
# pool:sn14@sn14Domain:transfer]
# 0000D065DD1A98594BDE927F8E7869AD899E,0]
# Unknown] atlas:DATADISK@osm 3429498880 3598854 true {GFtp-2.0 grid-se.physik.uni-wuppertal.de 54099}
# door:GFTP-grid-se-Unknown-281222@gridftp-grid-seDomain:1278302632625-280160] {10001:"No such file or directory: 0000D065DD1A98594BDE927F8E7869AD899E"}
#GDold        requestExitCode = int(lastLine.split(' {')[2].split(':')[0])
        try:
          requestExitCode = int(lastLine.split('\t')[-1].strip('[]').split(':')[0])
        except:
          continue
        if requestExitCode>0:
#          failedTransferList.append(lastLine)

          traObj = TransferObj( lastLine, indexBilling )
          failedTransferList.append(traObj)
          if debug >= 3: print('INFO: transfer failed: ' + lastLine)

        else:

          traObj = TransferObj( lastLine, indexBilling )

          if traObj.ok:
            transferList.append(traObj)
          if debug >=3:
            print('Found transfer: ' + str(traObj.ok) + lastLine)

    p2pvol = 0
    for tro in p2pList:
      p2pvol += tro.datavolume # MB

    rmvol = 0
    for rmo in rmList:
      rmvol += rmo.datavolume # MB



    # end loop billing entries
    print(format('Number of of triggered Pool2pool replications:',65) + format(len(p2pList),-12))
    print(format('Volume of triggered Pool2pool replications: (GB)',65) + format(p2pvol/1000,-12))
    print(format('Number of entries : ', 65) + format(nentries, -12))
    print(format('Number of entries request :', 65) + format(len(requestDict), -12))
    print(format('Number of entries transfer:', 65) + format(len(transferList), -12))
    print(format('Number of entries removed:', 65) + format(len(rmList), -12))
    print(format('Volume of entries removed: (GB)', 65) + format(rmvol/1000,-12))
    print(format('overall number of entries with failed request:', 65) + format(len(failedRequestList), -12))
    print(format('overall number of entries with failed transfer:', 65) + format(len(failedTransferList), -12))

    print(format('Time for parsing: ', 65) + format(mytimer.getdiff(), -12))

    # clean up
    rmList = []
    
    # loop over transfers
    trreqList=[]
    trorphanList=[]
    indexTransfer=-1
    indexRequest=0
    nReq = len(requestDict)
    
    for trobj in transferList:
      indexTransfer+=1
      if debug >=1 and indexTransfer%10000 ==0:
        print('transfer entries still to be processed: '+ str(len(transferList)-indexTransfer))

      noRequestEntry=False


      try:
        ro = requestDict[trobj.hash]
        trs = TrSum( trobj=trobj, reqobj=ro )
        trsumList.append( trs )
        if debug>=4 :
          print('Trsum :', trs.fname, trs.pnfsid, trs.host)
      except:
        if  trobj.hash in failedRequestDict:
          trs = ( failedRequestDict[trobj.hash], trobj  )
          failedTrReqList.append( trs )
        else:
          trorphanList.append(trobj)
          if debug>=2 :
            print('ERROR: no request entry found for', trobj.line)



    # end transfer loop


    print(format('Time for matching: ', 65) + format(mytimer.getdiff(), -12))


    print(format('Overall number of matching transfers/requests: ', 65) + format(len(trsumList), -12 ))
    print(format('Overall number of matching transfers/failed requests: ', 65) + format(len(failedTrReqList), -12 ))
    print(format('Overall number of unmatched transfers: ', 65) + format(len(trorphanList), -12 ))
    



    # loop over failed transfers
    failedtrorphanList=[]
    indexTransfer=-1
    for trobj in failedTransferList:
      indexTransfer+=1
      if debug >=1 and indexTransfer%10000 ==0:
        print('failed transfer entries still to be processed: '+ str(len(failedTransferList)-indexTransfer))

      indexRequest=-1
      noRequestEntry=False

      if  trobj.hash in failedRequestDict:
        trs = ( failedRequestDict[trobj.hash], trobj  )
        failedTrReqList.append( trs )
      else:
        failedtrorphanList.append(trobj)
        if debug>=3 :
          print('ERROR: no request entry found for', trobj.line)



    # end transfer loop


    print(format('Overall number of matching failed transfers/requests : ', 65) + format(len(failedTrReqList), -12 ))
    print(format('Overall number of unmatched failed requests : ', 65) + format(len(failedRequestList), -12 ))
    print(format('Overall number of unmatched failed transfers: ', 65) + format(len(failedtrorphanList), -12 ))
    


    if options.pickleOutDir=='BILLINGDIR':
      outdir = directory
    else:
      outdir = options.pickleOutDir

    storeTrObjs( trsumList, outdir, str(dateDayToProcess) )
    storeTrObjs( failedTrReqList + failedRequestList, outdir, 'error-'+str(dateDayToProcess) )
#    storeTrObjs( failedRequestList, outdir, 'error-request-'+str(dateDayToProcess) )
    print(format('Time for storing w/ cPickle: ', 65) + format(mytimer.getdiff(), -12))

  storeHostDict( host_ip_dict, '/root/dCache/billing-scripts' )

# end __main__

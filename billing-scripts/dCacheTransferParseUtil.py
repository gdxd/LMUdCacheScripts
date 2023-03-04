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

# 2015-02-19 00:00:00+01:00	mMsg:	[pool:lcg-lrz-dc58_0:transfer]	[pool:lcg-lrz-dc58_0:1424300400849-7]	[0000731E7D09517C449CA279ABB3AF1C0B80:/upload/526a0860-e8f8-477a-a8b3-8a3b5b61555a/testfile-PUT-ATLASDATADISK-1424300395-eac4503b9849.txt]	21B	[atlas:DataDisk@osm]	[]	[/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ddmadmin/CN=531497/CN=Robot: ATLAS Data Management]	[[/atlas/Role=production]:[/atlas/lcg1|/atlas/usatlas|/atlas/Role=production|/atlas]]	[prdatl01]	[50201]	[1307:1307]	0ms	[GFtp-2.0 188.184.149.151 48774]	[door:ftp.gsi_lcg-lrz-dc59-920@ftp_lcg-lrz-dc59:AAUPZMcevSA-1424300396153000]	no-p2p	upload	21B	42ms	[0:""]
# 2015-02-19 00:00:01+01:00	drMsg:	[door:srm_lcg-lrz-srm@srm_lcg-lrz-srm:request]	[OJw:50539:srm2:prepareToPut:-1589540520:-1589540519]	[0000731E7D09517C449CA279ABB3AF1C0B80:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/SAM/testfile-PUT-ATLASDATADISK-1424300395-eac4503b9849.txt]	21B	[atlas:DataDisk@osm]	[]	[/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ddmadmin/CN=531497/CN=Robot: ATLAS Data Management]	[[/atlas/Role=production]:[/atlas/lcg1|/atlas/usatlas|/atlas/Role=production|/atlas]]	[prdatl01]	[50201]	[1307:1307]	0ms	188.184.149.151	0ms	[0:""]

# 2017 ... xrootd
# 2017-05-09 00:00:09+02:00	mMsg:	[pool:lcg-lrz-dc65_5:transfer]	[pool:lcg-lrz-dc65_5:1494280809746-70470]	[0000119F777AE52F4872987D2269AB305382:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/mc15_13TeV/b1/06/EVNT.11008871._000862.pool.root.1]	190966039B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[]	[:]	0ms	[Xrootd-2.7:10.156.72.19:47568]	[door:xrootd_lcg-lrz-dc10:AAVPBvOeSCA:1494280809580000]	no-p2p	download	10236B	105ms	[/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/mc15_13TeV/b1/06/EVNT.11008871._000862.pool.root.1]	[0:""]
# 2017-05-09 00:00:09+02:00	drMsg:	[door:xrootd_lcg-lrz-dc10@xrootd_lcg-lrz-dc10:request]	[door:xrootd_lcg-lrz-dc10:AAVPBvOeSCA:1494280809580000]	[0000119F777AE52F4872987D2269AB305382:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/mc15_13TeV/b1/06/EVNT.11008871._000862.pool.root.1]	190966039B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[]	[:]	0ms	[10.156.72.19]	170ms	[/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/mc15_13TeV/b1/06/EVNT.11008871._000862.pool.root.1]	[0:""]


# 2018 ... xrootd
#2018-07-09 00:00:02+02:00       mMsg:   [pool:lcg-lrz-dc15_7@pool_lcg-lrz-dc15_7:transfer]      [pool:lcg-lrz-dc15_7@pool_lcg-lrz-dc1 5_7:1531087202633-4828]    [000037009D3E4F494DB7B4DAC918DDF673FD:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/data16_13TeV/b3/2a/DAOD_HIGG5D3.10309715._000201.pool.root.1]    891641577B      [atlas:DataDisk@osm]    []      []      [[]:[]] []      []      [:]     0ms     [Xrootd-2.7:10.156.72.1:38724]  [door:xrootd_lcg-lrz-dc11@xrootd_lcg-lrz-dc11:AAVwg+k6png:1531086739523000]     no-p2p  download        362028946B      372MiB/s        -MiB/s  463095ms        18152ms 444939ms        -ms     -ms     [/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/data16_13TeV/b3/2a/DAOD_HIGG5D3.10309715._000201.pool.root.1] [0:""]
#2018-07-09 00:00:02+02:00       drMsg:  [door:xrootd_lcg-lrz-dc11@xrootd_lcg-lrz-dc11:request]  [door:xrootd_lcg-lrz-dc11@xrootd_lcg-lrz-dc11:AAVwg+k6png:1531086739523000]     [000037009D3E4F494DB7B4DAC918DDF673FD:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/data16_13TeV/b3/2a/DAOD_HIGG5D3.10309715._000201.pool.root.1]    891641577B      [atlas:DataDisk@osm]    []      []      [[]:[]] []      []      [:]     0ms     [10.156.72.1]   463112ms        [/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/data16_13TeV/b3/2a/DAOD_HIGG5D3.10309715._000201.pool.root.1] [0:""]

# mMsg:	[pool:lcg-lrz-dc62_0@pool_lcg-lrz-dc62_0:transfer]	[pool:lcg-lrz-dc62_0@pool_lcg-lrz-dc62_0:1531157313131-198029]	[00008407CE1B780946D88D7FA2B68218723C:Unknown]	10139324B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[0]	[0:0]	0ms	[Http-1.1:129.187.131.32:0:lcg-lrz-dc32_7:pool_lcg-lrz-dc32_7:/00008407CE1B780946D88D7FA2B68218723C]	[pool:lcg-lrz-dc32_7@pool_lcg-lrz-dc32_7]	p2p	download	10139324B	2.31E3MiB/s	-MiB/s	262ms	217ms	45ms	-ms	-ms	[Unknown]	[0:""]

# mMsg:	[pool:lcg-lrz-dc62_3@pool_lcg-lrz-dc62_3:transfer]	[pool:lcg-lrz-dc62_3@pool_lcg-lrz-dc62_3:1531139249109-174318]	[0000CB86F83D1B4D45F8AE096A0F25DF6235:Unknown]	17853472B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[0]	[0:0]	0ms	[Http-1.1:129.187.131.14:0:lcg-lrz-dc14_5:pool_lcg-lrz-dc14_5:/0000CB86F83D1B4D45F8AE096A0F25DF6235]	[pool:lcg-lrz-dc14_5@pool_lcg-lrz-dc14_5]	p2p	download	24576B	78.8MiB/s	-MiB/s	63894ms	16ms	63877ms	-ms	-ms	[Unknown]	[666:"java.io.IOException: Broken pipe"]

#
import os, glob, datetime, time, gzip, pickle, socket
import simpleTiming
from optparse import OptionParser


def yesterday():
  d=datetime.datetime.fromordinal(datetime.datetime.today().toordinal()-1).date().isoformat()
  return d


host_ip_dict = {}


class TrSum( object): 
  " class for summary info per transfer "
  def __init__( self, proto="", host="", datavolume="", write=None, duration=0, time="", fname="", trobj=None, reqobj=None ):
    if not trobj:
      self.proto =          proto
      self.host  =          host       
      self.datavolume =     datavolume
      self.write =          write  
      self.duration =       duration
      self.time     =       time        
      self.fname     =      fname
      self.domain = ''
    else:
      self.proto = reqobj.proto
      self.datavolume  = trobj.Datavolume
      self.duration  = trobj.Duration
      self.write  = trobj.Write
      self.pool  = trobj.Pool
      self.fsize = trobj.fsize
      self.time = reqobj.Time

      if self.proto == 'GFTP' and self.write :
        self.host  = reqobj.host
      else:
        self.host  = trobj.host



      try:
#        self.fname = reqobj.line.split('[')[4].split(']')[0]
        self.fname = trobj.fname
      except:
        self.fname = ''

      # if self.host.find('.')>=0:
      #   if self.host.upper() == self.host.lower(): # ip num

      #     try:
      #       hname = host_ip_dict[self.host]
      #     except:
      #       hname = socket.getfqdn(self.host)
      #       host_ip_dict[self.host] = hname
      #       #
      #     self.host = hname
      #     #
      #     self.domain = self.host.split('.',1)[1]

      # elif len(self.host.split(':'))>6 : # IPv6
      #   try:
      #     hname = host_ip_dict[self.host]
      #   except:
      #     hname = socket.getfqdn(self.host)
      #     host_ip_dict[self.host] = hname
      #       #
      #   self.host = hname
      #   #
      #   da = self.host.split('.',1)
      #   if len(da) > 1:
      #     self.domain = da[1]
      #   else:
      #     self.domain=self.host
          
          

      # else:
      #   self.domain=self.host


      try:
        hname = host_ip_dict[self.host]
      except:
        hname = socket.getfqdn(self.host)
        host_ip_dict[self.host] = hname
        #
      self.host = hname
      #
      if self.host.upper() == self.host.lower(): # IPv4
        self.domain=self.host
      else:      
        da = self.host.split('.',1)
        if len(da) > 1:
          self.domain = da[1]
        else:
          self.domain=self.host
          
          


        
      self.pnfsid = reqobj.PNFSid



class RmSum( object): 
  " class for summary info per remove "
  def __init__( self, host="", datavolume="", time="", fname="",pool="", pnfsid="" ):
    self.host  =         host       
    self.datavolume =    datavolume
    self.time     =      time        
    self.fname    =      fname
    self.pool     =      pool
    self.pnfsid   =      pnfsid




class TransferObj( object ):
  " class to contain billing transfer info"
  def __init__( self, line, entry ):
    self.entry = entry
    self.line = ''
    self.Write = False 
    self.host = ''
    self.fname = ''
    transfer = line.split('\t')

    self.message  = ''
    #reqobj.line.split('\t')[-1].strip('[]')


    try:
  #'00:00:00+02:00'
      self.Time=transfer[0].split()[1].split('+')[0]
  #[0000D7FAA2114E8F4B3092CD50496963C594:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasdatadisk/rucio/data15_13TeV/7a/4e/DAOD_TOPQ1.10297373._000313.pool.root.1]
      PNFSINFO=transfer[4].strip('[]').split(':')
      self.PNFSid = PNFSINFO[0]
      self.fname = PNFSINFO[1]
      bytes = transfer[5].strip('B')
      self.fsize  = float(bytes)/(1024*1024)

#GD18      bytes = transfer[-4].strip('B')
      bytes = transfer[-10].strip('B')
      self.Datavolume=float(bytes)/(1024*1024)
#GD18      secs = transfer[-3].strip('ms')
      secs = transfer[-7].strip('ms')
      self.Duration=float(secs)

#GD18      Direction=transfer[-5]
      Direction=transfer[-11]
      if Direction.find('upload')>=0:
        self.Write = True 
  #door:GFTP-grid-se-Unknown-58146@gridftp-grid-seDomain:1275347334131-58042] {0:""}
#GD18      self.Door = transfer[-7].strip('[]').split(':')[1]
      self.Door = transfer[-13].strip('[]').split(':')[1]

      self.Pool = transfer[2].strip('[]').split('@')[0].split(':')[1]

      self.message  = transfer[-1].strip('[]')


      # # works only for dcap
      # try:
      #   self.host = transfer[3].strip(':').strip('{').strip('}').split(',')[1].split(':')[0]
      #   if self.host.find('.')>=0:
      #     if self.host.upper()!=self.host.lower():
      #       self.Domain=self.host.split('.',1)[1]
      #     else:
      #       self.Domain=self.host.split('.')[0]+'.'+self.host.split('.')[1]+'.*'
      #   else:
      #     self.Domain=self.host

      # 
      try:
        # dcap/http
#GD18        hostinf = transfer[-8]
        hostinf = transfer[-14]
        if hostinf.upper().find("DCAP") >= 0  :
          self.host = hostinf.split(',')[1].split(':')[0]
        elif hostinf.upper().find("HTTP") >= 0 or hostinf.upper().find("XROOT") >= 0:
          self.host = hostinf.split(':')[1]

        # gftp
        elif hostinf.upper().find("GFTP") >= 0 :
          self.host = hostinf.split()[1]

      except:
        pass


#GD18      self.hash = hash(transfer[-7].strip('[]'))
      self.hash = hash(transfer[-13].strip('[]'))

      self.ok = True

    except Exception as x:
      print("Transfer parsing troubles: ", x.__class__.__name__ , ' : ', x, ' Line: ', line)
      print(self.__dict__)
      print('transfer[3:5]=', transfer[3:5])
      self.ok = False



  def __str__( self ):
    return self.line

class RequestObj( object):
  " class to contain billing request info"
  def __init__( self, line, entry ):
    self.entry = entry
    #self.line = line
    self.line = ''
    self.host = ''
    self.message = ''

#    request = line.split('\t[')
    request = line.split('\t')

    try:
#      self.Door=request[1].split('@')[0]
      tag = request[2].strip('[]')
      self.Door=tag.split('@')[0]
      # decode protocol
      proto = self.Door.split(':')[1]
      pend = proto.find('lcg-lrz') - 1 # lrz specific
      self.proto = proto[0:pend].upper()

      self.PNFSid=request[4].strip('[]').split(':')[0]
      self.Time=request[0].split()[1].split('+')[0]

      self.message  = request[-1].strip('[]')
      
      #"/C=DE/O=GermanGrid/OU=LMU/CN=Rodney Walker":10761:1307:wn092.pleiades.uni-wuppertal.de]
      #"/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=gridmon/CN=137254/CN=Robot: Grid Monitoring-Framework/E=grid.monitoring-framework@cern.ch":21000:2031:sam011.cern.ch
      #      requestCredentials=request[2].strip(']').split(':')
      #      if len(requestCredentials) >=4 and self.proto=='GFTP':
      try:
        self.host=request[-4].strip('[]')
        self.File=request[-2].strip('[]')
      except Exception as x:
        pass



  #0000E82ABA02230A4421BE206816D83DAF42,0]
  #/pnfs/physik.uni-wuppertal.de/data/atlas/atlasscratchdisk/user10.AgnieszkaLeyko/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306_sub07654925/user10.AgnieszkaLeyko.mc09_7TeV.106043.PythiaWenu_no_filter.merge.AOD.e468_s765_s767_r1302_r1306._1076473737.log.tgz] <unknown> 1275347334131 0 {0:""}



      #
      self.hash = hash(request[3].strip('[]'))

      self.ok = True
    except Exception as x:
      print("Request parsing troubles: ", x.__class__.__name__ , ' : ', x,  ' Line: ',line)
      self.ok = False

  def __str__( self ):
    return self.line

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

  
class SumObj(object):
  def __init__(self):
    self.volume = 0 # sum vol transfered
    self.vsize = 0  # sum vol filesize
    self.duration = 0 # sum transfer duration
    self.writevolume = 0 # sum vol written
    self.number = 0   # sum number access
    self.numberLargeRead = 0 # sum number access > 10 MB
  pass


def getSumbyTag( tag, ititem, bynode = True ):
  "sum tranfer info by tag"


  tags = ['bydomain','bypool', 'byprotocol', 'byspacetoken', 'byfile', 'bydataset', 'bydstag']
  try:
      tagind = tags.index(tag)
  except:
      print('getSumbyTag unknown tag', tag)
      return []

  tagResults={}
  
  while ( True ) :
    try:
      ts = ititem.__next__()
    except StopIteration:
      break

    if tagind  == 0:
        key = ts.domain
    elif tagind  == 1:
        if bynode:
            key = ts.pool.split('_')[0]
        else:
            key = ts.pool

    elif tagind  == 2:
        key = ts.proto
    elif tagind == 3:
        if len(ts.fname)<2 : continue # sanity check

        try:
            key = ts.fname.strip('/').split('/')[5]
            if key.find('atlas')<0 and key.find('belle')<0 : # skip non-atlas/belle entries
                continue
        except Exception as x:
            #      print("Troubles: ", x.__class__.__name__ , ':', x)
            continue

    elif tagind == 4:
        if len(ts.fname)<2 : continue # sanity check

        try:
            spacetoken = ts.fname.strip('/').split('/')[5]
            if spacetoken.find('atlas')<0 and key.find('belle')<0  : # skip non-atlas/belle entries
                continue
        except Exception as x:
            #      print("Troubles: ", x.__class__.__name__ , ':', x)
            continue

        # combine fname and pnfsid
        key = ts.pnfsid +':'+ ts.fname
    elif tagind == 5:
        if len(ts.fname)<2 : continue # sanity check

        try:
            fnparts = ts.fname.strip('/').split('/')
            fn = fnparts[5]
            if fn.find('atlasdatadisk')<0 and  fn.find('atlaslocalgroupdisk')<0 and  fn.find('atlasgroupdisk')<0 and  fn.find('localgroupdisk')<0  : # only these spacetokens
                continue
            # extract ds-name
            key = fnparts[-2]
        except Exception as x:
            #      print("Troubles: ", x.__class__.__name__ , ':', x)
            continue

    else:
        if len(ts.fname)<2 : continue # sanity check

        try:
            fnparts = ts.fname.strip('/').split('/')
            fn = fnparts[5]
            if fn.find('atlasdatadisk')<0 and  fn.find('atlaslocalgroupdisk')<0 and  fn.find('atlasgroupdisk')<0 and  fn.find('localgroupdisk')<0   : # only these spacetokens
                continue
            # extract ds-name tags
            dstags = fnparts[-2].split('.')
            if dstags[0] == 'user':
              key = dstags[0]+'***'
            else:
              key = dstags[0]+'_'+dstags[4]
        except Exception as x:
            #      print("Troubles: ", x.__class__.__name__ , ':', x)
            continue





    if key == None: continue
    
    try:
      sumobj = tagResults[key]
      
    except Exception as x:
#      print("Troubles: ", x.__class__.__name__ , ':', x)
#      print('Domain:',ts.domain)
      sumobj =  SumObj()
      tagResults[key] = sumobj

    sumobj.volume += ts.datavolume
    sumobj.vsize += ts.fsize
    if ts.write:
      sumobj.writevolume += ts.datavolume
    sumobj.duration += ts.duration
    sumobj.number += 1
    if ts.datavolume>10. :  # count transfers >10 MB
      sumobj.numberLargeRead += 1


  # convert into list
  tagResList=[]
  for k in tagResults.keys():
    domain = k
    volume = tagResults[k].volume # volume
    vsize  = tagResults[k].vsize # file size sum
    number = tagResults[k].number
    numberLargeRead = tagResults[k].numberLargeRead
    duration = tagResults[k].duration # duration
    if duration>0: 
      rate = float(volume)/duration
    else:
      rate = 0
    vwrite= tagResults[k].writevolume # volume writing
    vread = volume - vwrite

    tagResList.append( (domain, volume/1024., number, duration, rate, vwrite/1024., vread/1024., numberLargeRead, vsize/1024.) )

  return(tagResList, tagResults)


def getErrorSumbyTag( tag, ititem, errorType = None, bynode = True):
  "sum error info by tag"


  tags = ['byprotocol', 'bypool', 'bycode', 'byerrmess'  ]
  try:
    tagind = tags.index(tag)
  except:
    print('getSumbyTag unknown tag', tag)
    return []

#  print('getErrorSumbyTag:', tag, tagind)

  tagResults = {}

  while ( True ) :
    try:
      (reqobj, trobj) = ititem.__next__()
    except StopIteration:
      break

    # distinguish request-only errors and transfer errors
    if errorType != None:
      if errorType == "transfer":
        if trobj == None: continue
      elif errorType == "request":
        if trobj != None: continue
        

    key = None
    if tagind  == 0:
        key = reqobj.proto
    elif tagind  == 1:
      if trobj!=None :
        if bynode:
            key = trobj.Pool.split('_')[0]
        else:
            key = trobj.Pool


    elif tagind  == 2:
      try:
#        message  = reqobj.line.split('\t')[-1].strip('[]')
#        message = reqobj.line.split('{')[1]
        parts = reqobj.message.split(':')
        key = parts[0]
      except Exception as x:
        print("Troubles: ", x.__class__.__name__ , ':', x)
        print(reqobj.line)
        print(message)
        continue

    elif tagind  == 3:
      try:
        message  = reqobj.message.split(':')[1]
#        message = reqobj.line.split('{')[1].strip()
#        key = message[0:70]
        key = message[0:70].strip()
      except Exception as x:
        print("Troubles: ", x.__class__.__name__ , ':', x)
        print(reqobj.line)
        print(message)
        continue


    if key == None: continue
    

    try:
      tagResults[key] += 1
      
    except Exception as x:
#      print("Troubles: ", x.__class__.__name__ , ':', x)
#      print('Domain:',ts.domain)
      tagResults[key] = 1
#      print(key, tagResults[key])
      
  # convert into list
  tagResList=[]
  for k in tagResults.keys():
    tag = k
    tagResList.append( (tag, tagResults[k] ) )


#  print len(

  return(tagResList, tagResults)



def storeTrObjs( trsumList, dirname, datestr ):
  """store transfer objects in pickle file"""
  fname = 'dctr-'+datestr+'.cpickle.gz'
  try:
    fil=gzip.open(dirname+'/'+fname, 'wb' )
    for obj in trsumList:
      pickle.dump( obj, fil, protocol=2 )

    fil.close()
  except Exception as x:
    print("storeTrObjs Troubles: ", x.__class__.__name__ , ':', x)

def PrintDomainResult(domainResults, protocol='GFTP', nlprmax = 200, sortKey=1):
  print(format('Domain', '35') + str('DV [GB]').center(15) + str('Transfers').center(15) + str('Duration [sec]').center(15) + str('Rate [kB/s]').center(15) +' | '+ str('write [GB]').center(15) + str('read [GB]').center(15))
  Datavolume=0
  TransferNumber=0
  Duration=0
  DVwrite=0
  DVread=0
  nl = 0
  for domainResultLine in sorted(domainResults, key=lambda entry: entry[sortKey], reverse=True):
    if nl<nlprmax:
      try:
        print(format(domainResultLine[0], 35) + format("%12.3f" % domainResultLine[1], -15) + format(domainResultLine[2], -15) + format("%12.0f" % (domainResultLine[3]/1000), -15)  + format("%10.3f" % ( domainResultLine[1]*1e9/domainResultLine[3] ), -15)+' | '+ format("%12.3f" % domainResultLine[5], -15) + format("%12.3f" % domainResultLine[6], -15))
      except Exception as x:
        print("PrintDomainResult Troubles: ", x.__class__.__name__ , ':', x)
    nl += 1

    Datavolume+=domainResultLine[1]
    TransferNumber+=domainResultLine[2]
    Duration+=domainResultLine[3]
    DVwrite+=domainResultLine[5]
    DVread+=domainResultLine[6]
  print('---------------------------------------------------------------------------------+-------------------------------')
  if Duration <= 0.:
    print('Error, Duration 0 for ' + protocol)
    Duration = 1e-6
  print(format('Sum '+ protocol +':', -35) + format("%12.3f" % Datavolume, -15) + format(TransferNumber, -15) + format("%12.0f" % (Duration/1000), -15)  + format("%10.3f" % ( Datavolume*1e9/Duration ), -15)  +' |'+ format("%12.3f" % DVwrite, -15) + format("%12.3f" % DVread, -15))
  print('=================================================================================+===============================')
  return True


def PrintErrorResult(tagResults, protocol='GFTP', tag=None,nlprmax = 200, sortKey=1):

  if tag == 'byerrmess':
    nc=70
  else:
    nc=35

  print(format('Tag', nc) + ' Number ' )
  count = 0
  nl = 0
  for tagResultLine in sorted(tagResults, key=lambda entry: entry[sortKey], reverse=True):
    if nl<nlprmax:
      print(format(tagResultLine[0], nc) + format("%10d" % tagResultLine[1], -15))

    nl += 1
    count += tagResultLine[1]

  print('---------------------------------------------------------------------------------+-------------------------------')
  print(format('Sum '+ protocol +':', -nc) + format("%10d" % count, -15 ))
  print('=================================================================================+===============================')
  return True



    

def PrintFileResult( results, nfmin=50, sortKey=2 ):
  nfmax = [1,2,3,5,10,20,50,100,500,1000,-1]
  nelem = len(nfmax)
  sumv = nelem*[0]
  sumvu = nelem*[0]
  sumvul = nelem*[0]
  sumn = nelem*[0]
  sumvs = nelem*[0]
  sumvsu = nelem*[0]
  cumsumvu = 0.
  cumsumvul = 0.
  cumsumvs = 0.
  cumsumvsu = 0.
  indnf = 0
  for result in sorted(results, key=lambda entry: entry[sortKey]):
    nr = result[2]
    nrl = result[7] # reads larger 10 MB
    vsize = result[8] # file size
    if nrl<1: nrl = 1

    while indnf<nelem-1 and nr>nfmax[indnf]:
      indnf += 1

    sumv[indnf] += result[1]
    sumvu[indnf] += result[1]/nr
    sumvul[indnf] += result[1]/nrl
    sumn[indnf] += 1

    sumvs[indnf] += vsize
    sumvsu[indnf] += vsize/nr

    if nr > nfmin:
      sstr = 'atlas/dq2/'
      ind = result[0].find(sstr)
      try:
        fname = result[0][ind+len(sstr):] # substring after 'atlas/dq2'
      except:
        fname = result[0]

      pnfsid = result[0].split(':')[0]
      if fname.find("atlaslocalgroupdisk") >=0 or fname.find("atlasdatadisk") >= 0 or nr > 200:
        print(format(pnfsid, 40) + format(fname, 35) + format("%12.3f" % result[1], -15) + format(result[2], -15))


  for i in range(nelem):
    if sumn[i] > 0:
      cumsumvu += sumvu[i]
      cumsumvul += sumvul[i]
      cumsumvs += sumvs[i]
      cumsumvsu += sumvsu[i]
      print("Files read <=%5d times: %5d %7.0f  %7.0f  %7.0f  %7.0f  %7.0f  %7.0f  %7.0f  %7.0f  %7.0f" % ( nfmax[i], sumn[i], sumv[i], sumvu[i], cumsumvu, sumvul[i], cumsumvul, sumvs[i],cumsumvs,sumvsu[i],cumsumvsu))



def PrintSumPerHour( datavolumeListDetail, transferTimeListDetail ):
  arr=24*[0.]

  for i in xrange(len(datavolumeListDetail)):
    # '01:08:54'
    try:
      if ( transferDurationListDetail[i] > 0 ): # no p2p transfers
        hour = int(transferTimeListDetail[i].split(':')[0])
        arr[hour] += datavolumeListDetail[i]
    except:
      print('PrintSumPerHour trouble', i, transferTimeListDetail[i])

  print('### Transfer per hour :')
  for i in range(len(arr)):
    print("%5d   %10.0f" % (i, arr[i]))

  print(' ')
    
def decodeRmLine( line ):
  'handle remove entries'
  # 2017-05-08 00:30:53+02:00	drMsg:	[door:webdav.tls_lcg-lrz-dc14@webdav_lcg-lrz-dc14:remove]	[door:webdav.tls_lcg-lrz-dc14@webdav_lcg-lrz-dc14:1494196253742-2065551]	[000075832962B8144B29AC8FB82E1F52EE9D:/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasscratchdisk/rucio/panda/a2/a5/panda.um.group.perf-muons.11300211.EXT1._000070.muonscale.root]	10466567B	[<unknown>]	[]	[/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=ddmadmin/CN=531497/CN=Robot: ATLAS Data Management]	[[/atlas/Role=production]:[/atlas/lcg1|/atlas/Role=production|/atlas|/atlas/usatlas]]	[prdatl01]	[50201]	[1307:1307]0ms	[unknown]	0ms	[/pnfs/lrz-muenchen.de/data/atlas/dq2/atlasscratchdisk/rucio/panda/a2/a5/panda.um.group.perf-muons.11300211.EXT1._000070.muonscale.root]	[0:""]

  try:
    p2pinfo = line.split('\t')
    host = p2pinfo[2].split(':')[1].split('_')[-1]
#    host = line.split()[9].split(',')[1].split(':')[0]
#    domain = host.split('.')[0]+'.'+host.split('.')[1]+'.*'
    volume = float(p2pinfo[5].strip('B'))/(1024.*1024)
    mtime = p2pinfo[0].split()[1]
    finfo = p2pinfo[3].strip('[]').split(':')
    fname = finfo[1]
    pnfsid = finfo[0]
    rmobj = RmSum( host, volume, mtime, fname, None, pnfsid )
  except Exception as x:
    print("Troubles in decodeRmLine ", x.__class__.__name__ , ':', x, line)
    rmobj = None
  return rmobj



def decodeP2PLine( line ):
  'handle p2p transfer entries'
  #OLD '12.04 23:57:20 [pool:lcg-lrz-dc56_7:transfer] [000052104D06328C473A983D7125002D483C,160216] [Unknown] atlas:ScratchDisk@osm 160216 324 false {Http-1.1lcg-lrz-dc61.grid.lrz.de:0:lcg-lrz-dc61_8:pool_lcg-lrz-dc61_8Domain:/000052104D06328C473A983D7125002D483C} [pool:lcg-lrz-dc61_8@pool_lcg-lrz-dc61_8Domain] [p2p=true] {0:""}'

  # 2017-04-19 20:20:45+02:00	mMsg:	[pool:lcg-lrz-dc51_2:transfer]	[pool:lcg-lrz-dc51_2:1492626045975-4614]	[0000B4497E9EEC614864887A24E563F5CD4A:Unknown]	940698B	[atlas:DataDisk@osm]	[]	[]	[[]:[]]	[]	[0]	[0:0]	0ms	[Http-1.1:129.187.131.50:0:lcg-lrz-dc50_2:pool_lcg-lrz-dc50_2:/0000B4497E9EEC614864887A24E563F5CD4A]	[pool:lcg-lrz-dc50_2@pool_lcg-lrz-dc50_2]	p2p	download	940698B	302ms	[Unknown]	[0:""]

  try:
    p2pinfo = line.split('\t')
#    host = p2pinfo[line.split()[9].split(':')[2].split('_')[0]
    host = p2pinfo[14].split(':')[3]
#    host = line.split()[9].split(',')[1].split(':')[0]
#    domain = host.split('.')[0]+'.'+host.split('.')[1]+'.*'
#GD18    volume = float(p2pinfo[-4].strip('B'))/(1024.*1024)
    volume = float(p2pinfo[-10].strip('B'))/(1024.*1024)
    mtime = p2pinfo[0].split()[1]

    trobj = TrSum( 'P2P', host, volume, None, 0, mtime, None )
  except Exception as x:
    print("Troubles in decodeP2PLine ", x.__class__.__name__ , ':', x, line)
    trobj = None
  return trobj

def fileOpen( filename ):
  "try to open file gzipped or ungzipped"
  filenamegz = filename + '.gz'     # try compressed
    
  if os.path.exists(filename):
    f=open(filename)
  elif os.path.exists(filenamegz):
    f=gzip.open(filenamegz)
  else:
    print("ERROR: File "+filename+" does not exist")
    f=None

  return f


def getTrObjs( filelist, protocol = None ):
  """
  read transfer objects from pickle file
  first uncompressing and then doing pickle.load from uncompressed file is much faster than
  doing it from compressed file (3 s vs 30 s)
  Also pickle from uncompressed String is much slower, probably some CIO magic ...
  """

  spid = str(os.getpid)

  for fname in filelist:

    print('open file ', fname)


    try:
      if fname.find('.gz')>0 :
        fil=gzip.open( fname, 'rb' )
      else:
        fil=open( fname, 'rb' )

      bytes = fil.read()
      print(len(bytes))
      fil.close()

      tmpfile = '/tmp/babbaluba.'+spid
      fo = open(tmpfile,'wb')
      fo.write(bytes)
      fo.close()
      
      fo = open(tmpfile,'rb')
      
      while True:
        try:
          obj = pickle.load(fo)
          if protocol != None: 
            if protocol == 'LOCAL':
              while obj.proto == 'GFTP': # skip GFTP
                obj = pickle.load(fo)
            else:
              while obj.proto != protocol:
                obj = pickle.load(fo)

          yield obj
#          yield pickle.load(fil)
        except EOFError: break
      fo.close()
      os.unlink(tmpfile)
#      fil.close()
    except Exception as x:
      print("getTrObjs Troubles: ", x.__class__.__name__ , ':', x)
     
def getReqTrObjs( filelist, protocol = None ):
  """
  read request/transfer objects from pickle-error file
  first uncompressing and then doing pickle.load from uncompressed file is much faster than
  doing it from compressed file (3 s vs 30 s)
  Also pickle from uncompressed String is much slower, probably some CIO magic ...
  """

  spid = str(os.getpid)

  for fname in filelist:

    print('open file ', fname)


    try:
      if fname.find('.gz')>0 :
        fil=gzip.open( fname, 'rb' )
      else:
        fil=open( fname, 'rb' )

      bytes = fil.read()
#      print(len(bytes))
      fil.close()

      tmpfile = '/tmp/babbaluba.'+spid
      fo = open(tmpfile,'wb')
      fo.write(bytes)
      fo.close()
      
      fo = open(tmpfile,'rb')
      
      while True:
        try:
          obj = pickle.load(fo)
          yield obj
#          yield pickle.load(fil)
        except EOFError: break
      fo.close()
      os.unlink(tmpfile)
#      fil.close()
    except Exception as x:
      print("getTrObjs Troubles: ", x.__class__.__name__ , ':', x)
     
def storeHostDict( hdict, dirname ):
  """store host dict in pickle file"""
  fname = dirname+'/'+'hostDict.cpickle.gz'
  try:
    fil=gzip.open(fname, 'wb' )
    pickle.dump( hdict, fil, protocol=2 )
    fil.close()
    print("Stored host dict with ", len(hdict), " Entries ")
  except Exception as x:
    print("storeHostDict Troubles: ", x.__class__.__name__ , ':', x)

def getHostDict( dirname ):
  """get host dict from pickle file"""
  fname = dirname+'/'+'hostDict.cpickle.gz'
  try:
    fil=gzip.open(fname, 'rb' )
    hdict = pickle.load( fil )
    fil.close()
    print("Read host dict with ", len(hdict), " Entries ")
    return hdict
  except Exception as x:
    print("getHostDict Troubles: ", x.__class__.__name__ , ':', x)
    return None

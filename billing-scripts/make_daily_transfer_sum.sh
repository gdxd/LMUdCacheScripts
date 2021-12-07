#!/bin/sh
#
# GD 14/12/11   added error report
#

# option for day as argument (e.g. 2017-05-03)
day="yesterday"
dayopts=""
if [ "x$1" != "x"  ]; then
    #chk valid date
    date -d $1
    if [ $? -eq 0 ]; then
	day=$1
	dayopts="-s $1 -e $1"
    fi
fi

cd /root/dCache/billing-scripts
# BDIR=/data/dcache/billing
BDIR=/var/lib/dcache/billing
fname=`/bin/mktemp`
echo '########## Transfer Reports   ######################' > $fname
python3 dCacheTransfer-genPickle.py -f $BDIR -o BILLINGDIR $dayopts >> $fname
# python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S byprotocol >> $fname
# python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p gftp >> $fname
# python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain,bypool,byspacetoken,bydstag -p local  >> $fname
python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR $dayopts >> $fname
python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p FTP.GSI $dayopts >> $fname
#python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p DCAP.PLAIN $dayopts >> $fname
python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p XROOTD $dayopts >> $fname
python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p WEBDAV.TLS $dayopts >> $fname
python3 dCacheTransferReport-fromPickle.py -i BILLINGDIR -S bydomain -p REMOTETRANSFERMANAG $dayopts >> $fname

echo '\n\n##########   Error  Reports   ######################' >> $fname
python3 dCacheTransferErrorReport-fromPickle.py  -i BILLINGDIR $dayopts >> $fname
#
datestring=`/bin/date -d $day +'%F'`

/bin/mv $fname /tmp/billing.txt

# mailx -a broken
#echo ""| mailx -a /tmp/billing.txt -s "dCache Billing Stat for $datestring" Guenter.Duckeck@Physik.Uni-Muenchen.DE
cat /tmp/billing.txt  | mailx -s "dCache Billing Stat for $datestring" Guenter.Duckeck@Physik.Uni-Muenchen.DE christoph.anton.mitterer@lmu.de

# rm -f $fname




#!/bin/sh

#initialise and secure the shell execution environment
unset -v IFS
PATH='/usr/sbin:/sbin:/usr/bin:/bin'

date >> /var/log/dCache_adjust_movers.log
cd /root/dCache/scripts
/usr/bin/python3 ./dcache_adjust_movers.py 2>&1 >> /var/log/dCache_adjust_movers.log  

exit 0


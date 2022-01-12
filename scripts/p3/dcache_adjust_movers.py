#!/usr/bin/env python

#GD 14/06/11: fixed fail_ids() 

import sys, time, os, re, subprocess



import parsePoolInfo
import dcache_set_movers

DEBUG=False
#DEBUG=True

# LRZ specific 
infourl = "http://lcg-lrz-dcache0.grid.lrz.de:59998/info/pools"
# NMOVERDEF = 15
# increased GD Jan-18, 2018
# back to 20 GD Jan-31, 2018
# down to 15 GD Feb-8, 2018
# up to 25 GD Apr-12, 2018
# down to 15 GD Sep-19, 2018
# up to 30 GD Nov-7, 2019
NMOVERDEF = 30
NMOVERMAX = 250

# list of pools to exclude from adjustment0
# POOLSIGNORE = [19,32,28,999]
# POOLSIGNORE = [28,0] # dummy
POOLSIGNORE = [999]
# store in dict
POOLLOAD = {}

#
def getPoolLoad( pool ):
    # pool = lcg-lrz-dc25_2 --> lcg-lrz-dc25.grid.lrz-muenchen.de
    if pool in POOLLOAD :
        return POOLLOAD[pool]

    load = -1
    domain = 'grid.lrz.de'
    phost = pool.split('_')[0] + '.' + domain
    

    s,o = subprocess.getstatusoutput("ssh " + phost + "  uptime ")
    # '  7:14pm  up 92 days  0:55,  0 users,  load average: 11.19, 10.66, 7.97'

    if s == 0:
        ls = o.index('load')
        if ls>=0 :
            try:
                load = float( o[ls:].split()[2].split(',')[0]) # 1-min load
            except:
                pass
    
    POOLLOAD[pool] = load
    return load


# LRZ specific 
def pname2num( pool ):
    "extract pool-node number from name : lcg-lrz-dc37_3 --> 37"
    pnode = pool.split('_')[0]
    pbase = 'lcg-lrz-dc'
    pnum = int(pnode[len(pbase):])

    return pnum


def checkLoadOk( pool, load ):

    if load < 0 : 
#	return True  # GD hack
        return False

    pnum = pname2num( pool )

    if pnum<=10: # 2013 Dell pools
        if load < 20 :
            return True
    elif pnum<49: # 2015+ HP stuff
        if load < 20 :
            return True
    elif pnum<55: # 1st Dell pools
        if load < 12 :
            return True
    elif pnum<68: # 2nd Dell pools
        if load < 20 :
            return True



    return False

# LRZ specific        
def poolIgnore( pool ):
    "check whether pool should be excluded from queue adjustment"
    pnum = pname2num( pool )
    if pnum in POOLSIGNORE: 
        return True
    else:
        return False
    

def main():
    poolinfo = parsePoolInfo.doParse( infourl )


    plreset = {}
    pladjust={}


    nactive=0
    nqueued=0
    for ptag in sorted(poolinfo.keys()):
        qp =  poolinfo[ptag]

        pool = ptag.split(':')[0]

        if poolIgnore( pool ) : 
            if DEBUG: print('Pool ignored : ', ptag)
            continue

        if DEBUG: print(ptag, qp.maxactive, qp.active, qp.queued)

        nactive += qp.active
        nqueued += qp.queued

        queue = ptag.split(':')[1]


        # reduce max movers if not needed
        if qp.maxactive > NMOVERDEF:
            if ( not checkLoadOk(pool, getPoolLoad( pool )) ):
                plreset[ptag] = NMOVERDEF
            elif qp.active + qp.queued < qp.maxactive - 8:
#                nmovernew = max( qp.active + qp.queued, NMOVERDEF )
                nmovernew = max( qp.maxactive - 5, NMOVERDEF )
                plreset[ptag] = nmovernew
	elif qp.maxactive < NMOVERDEF:
            if checkLoadOk(pool, getPoolLoad( pool )) :
	        plreset[ptag] = NMOVERDEF

        # increase movers if queued movers
        if qp.queued>0 and qp.active + qp.queued > qp.maxactive and qp.maxactive < NMOVERMAX:
            pload = getPoolLoad( pool )

            if checkLoadOk(pool, pload ):
                nmovernew = min( qp.maxactive + 10, NMOVERMAX )
                pladjust[ptag] = nmovernew
            else:
                print("Not increasing movers on %s %d , too high load %f" % ( pool,  qp.maxactive, pload ))

    print('adjust_movers: In total %d active movers and %d queued' % ( nactive, nqueued ))


    if len(plreset) == 0 and len(pladjust) == 0:
        print('adjust_movers: no action')

    else:
        if len(plreset) > 0:
            print('adjust_movers: reducing movers for ', len(plreset), ' pools ')
            dcache_set_movers.adjust_pools( plreset )

        if len(pladjust) > 0:
            print('adjust_movers: increasing movers for ', len(pladjust), ' pools ')

            dcache_set_movers.adjust_pools( pladjust, test=False )

    #
                
if __name__ == '__main__':
    main()

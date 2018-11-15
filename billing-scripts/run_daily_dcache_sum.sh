#!/bin/sh
PATH='/usr/sbin:/sbin:/usr/bin:/bin'
#
echo 'Starting daily dCache summary at ' $(date)
/root/dCache/billing-scripts/make_daily_transfer_sum.sh
#/root/dCache/billing-scripts/make_daily_transfer_sum.sh >/dev/null 2>&1

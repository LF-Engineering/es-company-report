#!/bin/bash
dt=`date '+%Y-%m-%dT%H:%M:%SZ'`
mkdir $dt 1>/dev/null 2>/dev/null || exit 1
cp -v prod_*.log prod_*.csv "$dt"
echo "copied to $dt"

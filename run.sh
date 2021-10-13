#!/bin/bash
if [ -z "$1" ]
then
  echo "$0: you need to specify env as a 1st arg, for example test or prod"
  exit 1
fi
ES_URL="`cat ES_URL.$1.secret`" DB_URL="`cat DB_URL.$1.secret`" ES_LOG_URL="`cat ES_URL.log.secret`" ./report

#!/bin/bash
if [ -z "$GOPATH" ]
then
  export GOPATH="${HOME}/go"
fi
cd "${GOPATH}/src/github.com/LF-Engineering/es-company-report" || exit 1
git pull || exit 2
make report || exit 3
./move.sh || exit 4
date >> prod_datalake.log
echo "---------------------------------" >> prod_datalake.log
INCREMENTAL=1 REPORT=datalake MAX_THREADS=6 DATASOURCES='git,github-issue,gerrit,jira,bugzilla,bugzillarest,confluence' NAME_PREFIX=prod ./run.sh prod | tee -a prod_datalake.log
echo '' >> prod_datalake.log

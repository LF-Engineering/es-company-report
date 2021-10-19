#!/bin/bash
INCREMENTAL='' REPORT=datalake MAX_THREADS=6 DATASOURCES='git,github-issue,gerrit,jira,bugzilla,bugzillarest,confluence' NAME_PREFIX=full_prod ./run.sh prod | tee -a full_prod_datalake.log

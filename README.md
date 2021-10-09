# es-company-report

Report given company's contribution data stored in ElasticSearch

# Running

- Specify report type via `REPORT=org`.
- Provide your `ES_URL.${env_name}.secret` value(s).
- Provide your `DB_URL.${env_name}.secret` value(s).
- Run `[DBG=1] [FROM=YYYY-MM-DD] [TO=YYYY-MM-DD] REPORT=org ORG='Facebook, Inc.' ./run.sh env_name`.
- Run `[DBG=1] [MAX_THREADS=10] [NAME_PREFIX=env-name] [DATASOURCES='git,github-issue,gerrit,jira,bugzilla,bugzillarest,confluence'] [SUB_REPORTS='loc,prs,issues,docs'] REPORT=datalake ./run.sh env_name`.


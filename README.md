# es-company-report

Report given company's contribution data stored in ElasticSearch

# Running

- Specify report type via `REPORT=org`.
- Provide your `ES_URL.${env_name}.secret` value(s).
- Provide your `DB_URL.${env_name}.secret` value(s).
- Run `[FROM=YYYY-MM-DD] [TO=YYYY-MM-DD] REPORT=org ORG='Facebook, Inc.' ./run.sh env_name`.


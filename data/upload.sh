set -ex

extract-gdc gdc.jsonl.gz gdc.samples-per-file.jsonl.gz
cda-transform gdc.jsonl.gz gdc.H.jsonl.gz ../gdc-transform.yml
cda-aggregate ../merge.yml gdc.H.jsonl.gz gdc.A.jsonl.gz

extract-pdc pdc.jsonl.gz pdc.files-per-sample-dict.json.gz
cda-transform pdc.jsonl.gz pdc.H.jsonl.gz ../pdc-transform.yml
cda-aggregate ../merge.yml pdc.H.jsonl.gz pdc.A.jsonl.gz

cda-merge ../merge.yml gdc.pdc.M.jsonl.gz --gdc gdc.A.jsonl.gz --pdc pdc.A.jsonl.gz

# bq load --autodetect --source_format NEWLINE_DELIMITED_JSON cda_mvp.v2 gdc.pdc.M.jsonl.gz

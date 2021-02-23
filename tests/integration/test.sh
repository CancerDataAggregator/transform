set -ex

# Both these tests use the cache

extract-gdc gdc.jsonl.gz ../../data/gdc.samples-per-file.jsonl.gz ../../data/gdc.fileuuid.jsonl.gz --cases gdc-case-list.txt  
python check-gdc-pull.py gdc.jsonl.gz gdc-case-list.txt

extract-pdc pdc.jsonl.gz ../../data/pdc.files-per-sample-dict.json.gz --cases pdc-case-list.txt
python check-pdc-pull.py pdc.jsonl.gz pdc-case-list.txt

cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz ../../gdc-transform.yml

cda-transform pdc.jsonl.gz pdc.transf.jsonl.gz ../../pdc-transform.yml

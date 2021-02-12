set -ex

# If the cache files are missing, download them.
for FILE in gdc.samples-per-file.jsonl.gz pdc.files-per-sample-dict.json.gz; do
    if [ ! -f "$FILE" ]; then
      curl --remote-name https://storage.googleapis.com/broad-cda-dev/public/$FILE
    fi
done

# Both these tests use the cache

extract-gdc gdc.jsonl.gz gdc.samples-per-file.jsonl.gz --cases gdc-case-list.txt  
python check-gdc-pull.py gdc.jsonl.gz gdc-case-list.txt

extract-pdc pdc.jsonl.gz pdc.files-per-sample-dict.json.gz --cases pdc-case-list.txt
python check-pdc-pull.py pdc.jsonl.gz pdc-case-list.txt

cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz gdc-transform.yml

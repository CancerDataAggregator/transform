set -ex

# If the cache files are missing, download them.
for FILE in gdc.files-specimens-cases.jsonl.gz pdc.files-specimens-cases.jsonl.gz pdc.all_cases.jsonl.gz all_subjects.jsonl.gz; do
    if [ ! -f "$FILE" ]; then
      gsutil cp gs://broad-cda-dev/public/$FILE $FILE
      #curl --remote-name https://storage.googleapis.com/broad-cda-dev/public/$FILE
    fi
done

set -ex

# If the cache files are missing, download them.
for FILE in gdc.samples-per-file.jsonl.gz pdc.files-per-sample-dict.json.gz; do
    if [ ! -f "$FILE" ]; then
      curl --remote-name https://storage.googleapis.com/broad-cda-dev/public/$FILE
    fi
done

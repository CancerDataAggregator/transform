set -ex

# If the cache files are missing, download them.
for FILE in gdc.files-specimens-cases.jsonl.gz pdc.files-specimens-cases.jsonlgz; do
    if [ ! -f "$FILE" ]; then
      curl --remote-name https://storage.googleapis.com/broad-cda-dev/public/$FILE
    fi
done

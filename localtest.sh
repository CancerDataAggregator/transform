cd data
sh get-cache-files.sh
cd ../tests/integration
# Test cache generation
extract-gdc gdc.jsonl.gz gdc.new.cache.jsonl.gz 
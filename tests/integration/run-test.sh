#!/bin/sh

extract-gdc gdc.jsonl.gz --cases gdc-case-list.txt
extract-pdc pdc.jsonl.gz --cases pdc-case-list.txt
cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz gdc-transform.yml

# Generate the md5sum for the generated text and compare it with a previous run.
# Note that md5sum on the gz doesn't work because the gz file has a timestamp in its header.
gzip --decompress --to-stdout gdc.transf.jsonl.gz | md5sum --status --check gdc.transf.jsonl.gz-checksum.txt
RESULT=$?
if [ $RESULT -ne 0 ]; then
  echo ERROR
  cat transform.log
else
  # Only delete output if no error occurred.
  rm -f gdc.jsonl.gz pdc.jsonl.gz gdc.transf.jsonl.gz transform.log
fi
exit $RESULT

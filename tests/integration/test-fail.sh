# Check that cda-transform fails for error in transform list
set -x

if cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz gdc-transform-bad.yml; then
    exit 1
else
    exit 0
fi

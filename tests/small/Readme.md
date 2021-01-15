# Create small test cases


```
# Script to extract a case and generate the original and transformed JSON 
CASE=d90249dc-40e8-449e-a24a-6d461f29f632

python ../../utility/extract-case.py ../../gdc.jsonl.gz gdc.${CASE}.orig.json ${CASE}
gzip gdc.${CASE}.orig.json -c > gdc.${CASE}.orig.json.gz

cda-transform gdc.${CASE}.orig.jsonl.gz gdc.${CASE}.transf.json.gz ../../gdc-transform.yml
gunzip gdc.${CASE}.transf.json.gz
```

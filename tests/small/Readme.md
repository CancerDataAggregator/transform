# Create small test cases

## GDC
```
# Script to extract a case and generate the original and transformed JSON 
CASE=d90249dc-40e8-449e-a24a-6d461f29f632

extract-gdc --case $CASE gdc.${CASE}.orig.jsonl.gz
gunzip gdc.${CASE}.orig.jsonl.gz -c > gdc.${CASE}.orig.json

cda-transform gdc.${CASE}.orig.jsonl.gz gdc.${CASE}.transf.json.gz ../../gdc-transform.yml
gunzip gdc.${CASE}.transf.json.gz
```

## PDC
```
# Script to extract a case and generate the original and transformed JSON 
CASE=01b2691b-63d8-11e8-bcf1-0a2705229b82

extract-pdc --case $CASE pdc.${CASE}.orig.jsonl.gz
gunzip pdc.${CASE}.orig.jsonl.gz -c > pdc.${CASE}.orig.json

# cda-transform pdc.${CASE}.orig.jsonl.gz gdc.${CASE}.transf.json.gz ../../gdc-transform.yml
# gunzip gdc.${CASE}.transf.json.gz
```

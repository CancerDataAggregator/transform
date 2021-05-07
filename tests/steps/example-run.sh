set -ex

# Generate GDC examples

extract-gdc gdc_TARGET_case1.jsonl.gz ../../data/gdc.samples-per-file.jsonl.gz --case 7eeced68-1717-4116-bcee-328ac70a9682
extract-gdc gdc_TARGET_case2.jsonl.gz ../../data/gdc.samples-per-file.jsonl.gz --case 9e229e56-f7e1-58f9-984b-a9453be5dc9a

gunzip -c gdc_TARGET_case1.jsonl.gz > gdc_TARGET_case1.json
gunzip -c gdc_TARGET_case2.jsonl.gz > gdc_TARGET_case2.json

cda-transform gdc_TARGET_case1.jsonl.gz gdc.case1.H.json.gz ../../GDC_mapping.yml GDC
python json2yaml.py gdc.case1.H.json.gz gdc.case1.H.yml

cda-transform gdc_TARGET_case2.jsonl.gz gdc.case2.H.json.gz ../../GDC_mapping.yml GDC
python json2yaml.py gdc.case2.H.json.gz gdc.case2.H.yml


cat gdc_TARGET_case1.jsonl.gz gdc_TARGET_case2.jsonl.gz > gdc.jsonl.gz

cda-transform gdc.jsonl.gz gdc.H.jsonl.gz ../../GDC_mapping.yml GDC
cda-aggregate ../../merge.yml gdc.H.jsonl.gz gdc.A.jsonl.gz
python json2yaml.py gdc.A.jsonl.gz gdc.A.yml


# Generate PDC examples

extract-pdc pdc_QC1_case1.jsonl.gz ../../data/pdc.files-per-sample-dict.json.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
extract-pdc pdc_QC1_case2.jsonl.gz ../../data/pdc.files-per-sample-dict.json.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83

gunzip -c pdc_QC1_case1.jsonl.gz > pdc_QC1_case1.json
gunzip -c pdc_QC1_case2.jsonl.gz > pdc_QC1_case2.json

cda-transform pdc_QC1_case1.jsonl.gz pdc.case1.H.json.gz ../../pdc-transform.yml
python json2yaml.py pdc.case1.H.json.gz pdc.case1.H.yml

cda-transform pdc_QC1_case2.jsonl.gz pdc.case2.H.json.gz ../../pdc-transform.yml
python json2yaml.py pdc.case2.H.json.gz pdc.case2.H.yml

cat pdc_QC1_case1.jsonl.gz pdc_QC1_case2.jsonl.gz > pdc.jsonl.gz

cda-transform pdc.jsonl.gz pdc.H.jsonl.gz ../../pdc-transform.yml
cda-aggregate ../../merge.yml pdc.H.jsonl.gz pdc.A.jsonl.gz
python json2yaml.py pdc.A.jsonl.gz pdc.A.yml

# Merge aggregated GDC and PDC files
cda-merge ../../merge.yml gdc.pdc.M.jsonl.gz --gdc gdc.A.jsonl.gz --pdc pdc.A.jsonl.gz

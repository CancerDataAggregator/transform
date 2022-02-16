set -ex

# Generate GDC examples

extract-gdc gdc_TARGET_case1.jsonl.gz ../../data/gdc.files-specimens-cases.jsonl.gz --case 7eeced68-1717-4116-bcee-328ac70a9682
extract-gdc gdc_TARGET_case2.jsonl.gz ../../data/gdc.files-specimens-cases.jsonl.gz --case 9e229e56-f7e1-58f9-984b-a9453be5dc9a
extract-gdc gdc_TARGET_file1.jsonl.gz ../../data/gdc.files-specimens-cases.jsonl.gz --file 6892c6ba-66aa-4083-a13e-845ae272700a
extract-gdc gdc_TARGET_file2.jsonl.gz ../../data/gdc.files-specimens-cases.jsonl.gz --file eb66f012-158d-409c-b79b-c4cc605eebb3

gunzip -c gdc_TARGET_case1.jsonl.gz > gdc_TARGET_case1.json
gunzip -c gdc_TARGET_case2.jsonl.gz > gdc_TARGET_case2.json
gunzip -c gdc_TARGET_file1.jsonl.gz > gdc_TARGET_file1.json
gunzip -c gdc_TARGET_file2.jsonl.gz > gdc_TARGET_file2.json

cda-transform gdc_TARGET_case1.jsonl.gz gdc.case1.H.json.gz ../../GDC_mapping.yml GDC --endpoint cases
python json2yaml.py gdc.case1.H.json.gz gdc.case1.H.yml

cda-transform gdc_TARGET_case2.jsonl.gz gdc.case2.H.json.gz ../../GDC_mapping.yml GDC --endpoint cases
python json2yaml.py gdc.case2.H.json.gz gdc.case2.H.yml

cda-transform gdc_TARGET_file1.jsonl.gz gdc.file1.H.json.gz ../../GDC_mapping.yml GDC --endpoint files
python json2yaml.py gdc.file1.H.json.gz gdc.file1.H.yml

cda-transform gdc_TARGET_file2.jsonl.gz gdc.file2.H.json.gz ../../GDC_mapping.yml GDC --endpoint files
python json2yaml.py gdc.file2.H.json.gz gdc.file2.H.yml

cat gdc_TARGET_case1.jsonl.gz gdc_TARGET_case2.jsonl.gz > gdc.cases.jsonl.gz
cat gdc_TARGET_file1.jsonl.gz gdc_TARGET_file2.jsonl.gz > gdc.files.jsonl.gz

# Aggregation is necessary from the cases endpoint. The top level Subject entity may have more than
# one ResearchSubject/case associated with it. Aggregate ResearchSubjects/cases of the same Subject
cda-transform gdc.cases.jsonl.gz gdc.cases.H.jsonl.gz ../../GDC_mapping.yml GDC --endpoint cases
cda-aggregate ../../merge.yml gdc.cases.H.jsonl.gz gdc.cases.A.jsonl.gz
python json2yaml.py gdc.cases.A.jsonl.gz gdc.cases.A.yml

cda-transform gdc.files.jsonl.gz gdc.files.H.jsonl.gz ../../GDC_mapping.yml GDC --endpoint files
# Aggregation of files transformations for GDC is unnecessary. File is top level entity and extracted that 
# way from the GDC API. Hooray!
python json2yaml.py gdc.files.H.jsonl.gz gdc.files.H.yml


# Generate PDC examples

extract-pdc pdc_QC1_case1.jsonl.gz ../../data/pdc.files-specimens-cases.json.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
extract-pdc pdc_QC1_case2.jsonl.gz ../../data/pdc.files-specimens-cases.json.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83

gunzip -c pdc_QC1_case1.jsonl.gz > pdc_QC1_case1.json
gunzip -c pdc_QC1_case2.jsonl.gz > pdc_QC1_case2.json

cda-transform pdc_QC1_case1.jsonl.gz pdc.case1.H.json.gz ../../PDC_mapping.yml PDC
python PDCH2yaml.py pdc.case1.H.json.gz pdc.case1.H.yml

cda-transform pdc_QC1_case2.jsonl.gz pdc.case2.H.json.gz ../../PDC_mapping.yml PDC
python PDCH2yaml.py pdc.case2.H.json.gz pdc.case2.H.yml

cat pdc_QC1_case1.jsonl.gz pdc_QC1_case2.jsonl.gz > pdc.jsonl.gz

cda-transform pdc.jsonl.gz pdc.H.jsonl.gz ../../PDC_mapping.yml PDC
cda-aggregate ../../merge.yml pdc.H.jsonl.gz pdc.A.jsonl.gz
python json2yaml.py pdc.A.jsonl.gz pdc.A.yml

# Merge aggregated GDC and PDC files
cda-merge ../../merge.yml gdc.pdc.M.jsonl.gz --gdc gdc.A.jsonl.gz --pdc pdc.A.jsonl.gz

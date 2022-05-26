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

cda-transform gdc_TARGET_case1.jsonl.gz gdc.case1.H.json.gz ../../GDC_subject_endpoint_mapping.yml GDC --endpoint cases
python json2yaml.py gdc.case1.H.json.gz gdc.case1.H.yml

cda-transform gdc_TARGET_case2.jsonl.gz gdc.case2.H.json.gz ../../GDC_subject_endpoint_mapping.yml GDC --endpoint cases
python json2yaml.py gdc.case2.H.json.gz gdc.case2.H.yml

cda-transform gdc_TARGET_file1.jsonl.gz gdc.file1.H.json.gz ../../GDC_file_endpoint_mapping.yml GDC --endpoint files
python json2yaml.py gdc.file1.H.json.gz gdc.file1.H.yml

cda-transform gdc_TARGET_file2.jsonl.gz gdc.file2.H.json.gz ../../GDC_file_endpoint_mapping.yml GDC --endpoint files
python json2yaml.py gdc.file2.H.json.gz gdc.file2.H.yml

cat gdc_TARGET_case1.jsonl.gz gdc_TARGET_case2.jsonl.gz > gdc.cases.jsonl.gz
cat gdc_TARGET_file1.jsonl.gz gdc_TARGET_file2.jsonl.gz > gdc.files.jsonl.gz

# Aggregation is necessary from the cases endpoint. The top level Subject entity may have more than
# one ResearchSubject/case associated with it. Aggregate ResearchSubjects/cases of the same Subject
cda-transform gdc.cases.jsonl.gz gdc.cases.H.jsonl.gz ../../GDC_subject_endpoint_mapping.yml GDC --endpoint cases
cda-aggregate ../../subject_endpoint_merge.yml gdc.cases.H.jsonl.gz gdc.cases.A.jsonl.gz
python json2yaml.py gdc.cases.A.jsonl.gz gdc.cases.A.yml

cda-transform gdc.files.jsonl.gz gdc.files.H.jsonl.gz ../../GDC_file_endpoint_mapping.yml GDC --endpoint files
# Aggregation of files transformations for GDC is unnecessary. File is top level entity and extracted that 
# way from the GDC API. Hooray!
python PDCH2yaml.py gdc.files.H.jsonl.gz gdc.files.H.yml


# Generate PDC examples

extract-pdc pdc_QC1_case1.jsonl.gz ../../data/pdc.files-specimens-cases.jsonl.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
extract-pdc pdc_QC1_case2.jsonl.gz ../../data/pdc.files-specimens-cases.jsonl.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83
extract-pdc ../../data/pdc.all_cases.jsonl.gz ../../data/pdc.files-specimens-cases.jsonl.gz --file 4bd2fb94-19ca-11e9-99db-005056921935 --files_out_file pdc_QC1_file1.jsonl.gz
extract-pdc ../../data/pdc.all_cases.jsonl.gz ../../data/pdc.files-specimens-cases.jsonl.gz --file 4d553670-1b42-11e9-b79b-005056921935 --files_out_file pdc_QC1_file2.jsonl.gz

gunzip -c pdc_QC1_case1.jsonl.gz > pdc_QC1_case1.json
gunzip -c pdc_QC1_case2.jsonl.gz > pdc_QC1_case2.json
gunzip -c pdc_QC1_file1.jsonl.gz > pdc_QC1_file1.json
gunzip -c pdc_QC1_file2.jsonl.gz > pdc_QC1_file2.json

cda-transform pdc_QC1_case1.jsonl.gz pdc.case1.H.json.gz ../../PDC_subject_endpoint_mapping.yml PDC --endpoint cases
python PDCH2yaml.py pdc.case1.H.json.gz pdc.case1.H.yml

cda-transform pdc_QC1_case2.jsonl.gz pdc.case2.H.json.gz ../../PDC_subject_endpoint_mapping.yml PDC --endpoint cases
python PDCH2yaml.py pdc.case2.H.json.gz pdc.case2.H.yml

cda-transform pdc_QC1_file1.jsonl.gz pdc.file1.H.json.gz ../../PDC_file_endpoint_mapping.yml PDC --endpoint files
python PDCH2yaml.py pdc.file1.H.json.gz pdc.file1.H.yml

cda-transform pdc_QC1_file2.jsonl.gz pdc.file2.H.json.gz ../../PDC_file_endpoint_mapping.yml PDC --endpoint files
python PDCH2yaml.py pdc.file1.H.json.gz pdc.file1.H.yml

cat pdc_QC1_case1.jsonl.gz pdc_QC1_case2.jsonl.gz > pdc.cases.jsonl.gz
cat pdc_QC1_file1.jsonl.gz pdc_QC1_file2.jsonl.gz > pdc.files.jsonl.gz

cda-transform pdc.cases.jsonl.gz pdc.cases.H.jsonl.gz ../../PDC_subject_endpoint_mapping.yml PDC --endpoint cases
cda-aggregate ../../subject_endpoint_merge.yml pdc.cases.H.jsonl.gz pdc.cases.A.jsonl.gz
python json2yaml.py pdc.cases.A.jsonl.gz pdc.cases.A.yml

cda-transform pdc.files.jsonl.gz pdc.files.H.jsonl.gz ../../PDC_file_endpoint_mapping.yml PDC --endpoint files
# Aggregation of files transformations for PDC is unnecessary. File is top level entity and extracted that 
# way from the PDC API. Hooray!
python PDCH2yaml.py pdc.files.H.jsonl.gz pdc.files.H.yml

# Generate IDC examples

extract-idc IDC_mapping.yml --dest_table_id broad-cda-dev.github_testing.idc_subject --source_table \
bigquery-public-data.idc_v9.dicom_pivot_v9 --endpoint \
Patient --out_file idc_TARGET_Subject1.jsonl.gz --make_bq_table True --make_bucket_file True \
--dest_bucket broad-cda-dev --dest_bucket_file_name public/idc_TARGET_Subject1.jsonl.gz

# Merge aggregated GDC and PDC subject
cda-merge gdc.pdc.subjects.jsonl.gz --gdc_subjects gdc.cases.A.jsonl.gz --pdc_subjects pdc.cases.A.jsonl.gz --subject_how_to_merge_file ../../subject_endpoint_merge.yml --merge_subjects True 

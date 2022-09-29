set -ex

# Extract cases/subject tests
extract-gdc gdc.jsonl.gz ../../cdatransform/extract/gdc_case_fields.txt --cases gdc-case-list.txt  
python check-gdc-pull.py gdc.jsonl.gz gdc-case-list.txt

extract-pdc pdc.jsonl.gz --cases pdc-case-list.txt
python check-pdc-pull.py pdc.jsonl.gz pdc-case-list.txt

cda-transform gdc.jsonl.gz gdc.subjects.jsonl.gz ../../GDC_subject_endpoint_mapping.yml --endpoint cases

cda-transform pdc.jsonl.gz pdc.subjects.jsonl.gz ../../PDC_subject_endpoint_mapping.yml --endpoint cases

cda-aggregate ../../subject_endpoint_merge.yml gdc.subjects.jsonl.gz gdc.subjects.merged.jsonl.gz --endpoint subjects

cda-aggregate ../../subject_endpoint_merge.yml pdc.subjects.jsonl.gz pdc.subjects.merged.jsonl.gz --endpoint subjects

cda-merge gdc.pdc.subjects.jsonl.gz --gdc_subjects gdc.subjects.merged.jsonl.gz --pdc_subjects pdc.subjects.merged.jsonl.gz --subject_how_to_merge_file ../../subject_endpoint_merge.yml --merge_subjects True

# Extract files tests
extract-gdc gdc.files.jsonl.gz ../../cdatransform/extract/gdc_file_fields.txt --files gdc-file-list.txt  
python check-gdc-pull.py gdc.files.jsonl.gz gdc-file-list.txt

extract-pdc pdc.files.jsonl.gz --files pdc-file-list.txt
python check-pdc-pull.py pdc.files.jsonl.gz pdc-file-list.txt

cda-transform gdc.files.jsonl.gz gdc.files.transformed.jsonl.gz ../../GDC_file_endpoint_mapping.yml --endpoint files

cda-transform pdc.files.jsonl.gz pdc.files.transformed.jsonl.gz ../../PDC_file_endpoint_mapping.yml --endpoint files

cda-aggregate ../../file_endpoint_merge.yml gdc.files.transformed.jsonl.gz gdc.files.merged.jsonl.gz --endpoint files

cda-aggregate ../../file_endpoint_merge.yml pdc.files.transformed.jsonl.gz pdc.files.merged.jsonl.gz --endpoint files

# No longer need to merge files with subjects info
#cda-merge ../../data/all_subjects.jsonl.gz --gdc_files gdc.files.merged.jsonl.gz --pdc_files pdc.files.merged.jsonl.gz --file_how_to_merge_file ../../file_endpoint_merge.yml --merge_files True --merged_files_file gdc.pdc.files.jsonl.gz
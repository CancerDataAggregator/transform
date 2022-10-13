transform steps

set -ex

# GDC Extraction, Transformation, and Aggregation
extract-gdc 
    gdc.all_cases.jsonl.gz 
    cdatransform/extract/gdc_case_fields.txt 
    --endpoint cases
extract-gdc 
    gdc.all_files.jsonl.gz 
    cdatransform/extract/gdc_file_fields.txt 
    --endpoint files
cda-transform 
    gdc.all_cases.jsonl.gz gdc.all_cases.H.jsonl.gz 
    GDC_subject_endpoint_mapping.yml 
    --endpoint cases
cda-transform 
    gdc.all_files.jsonl.gz gdc.all_files.H.jsonl.gz 
    GDC_file_endpoint_mapping.yml 
    --endpoint files
cda-aggregate 
    subject_endpoint_merge.yml 
    gdc.all_cases.H.jsonl.gz gdc.all_Subjects.jsonl.gz 
    --endpoint subjects
cda-aggregate 
    file_endpoint_merge.yml 
    gdc.all_files.H.jsonl.gz gdc.all__Files.jsonl.gz 
    --endpoint files

# PDC Extraction, Transformation, and Aggregation
extract-pdc 
    pdc.all_cases.jsonl.gz 
    --endpoint cases
extract-pdc 
    pdc.all_files.jsonl.gz 
    --endpoint files
cda-transform 
    pdc.all_cases.jsonl.gz 
    pdc.all_cases.H.jsonl.gz 
    PDC_subject_endpoint_mapping.yml 
    --endpoint cases
cda-transform 
    pdc.all_files.jsonl.gz 
    pdc.all_files.H.jsonl.gz 
    PDC_file_endpoint_mapping.yml 
    --endpoint files
cda-aggregate 
    subject_endpoint_merge.yml 
    pdc.all_cases.H.jsonl.gz 
    pdc.all_Subjects.jsonl.gz 
    --endpoint subjects
cda-aggregate 
    file_endpoint_merge.yml 
    pdc.all_files.H.jsonl.gz 
    pdc.all__Files.jsonl.gz 
    --endpoint files

# IDC Extraction and Transformation
extract-idc 
    IDC_mapping.yml 
    --dest_table_id gdc-bq-sample.dev.IDC_Subjects_V10 
    --source_table bigquery-public-data.idc_v10.dicom_pivot_v10 
    --gsa_key ../../GCS-service-account-key.json 
    --endpoint Patient
    --out_file idc_v10_Subjects.jsonl.gz 
    --make_bq_table True 
    --make_bucket_file True 
    --dest_bucket gdc-bq-sample-bucket 
    --dest_bucket_file_name idc_v10_Subjects*.jsonl.gz

extract-idc 
    IDC_mapping.yml 
    --dest_table_id gdc-bq-sample.dev.idc_v10_Files 
    --source_table bigquery-public-data.idc_v10.dicom_pivot_v10 
    --gsa_key ../../GCS-service-account-key.json 
    --endpoint File 
    --out_file idc_v10_Files.jsonl.gz 
    --make_bq_table True
    --make_bucket_file True 
    --dest_bucket gdc-bq-sample-bucket 
    --dest_bucket_file_name idc_v10_Files*.jsonl.gz
# Merge aggregated GDC, PDC, and IDC data
cda-merge 
    all_Subjects_3_1.jsonl.gz 
    --gdc_subjects gdc.all_Subjects.jsonl.gz 
    --pdc_subjects pdc.all_Subjects.jsonl.gz 
    --idc_subjects idc_v10_Subjects.jsonl.gz 
    --subject_how_to_merge_file subject_endpoint_merge.yml 
    --merge_subjects True

cda-merge 
    all_Files_3_1.jsonl.gz 
    --gdc_files gdc.all__Files.jsonl.gz 
    --pdc_files pdc.all__Files.jsonl.gz 
    --idc_files idc_v10_Files.jsonl.gz 
    --file_how_to_merge_file file_endpoint_merge.yml 
    --merge_files True 
    --merged_files_file all_Files_3_1.jsonl.gz

# Generate JSON schema for GDC/PDC upload
python cdatransform/load/JSON_schema.py  
    subjects_schema.json GDC_subject_endpoint_mapping.yml \
    ccdh_map.json missing_descriptions.json 
    ../cda-data-model/src/schema/json 
    Patient

python cdatransform/load/JSON_schema.py  
    files_schema.json GDC_file_endpoint_mapping.yml 
    ccdh_map.json 
    missing_descriptions.json 
    ../cda-data-model/src/schema/json 
    File

# Load GDC/PDC merged data to BQ
python cdatransform/load/load_gdc_pdc_to_bigquery.py 
    all_Subjects_3_1.jsonl.gz 
    subjects_schema.json 
    --dest_table_id gdc-bq-sample.dev.all_Subjects_v3_1_test1 
    --gsa_key ../../GCS-service-account-key.json

python cdatransform/load/load_gdc_pdc_to_bigquery.py 
    all_Files_3_1.jsonl.gz 
    files_schema.json 
    --dest_table_id gdc-bq-sample.dev.all_Files_v3_1_test1 
    --gsa_key ../../GCS-service-account-key.json
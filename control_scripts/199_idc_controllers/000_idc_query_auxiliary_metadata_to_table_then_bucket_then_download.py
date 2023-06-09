#!/usr/bin/env python3

from cda_etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

bq_project_name = 'YOUR_PROJECT_NAME'

intermediate_bucket = 'YOUR_BUCKET_NAME'

target_table_path = 'YOUR_TABLE_PATH'

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './999_create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = 'v14',
    source_table = 'auxiliary_metadata',
    dest_table = f'{bq_project_name}.{target_table_path}',
    dest_bucket = intermediate_bucket,
    dest_bucket_filename = 'auxiliary_metadata-*.jsonl.gz',
    output_filename = 'auxiliary_metadata.jsonl.gz'
)

fields_to_pull = [
    
    'idc_case_id',
    'submitter_case_id'
]

idc.query_idc_to_table( fields_to_pull )

idc.extract_table_to_bucket()

idc.download_bucket_data()


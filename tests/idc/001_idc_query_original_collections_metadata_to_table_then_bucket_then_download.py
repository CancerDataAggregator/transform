#!/usr/bin/env python3

from etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './tests/idc/create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = 'v13',
    source_table = 'original_collections_metadata',
    dest_table = 'broad-cda-dev.github_testing.idc_patient_testing',
    dest_bucket = 'gdc-bq-sample-bucket',
    dest_bucket_filename = 'original_collections_metadata-*.jsonl.gz',
    output_filename = 'original_collections_metadata.jsonl.gz'
)

fields_to_pull = [
    
    'idc_webapp_collection_id',
    'CancerType'
]

idc.query_idc_to_table( fields_to_pull )

idc.extract_table_to_bucket()

idc.download_bucket_data()


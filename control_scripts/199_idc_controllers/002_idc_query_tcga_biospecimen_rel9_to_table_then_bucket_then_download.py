#!/usr/bin/env python3 -u

import sys

from cda_etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

version_string = sys.argv[1]

bq_project_name = 'broad-cda-dev'

intermediate_bucket = 'gdc-bq-sample-bucket'

target_table_path = 'github_testing.idc_patient_testing'

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './999_create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = version_string,
    source_table = 'tcga_biospecimen_rel9',
    dest_table = f'{bq_project_name}.{target_table_path}',
    dest_bucket = intermediate_bucket,
    dest_bucket_filename = 'tcga_biospecimen_rel9-*.jsonl.gz',
    output_filename = 'tcga_biospecimen_rel9.jsonl.gz'
)

fields_to_pull = [
    
    'sample_barcode',
    'case_barcode',
    'days_to_collection',
    'sample_type_name',
    'case_gdc_id',
    'sample_gdc_id'
]

idc.query_idc_to_table( fields_to_pull )

idc.extract_table_to_bucket()

idc.download_bucket_data()


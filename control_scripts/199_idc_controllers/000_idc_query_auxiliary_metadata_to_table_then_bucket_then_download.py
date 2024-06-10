#!/usr/bin/env python3 -u

import sys

from cda_etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

version_string = sys.argv[1]

# Note: BigQuery returns rows in random order. Even if the underlying data hasn't
# changed, successive pulls will not generally produce precisely the same files.
# 
# Files from multiple pulls of the same data should be identical to one another
# after their rows have been sorted, though, and byte counts should match (once
# expanded -- zipped byte counts will still differ because each file's exact
# compression ratio depends on the order in which data is scanned during
# zip-style compression, and row order differs).

bq_project_name = 'broad-cda-dev'

intermediate_bucket = 'gdc-bq-sample-bucket'

target_table_path = 'github_testing.idc_patient_testing'

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './999_create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = version_string,
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


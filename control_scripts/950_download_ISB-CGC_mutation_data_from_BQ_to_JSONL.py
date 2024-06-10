#!/usr/bin/env python3

import sys

from cda_etl.extract.mutation.mutation_extractor import mutation_extractor

from os import path, makedirs, system

# A version string (e.g. 'hg38_gdc_current') is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if len( sys.argv ) <= 1:
    
    sys.exit( 'FATAL: This script needs an ISB source-version string (like "hg38_gdc_current") as its argument.' )

source_version = sys.argv[1]

source_project = 'isb-cgc-bq'

processing_bucket = 'gdc-bq-sample-bucket'

key_file = 'GCS-service-account-key.etl-github-testing.json'

key_generator = './999_create_service_account_key.etl-github-testing.bash'

if not path.isfile( key_file ):
    
    system( key_generator )

datasets = [
    
    'BEATAML1_0',
    'CDDP_EAGLE',
    'CGCI',
    'CMI',
    'CPTAC',
    'EXC_RESPONDERS',
    'HCMI',
    'MMRF',
    'TARGET',
    'TCGA'
]

for dataset in datasets:
    
    mtx = mutation_extractor(
        
        gsa_key = key_file,
        source_project = source_project,
        source_dataset = dataset,
        source_version = source_version,
        processing_bucket = processing_bucket
    )

    mtx.extract_table_to_bucket()

    mtx.download_bucket_data()



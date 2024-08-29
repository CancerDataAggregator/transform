#!/usr/bin/env python3 -u

import sys

from os import path, system

from cda_etl.extract.idc.idc_extractor import IDC_extractor
from cda_etl.lib import get_idc_extraction_fields

# ARGUMENT

# An IDC version string (e.g. 'v18') is required.
# 
# If the length of the argument list to this script is zero, fail.

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC version string, e.g. v18>\n" )

version_string = sys.argv[1]

# Note: BigQuery returns rows in random order. Even if the underlying data hasn't
# changed, successive pulls will not generally produce precisely the same files.
# 
# Files from multiple pulls of the same data should be identical to one another
# after their rows have been sorted, though, and byte counts should match (once
# expanded -- zipped byte counts will still differ because each file's exact
# compression ratio depends on the order in which data is scanned during
# zip-style compression, and row order differs).

bq_project_name = YOUR_PROJECT_NAME

intermediate_bucket = YOUR_TEMP_BUCKET

target_table_path = YOUR_TARGET_TABLE

service_account_key_file = 'GCS-service-account-key.json'

if not path.isfile( service_account_key_file ):
    
    system( './999_create_service_account_key.bash' )

target_field_lists = get_idc_extraction_fields()

for field_dict in target_field_lists:
    
    # To be clear(ish), there's only ever one table name per dict.

    for source_table_name in field_dict:
        
        print( '==================================================================================================', file=sys.stderr )

        print( f"Extracting {source_table_name} from IDC {version_string}...", file=sys.stderr )

        extractor = IDC_extractor(
            
            gsa_key = service_account_key_file,
            source_version = version_string,
            source_table = source_table_name,
            dest_table = target_table_path,
            dest_bucket = intermediate_bucket,
            dest_bucket_filename = f"{source_table_name}-*.jsonl.gz",
            output_filename = f"{source_table_name}.jsonl.gz"
        )

        extractor.query_idc_to_table( fields_to_pull=field_dict[source_table_name] )

        extractor.extract_table_to_bucket()

        extractor.download_bucket_data()



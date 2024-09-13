#!/usr/bin/env python3 -u

import gzip
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import get_column_metadata, get_current_timestamp

# ARGUMENT

if len( sys.argv ) != 6:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <data source label, e.g. 'GDC'> <data source version string, e.g. 'v40.0'> <extraction date as YYYY-MM-DD> <count summary file for harmonized data> <decorated, harmonized output directory for release_metadata.tsv>\n" )

data_source_label = sys.argv[1].upper()

data_source_version_string = sys.argv[2]

data_source_extraction_date = sys.argv[3]

count_summary_file = sys.argv[4]

decorated_harmonized_tsv_dir = sys.argv[5]

release_metadata_output_tsv = path.join( decorated_harmonized_tsv_dir, 'release_metadata.tsv' )

# PARAMETERS

release_metadata_fields = [
    
    'cda_table',
    'cda_column',
    'data_source',
    'data_source_version',
    'data_source_extraction_date',
    'data_source_row_count',
    'data_source_unique_value_count',
    'data_source_null_count'
]

# EXECUTION

for output_dir in [ decorated_harmonized_tsv_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"   [{get_current_timestamp()}] Creating release_metadata.tsv...", end='', file=sys.stderr )

with open( count_summary_file ) as IN, open( release_metadata_output_tsv, 'w' ) as OUT:
    
    print( *release_metadata_fields, sep='\t', file=OUT )

    summary_header = next( IN )

    for next_line in [ line.rstrip( '\n' ) for line in IN ]:
        
        [ table_filename, column_name, distinct_non_null_values, total_data_rows, has_nulls, number_of_rows_with_nulls, numeric_column ] = next_line.split( '\t' )

        table_name = re.sub( r'\.tsv(?:\.gz)?$', r'', table_filename )

        column_result = get_column_metadata( table_name, column_name )

        if ( table_name == 'dicom_series_type' and column_name == 'type' ) or \
            ( 'column_type' in column_result and column_result['column_type'] == 'categorical' and 'fetch_rows_returns' in column_result and column_result['fetch_rows_returns'] == True ):
            
            print( *[ table_name, column_name, data_source_label, data_source_version_string, data_source_extraction_date, total_data_rows, distinct_non_null_values, number_of_rows_with_nulls ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import get_column_metadata

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <desired output directory>\n" )

output_dir = sys.argv[1]

output_file = path.join( output_dir, 'column_metadata.tsv' )

column_metadata_fields = [
    
    'cda_table',
    'cda_column',
    'column_type',
    'summary_display',
    'fetch_rows_returns',
    'process_before_display',
    'virtual_table'
]

# EXECUTION

if not path.isdir( output_dir ):
    
    makedirs( output_dir )

column_metadata = get_column_metadata()

with open( output_file, 'w' ) as OUT:
    
    print( *column_metadata_fields, sep='\t', file=OUT )

    for table_name in sorted( column_metadata ):
        
        for column_name in sorted( column_metadata[table_name] ):
            
            current_record = column_metadata[table_name][column_name]

            process_before_display = ''

            if 'process_before_display' in current_record:
                
                process_before_display = current_record['process_before_display']

            virtual_table = ''

            if 'virtual_table' in current_record:
                
                virtual_table = current_record['virtual_table']

            print( *[ table_name, column_name, current_record['column_type'], current_record['summary_display'], current_record['fetch_rows_returns'], process_before_display, virtual_table ], sep='\t', file=OUT )



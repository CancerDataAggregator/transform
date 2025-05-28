#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import get_column_metadata
from cda_etl.load.cda_loader import CDA_loader

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

tsv_dir = sys.argv[1]

if not path.isdir( tsv_dir ):
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

output_file = path.join( tsv_dir, 'column_metadata.tsv' )

column_metadata_fields = [
    
    'cda_table',
    'cda_column',
    'column_type',
    'summary_returns',
    'data_returns',
    'process_before_display',
    'virtual_table'
]

# EXECUTION

# Make column_metadata.tsv.

column_metadata = get_column_metadata()

with open( output_file, 'w' ) as OUT:
    
    print( *column_metadata_fields, sep='\t', file=OUT )

    for table_name in sorted( column_metadata ):
        
        # Python 3 preserves insert order for dicts. That means column data will be displayed
        # in the order in which columns are listed in the definition (in lib.py) of
        # get_column_metadata(). Handy. Also worth noting because it's not obvious.

        for column_name in column_metadata[table_name]:
            
            current_record = column_metadata[table_name][column_name]

            process_before_display = ''

            if 'process_before_display' in current_record:
                
                process_before_display = current_record['process_before_display']

            virtual_table = ''

            if 'virtual_table' in current_record:
                
                virtual_table = current_record['virtual_table']

            print( *[ table_name, column_name, current_record['column_type'], current_record['summary_returns'], current_record['data_returns'], process_before_display, virtual_table ], sep='\t', file=OUT )

# Transform all input TSVs to SQL and make the pre- and postprocessing scripts to manage index destruction/reconstruction.

loader = CDA_loader()

loader.transform_dir_to_SQL( tsv_dir )



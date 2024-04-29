#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import columns_to_count

dir_to_scan = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

output_dir = path.join( 'auxiliary_metadata', '__column_stats', '__full_count_data', 'GDC' )

if not path.exists( output_dir ):
    
    makedirs( output_dir )

target_columns = columns_to_count( 'gdc' )

for table in sorted( target_columns ):
    
    input_file = path.join( dir_to_scan, f"{table}.tsv" )

    target_counts = dict()

    with open( input_file, 'r' ) as IN:
        
        column_names = next( IN ).rstrip( '\n' ).split( '\t' )

        for column_name in target_columns[table]:
            
            target_counts[column_name] = dict()

        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
            
            record = dict( zip( column_names, line.split( '\t' ) ) )

            for target_column in sorted( target_columns[table] ):
                
                value = record[target_column]

                if value is None:
                    
                    value = ''

                if value in target_counts[target_column]:
                    
                    target_counts[target_column][value] = target_counts[target_column][value] + 1

                else:
                    
                    target_counts[target_column][value] = 1

    for column_name in sorted( target_counts ):
        
        print( f"{table}.{column_name}", file=sys.stderr )

        output_file = path.join( output_dir, f"{table}.{column_name}.tsv" )

        with open( output_file, 'w' ) as OUT:
            
            print( *[ 'count', 'value' ], sep='\t', end='\n', file=OUT )

            # Sort value:count pairs descending by count.

            for value_tuple in sorted( target_counts[column_name].items(), key=lambda x:x[1], reverse=True ):
                
                print( *[ value_tuple[1], value_tuple[0] ], sep='\t', end='\n', file=OUT )



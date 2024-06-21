#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import columns_to_count

dir_to_scan = path.join( 'extracted_data', 'cds' )

output_dir = path.join( 'auxiliary_metadata', '__column_stats', '__full_count_data', 'CDS' )

skip_file = path.join( output_dir, 'zzz_SKIPPED_COLUMNS.tsv' )

if not path.exists( output_dir ):
    
    makedirs( output_dir )

target_columns = columns_to_count( 'cds' )

with open( skip_file, 'w' ) as SKIP:
    
    print( *[ 'table', 'skipped_column_name' ], sep='\t', file=SKIP )

    for table in sorted( target_columns ):
        
        input_file = path.join( dir_to_scan, f"{table}.tsv" )

        target_counts = dict()

        print_table = re.sub( r'^[^\/]+\/', r'', table )

        for column_name in target_columns[table]:
            
            target_counts[column_name] = dict()

        if path.exists( input_file ):
            
            with open( input_file, 'r' ) as IN:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                for column_name in column_names:
                    
                    if column_name not in target_columns[table]:
                        
                        print( *[ print_table, column_name ], sep='\t', file=SKIP )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( column_names, line.split( '\t' ) ) )

                    for target_column in sorted( target_columns[table] ):
                        
                        value = ''

                        if target_column in record and record[target_column] is not None:
                            
                            value = record[target_column]

                        if value in target_counts[target_column]:
                            
                            target_counts[target_column][value] = target_counts[target_column][value] + 1

                        else:
                            
                            target_counts[target_column][value] = 1

        for column_name in sorted( target_counts ):
            
            print( f"{print_table}.{column_name}", file=sys.stderr )

            output_file = path.join( output_dir, f"{print_table}.{column_name}.tsv" )

            with open( output_file, 'w' ) as OUT:
                
                print( *[ 'count', 'value' ], sep='\t', end='\n', file=OUT )

                # Sort value:count pairs descending by count.

                for value_tuple in sorted( target_counts[column_name].items(), key=lambda x:x[1], reverse=True ):
                    
                    print( *[ value_tuple[1], value_tuple[0] ], sep='\t', end='\n', file=OUT )



#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import columns_to_count, get_current_timestamp

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <data source, e.g. 'GDC'> <root directory to scan for TSV data> <desired output directory>\n" )

data_source = sys.argv[1].lower()

input_root = sys.argv[2]

output_dir = sys.argv[3]

# PARAMETERS

skip_file = path.join( output_dir, 'zzz_SKIPPED_COLUMNS.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

target_columns = columns_to_count( data_source )

with open( skip_file, 'w' ) as SKIP:
    
    print( *[ 'table', 'skipped_column_name' ], sep='\t', file=SKIP )

    for table_path in sorted( target_columns ):
        
        input_file = path.join( input_root, f"{table_path}.tsv" )

        target_counts = dict()

        table = re.sub( r'^[^\/]+\/', r'', table_path )

        if path.exists( input_file ):
            
            with open( input_file, 'r' ) as IN:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                for column_name in column_names:
                    
                    if column_name not in target_columns[table_path]:
                        
                        print( *[ table, column_name ], sep='\t', file=SKIP )

                for column_name in target_columns[table_path]:
                    
                    target_counts[column_name] = dict()

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( column_names, line.split( '\t' ) ) )

                    for target_column in sorted( target_columns[table_path] ):
                        
                        value = ''

                        if target_column in record and record[target_column] is not None:
                            
                            value = record[target_column]

                        if value in target_counts[target_column]:
                            
                            target_counts[target_column][value] = target_counts[target_column][value] + 1

                        else:
                            
                            target_counts[target_column][value] = 1

            for column_name in sorted( target_counts ):
                
                print( f"   [{get_current_timestamp()}] Writing {table}.{column_name}...", end='', file=sys.stderr )

                output_file = path.join( output_dir, f"{table}.{column_name}.tsv" )

                with open( output_file, 'w' ) as OUT:
                    
                    print( *[ 'count', 'value' ], sep='\t', end='\n', file=OUT )

                    # Sort value:count pairs descending by count.

                    for value_tuple in sorted( target_counts[column_name].items(), key=lambda x:x[1], reverse=True ):
                        
                        print( *[ value_tuple[1], value_tuple[0] ], sep='\t', end='\n', file=OUT )

                print( 'done.', file=sys.stderr )



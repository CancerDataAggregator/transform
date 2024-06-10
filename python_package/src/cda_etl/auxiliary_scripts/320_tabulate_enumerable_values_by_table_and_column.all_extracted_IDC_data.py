#!/usr/bin/env python3 -u

import gzip, jsonlines, re, sys

from os import makedirs, path

from cda_etl.lib import columns_to_count

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <IDC version string, e.g. v18>\n" )

version_string = sys.argv[1]

dir_to_scan = path.join( 'extracted_data', 'idc', version_string )

output_dir = path.join( 'auxiliary_metadata', '__column_stats', '__full_count_data', 'IDC.select_extracted_fields' )

skip_file = path.join( output_dir, 'zzz_SKIPPED_COLUMNS.tsv' )

if not path.exists( output_dir ):
    
    makedirs( output_dir )

target_columns = columns_to_count( 'idc' )

with open( skip_file, 'w' ) as SKIP:
    
    print( *[ 'table', 'skipped_column_name' ], sep='\t', file=SKIP )

    for table in sorted( target_columns ):
        
        input_file = path.join( dir_to_scan, f"{table}.jsonl.gz" )

        target_counts = dict()

        print_table = re.sub( r'^[^\/]+\/', r'', table )

        skipped_columns = list()

        for column_name in target_columns[table]:
            
            target_counts[column_name] = dict()

        print( f"Tabulating enumerable values from {print_table}...", end='\n\n', file=sys.stderr )

        with gzip.open( input_file ) as IN:
            
            reader = jsonlines.Reader( IN )

            for record in reader:
                
                for column_name in record:
                    
                    if column_name not in target_columns[table]:
                        
                        if column_name not in skipped_columns:
                            
                            skipped_columns.append( column_name )

                for target_column in sorted( target_columns[table] ):
                    
                    value = None

                    if target_column in record:
                        
                        value = record[target_column]

                    if value is None:
                        
                        value = ''

                    if value in target_counts[target_column]:
                        
                        target_counts[target_column][value] = target_counts[target_column][value] + 1

                    else:
                        
                        target_counts[target_column][value] = 1

        for column_name in skipped_columns:
            
            print( *[ print_table, column_name ], sep='\t', file=SKIP )

        for column_name in sorted( target_counts ):
            
            print( f"   ...recording {print_table}.{column_name} counts...", file=sys.stderr )

            output_file = path.join( output_dir, f"{print_table}.{column_name}.tsv" )

            with open( output_file, 'w' ) as OUT:
                
                print( *[ 'count', 'value' ], sep='\t', end='\n', file=OUT )

                # Sort value:count pairs descending by count.

                for value_tuple in sorted( target_counts[column_name].items(), key=lambda x:x[1], reverse=True ):
                    
                    print( *[ value_tuple[1], value_tuple[0] ], sep='\t', end='\n', file=OUT )

        print( f"\n...done tabulating {print_table}.", file=sys.stderr )



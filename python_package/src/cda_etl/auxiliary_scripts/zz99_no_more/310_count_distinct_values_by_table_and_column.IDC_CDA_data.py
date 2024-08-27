#!/usr/bin/env python -u

import gzip
import re
import sys

from os import listdir, makedirs, path

# PARAMETERS

version_string = sys.argv[1]

input_root = path.join( 'cda_tsvs', 'idc', version_string )

aux_root = 'auxiliary_metadata'

stats_dir = path.join( aux_root, '__column_stats' )

stats_file = path.join( stats_dir, 'IDC_column_stats_by_table.converted_to_CDA.tsv' )

distinct_value_count = dict()

# Exclude some unique-per-file data from analysis because it's expensive to collect and essentially uninformative when reported.

excluded_file_columns = [
    
    'id',
    'label',
    'drs_uri',
    'byte_size',
    'checksum',
    'imaging_series'
]

display_increment = 2000000

# EXECUTION

for output_dir in [ stats_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

with open( stats_file, 'w' ) as STATS_FILE:
    
    print( *['table_name', 'column_name', 'distinct_non_null_values', 'total_data_rows', 'has_nulls', 'number_of_rows_with_nulls', 'numeric_column' ], sep='\t', end='\n', file=STATS_FILE )

    distinct_value_count = dict()

    for file_basename in sorted( listdir( input_root ) ):
        
        input_file_path = path.join( input_root, file_basename )

        file_label = re.sub( r'\.gz$', r'', file_basename )

        if re.search( r'\.tsv$', file_label ) is not None and ( re.search( r'_', file_label ) is None or re.search( '_associated_project\.tsv$', file_label ) is not None ):
            
            distinct_value_count[file_label] = dict()

            print( file_label )

            with gzip.open( input_file_path, 'rt' ) as IN:
                
                column_names = next(IN).rstrip('\n').split('\t')

                column_has_nulls = dict( zip( column_names, [ False ] * len( column_names ) ) )

                column_all_numbers = dict( zip( column_names, [ True ] * len( column_names ) ) )

                column_null_count = dict( zip( column_names, [ 0 ] * len( column_names ) ) )

                column_distinct_values = dict( zip( column_names, [ set() for column_name in column_names ] ) )

                row_count = 0

                # The first column is always an ID column. It is both pointless and prohibitively expensive to count it. Don't.

                id_column_name = column_names[0]

                column_has_nulls[id_column_name] = 'NA'
                column_all_numbers[id_column_name] = 'NA'
                column_null_count[id_column_name] = 'NA'

                if file_label == 'file.tsv':
                    
                    # Exclude some unique-per-file data also, because it's expensive to collect and essentially uninformative when reported.

                    for column_name in excluded_file_columns:
                        
                        column_has_nulls[column_name] = 'NA'
                        column_all_numbers[column_name] = 'NA'
                        column_null_count[column_name] = 'NA'

                for line in IN:
                    
                    line = line.rstrip('\n')
                    
                    row_count = row_count + 1

                    if row_count % display_increment == 0:
                        
                        print( f"   ...scanned {row_count} rows...", file=sys.stderr )

                    values = line.split('\t')

                    # Skip values[0]: it's an ID and we're not counting statistics for those.

                    for i in range( 1, len( values ) ):
                        
                        # Exclude some unique-per-file data because it's expensive to collect and essentially uninformative when reported.

                        if file_label != 'file.tsv' or column_names[i] not in excluded_file_columns:
                            
                            if values[i] == '':
                                
                                column_has_nulls[column_names[i]] = True

                                column_null_count[column_names[i]] = column_null_count[column_names[i]] + 1

                            else:
                                
                                column_distinct_values[column_names[i]].add(values[i])

                                if re.search( r'^-?\d+(\.\d+)?([eE]-?\d+)?$', values[i] ) is None:
                                    
                                    column_all_numbers[column_names[i]] = False

                for column_name in column_names:
                    
                    has_nulls = str( column_has_nulls[column_name] )

                    is_all_numbers = str( column_all_numbers[column_name] )

                    if row_count == 0:
                        
                        has_nulls = 'NA'
                        is_all_numbers = 'NA'

                    if column_null_count[column_name] == row_count:
                        
                        is_all_numbers = 'NA'

                    distinct_values = len( column_distinct_values[column_name] )

                    if column_null_count[column_name] == 'NA':
                        
                        # We disabled counting for this column. Distinguish between '0 values observed' and 'NA == did not count'.

                        distinct_values = 'NA'

                    print( *[ f"{file_label}", column_name, distinct_values, row_count, has_nulls, column_null_count[column_name], is_all_numbers ], sep='\t', end='\n', file=STATS_FILE )



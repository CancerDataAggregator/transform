#!/usr/bin/env python -u

import re

from os import listdir, makedirs, path

# PARAMETERS

input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

aux_root = 'auxiliary_metadata'

stats_dir = path.join( aux_root, '__column_stats' )

stats_file = path.join( stats_dir, 'PDC_column_stats_by_table.raw.tsv' )

distinct_value_count = dict()

# EXECUTION

for output_dir in [ stats_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

with open( stats_file, 'w' ) as STATS_FILE:
    
    print( *['table_name', 'column_name', 'distinct_non_null_values', 'total_data_rows', 'has_nulls', 'number_of_rows_with_nulls', 'numeric_column' ], sep='\t', end='\n', file=STATS_FILE )

    distinct_value_count = dict()

    for subdir in sorted( listdir( input_root ) ):

        if re.search( r'^[A-Z]', subdir ) is not None:
            
            for file_basename in sorted( listdir( path.join( input_root, subdir ) ) ):
                
                input_file_path = path.join( input_root, subdir, file_basename )

                file_label = f"{subdir}/{file_basename}"

                if re.search( r'\.tsv$', file_basename ) is not None:
                    
                    distinct_value_count[file_label] = dict()

                    print( file_label )

                    with open( input_file_path ) as IN:
                        
                        column_names = next(IN).rstrip('\n').split('\t')

                        column_has_nulls = dict( zip( column_names, [ False ] * len( column_names ) ) )

                        column_all_numbers = dict( zip( column_names, [ True ] * len( column_names ) ) )

                        column_null_count = dict( zip( column_names, [ 0 ] * len( column_names ) ) )

                        column_distinct_values = dict( zip( column_names, [ set() for column_name in column_names ] ) )

                        row_count = 0

                        for line in [ next_line.rstrip('\n') for next_line in IN ]:
                            
                            row_count = row_count + 1

                            values = line.split('\t')

                            for i in range( 0, len( values ) ):
                                
                                if values[i] == '':
                                    
                                    column_has_nulls[column_names[i]] = True

                                    column_null_count[column_names[i]] = column_null_count[column_names[i]] + 1

                                else:
                                    
                                    column_distinct_values[column_names[i]].add(values[i])

                                    if re.search( r'^-?\d+(\.\d+)?$', values[i] ) is None:
                                        
                                        column_all_numbers[column_names[i]] = False

                        for column_name in column_names:
                            
                            has_nulls = str( column_has_nulls[column_name] )

                            is_all_numbers = str( column_all_numbers[column_name] )

                            if row_count == 0:
                                
                                has_nulls = 'NA'
                                is_all_numbers = 'NA'

                            if column_null_count[column_name] == row_count:
                                
                                is_all_numbers = 'NA'

                            print( *[ f"{file_label}", column_name, str( len( column_distinct_values[column_name] ) ), str( row_count ), has_nulls, column_null_count[column_name], is_all_numbers ], sep='\t', end='\n', file=STATS_FILE )



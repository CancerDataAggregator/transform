#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

# SUBROUTINES

def map_columns_one_to_one( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            return_map[current_from] = current_to

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add(current_to)

    return return_map

# PARAMETERS

input_root = 'merged_data/pdc'

aliquot_id_to_case_id_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.case_id.tsv' )

case_id_to_sample_id_input_tsv = path.join( input_root, 'Case', 'Case.sample_id.tsv' )

sample_id_to_aliquot_id_input_tsv = path.join( input_root, 'Sample', 'Sample.aliquot_id.tsv' )

output_file_one = 'Aliquot.case_id.from_FileMetadata.tsv'

output_file_two = 'Aliquot.case_id.from_Case_and_Sample.tsv'

# EXECUTION

shutil.copy2( aliquot_id_to_case_id_input_tsv, output_file_one )

# Load the map from case_id to sample_id.

case_id_to_sample_id = map_columns_one_to_many( case_id_to_sample_id_input_tsv, 'case_id', 'sample_id' )

# Load the map from sample_id to aliquot_id.

sample_id_to_aliquot_id = map_columns_one_to_many( sample_id_to_aliquot_id_input_tsv, 'sample_id', 'aliquot_id' )

# Reconstruct the Aliquot->case_id relationship from the Case- and Sample-loaded data.

aliquot_id_to_case_id = dict()

for case_id in case_id_to_sample_id:
    
    for sample_id in case_id_to_sample_id[case_id]:
        
        if sample_id not in sample_id_to_aliquot_id:
            
            sys.exit(f"Unexpected failure? {sample_id}\n")

        for aliquot_id in sample_id_to_aliquot_id[sample_id]:
            
            if aliquot_id not in aliquot_id_to_case_id:
                
                aliquot_id_to_case_id[aliquot_id] = set()

            aliquot_id_to_case_id[aliquot_id].add(case_id)

# Write the second output file from the reconstructed Aliquot->case_id relationship.

with open( output_file_two, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'aliquot_id', 'case_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for aliquot_id in sorted( aliquot_id_to_case_id ):
        
        for case_id in sorted( aliquot_id_to_case_id[aliquot_id] ):
            
            print( *[ aliquot_id, case_id ], sep='\t', end='\n', file=OUT )



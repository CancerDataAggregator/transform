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

input_dir = path.join( input_root, 'Case' )

case_id_to_sample_id_input_tsv = path.join( input_dir, 'Case.sample_id.tsv' )

case_id_to_study_id_input_tsv = path.join( input_dir, 'Case.study_id.tsv' )

sample_id_to_study_id_input_tsv = path.join( input_root, 'Sample', 'Sample.study_id.tsv' )

output_file_one = 'Sample.study_id.fromBiospecimen.tsv'

output_file_two = 'Sample.study_id.fromCase.tsv'

# EXECUTION

shutil.copy2( sample_id_to_study_id_input_tsv, output_file_one )

# Load the map from case_id to sample_id.

case_id_to_sample_id = map_columns_one_to_many( case_id_to_sample_id_input_tsv, 'case_id', 'sample_id' )

# Load the map from case_id to study_id.

case_id_to_study_id = map_columns_one_to_many( case_id_to_study_id_input_tsv, 'case_id', 'study_id' )

# Reconstruct the Sample->study_id relationship from the Case-loaded data.

sample_id_to_study_id = dict()

for case_id in case_id_to_sample_id:
    
    if case_id not in case_id_to_study_id:
        
        sys.exit( f"WTF? {case_id}\n" )

    for sample_id in case_id_to_sample_id[case_id]:
        
        for study_id in case_id_to_study_id[case_id]:
            
            if sample_id not in sample_id_to_study_id:
                
                sample_id_to_study_id[sample_id] = set()

            sample_id_to_study_id[sample_id].add(study_id)

# Write the second output file from the reconstructed Sample->study_id relationship.

with open( output_file_two, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'sample_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for sample_id in sorted( sample_id_to_study_id ):
        
        for study_id in sorted( sample_id_to_study_id[sample_id] ):
            
            print( *[ sample_id, study_id ], sep='\t', end='\n', file=OUT )



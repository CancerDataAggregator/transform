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

###

diagnosis_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Diagnosis', 'Diagnosis.tsv' ), 'diagnosis_id', 'diagnosis_submitter_id' )

demographic_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Demographic', 'Demographic.tsv' ), 'demographic_id', 'demographic_submitter_id' )

###

diagnosis_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.diagnosis_id.tsv' ), 'diagnosis_id', 'case_id' )

demographic_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.demographic_id.tsv' ), 'demographic_id', 'case_id' )

case_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Case', 'Case.study_id.tsv' ), 'case_id', 'study_id' )

###

diagnosis_study_output_tsv = path.join( input_root, 'Diagnosis', 'Diagnosis.study_id.tsv' )

with open( diagnosis_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'diagnosis_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_submitter_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ diagnosis_id, study_id ], sep='\t', end='\n', file=OUT )

demographic_study_output_tsv = path.join( input_root, 'Demographic', 'Demographic.study_id.tsv' )

with open( demographic_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'demographic_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for demographic_id in sorted( demographic_id_to_submitter_id ):
        
        case_id = demographic_id_to_case_id[demographic_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ demographic_id, study_id ], sep='\t', end='\n', file=OUT )



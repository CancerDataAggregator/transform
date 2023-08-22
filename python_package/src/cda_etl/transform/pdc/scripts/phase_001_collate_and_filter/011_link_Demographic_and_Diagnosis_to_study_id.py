#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

# PARAMETERS

input_root = 'extracted_data/pdc_postprocessed'

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

sort_file_with_header( diagnosis_study_output_tsv )

demographic_study_output_tsv = path.join( input_root, 'Demographic', 'Demographic.study_id.tsv' )

with open( demographic_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'demographic_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for demographic_id in sorted( demographic_id_to_submitter_id ):
        
        case_id = demographic_id_to_case_id[demographic_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ demographic_id, study_id ], sep='\t', end='\n', file=OUT )

sort_file_with_header( demographic_study_output_tsv )



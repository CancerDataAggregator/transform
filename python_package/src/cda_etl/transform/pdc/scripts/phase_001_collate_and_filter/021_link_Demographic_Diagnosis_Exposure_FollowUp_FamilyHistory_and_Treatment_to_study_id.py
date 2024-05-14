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

exposure_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Exposure', 'Exposure.tsv' ), 'exposure_id', 'exposure_submitter_id' )

family_history_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'FamilyHistory', 'FamilyHistory.tsv' ), 'family_history_id', 'family_history_submitter_id' )

follow_up_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'FollowUp', 'FollowUp.tsv' ), 'follow_up_id', 'follow_up_submitter_id' )

treatment_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Treatment', 'Treatment.tsv' ), 'treatment_id', 'treatment_submitter_id' )

###

diagnosis_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.diagnosis_id.tsv' ), 'diagnosis_id', 'case_id' )

demographic_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.demographic_id.tsv' ), 'demographic_id', 'case_id' )

exposure_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.exposure_id.tsv' ), 'exposure_id', 'case_id' )

family_history_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.family_history_id.tsv' ), 'family_history_id', 'case_id' )

follow_up_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.follow_up_id.tsv' ), 'follow_up_id', 'case_id' )

treatment_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.treatment_id.tsv' ), 'treatment_id', 'case_id' )

###

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

exposure_study_output_tsv = path.join( input_root, 'Exposure', 'Exposure.study_id.tsv' )

with open( exposure_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'exposure_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for exposure_id in sorted( exposure_id_to_submitter_id ):
        
        case_id = exposure_id_to_case_id[exposure_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ exposure_id, study_id ], sep='\t', end='\n', file=OUT )

sort_file_with_header( exposure_study_output_tsv )

family_history_study_output_tsv = path.join( input_root, 'FamilyHistory', 'FamilyHistory.study_id.tsv' )

with open( family_history_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'family_history_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for family_history_id in sorted( family_history_id_to_submitter_id ):
        
        case_id = family_history_id_to_case_id[family_history_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ family_history_id, study_id ], sep='\t', end='\n', file=OUT )

sort_file_with_header( family_history_study_output_tsv )

follow_up_study_output_tsv = path.join( input_root, 'FollowUp', 'FollowUp.study_id.tsv' )

with open( follow_up_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'follow_up_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for follow_up_id in sorted( follow_up_id_to_submitter_id ):
        
        case_id = follow_up_id_to_case_id[follow_up_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ follow_up_id, study_id ], sep='\t', end='\n', file=OUT )

sort_file_with_header( follow_up_study_output_tsv )

treatment_study_output_tsv = path.join( input_root, 'Treatment', 'Treatment.study_id.tsv' )

with open( treatment_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'treatment_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for treatment_id in sorted( treatment_id_to_submitter_id ):
        
        case_id = treatment_id_to_case_id[treatment_id]

        if case_id in case_id_to_study_id:
            
            for study_id in case_id_to_study_id[case_id]:
                
                print( *[ treatment_id, study_id ], sep='\t', end='\n', file=OUT )

sort_file_with_header( treatment_study_output_tsv )



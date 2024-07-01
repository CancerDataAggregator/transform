#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

aux_root = 'auxiliary_metadata'

output_dir = path.join( aux_root, '__PDC_supplemental_metadata' )

out_file = path.join( output_dir, 'PDC_entity_submitter_id_to_program_project_and_study.tsv' )

###

aliquot_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Aliquot', 'Aliquot.tsv' ), 'aliquot_id', 'aliquot_submitter_id' )

sample_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Sample', 'Sample.tsv' ), 'sample_id', 'sample_submitter_id' )

diagnosis_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Diagnosis', 'Diagnosis.tsv' ), 'diagnosis_id', 'diagnosis_submitter_id' )

demographic_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Demographic', 'Demographic.tsv' ), 'demographic_id', 'demographic_submitter_id' )

case_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.tsv' ), 'case_id', 'case_submitter_id' )

study_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Study', 'Study.tsv' ), 'study_id', 'study_submitter_id' )

study_id_to_pdc_id = map_columns_one_to_one( path.join( input_root, 'Study', 'Study.tsv' ), 'study_id', 'pdc_study_id' )

project_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'Project', 'Project.tsv' ), 'project_id', 'project_submitter_id' )

project_id_to_program_id = map_columns_one_to_one( path.join( input_root, 'Program', 'Program.project_id.tsv' ), 'project_id', 'program_id' )

program_id_to_program_name = map_columns_one_to_one( path.join( input_root, 'Program', 'Program.tsv' ), 'program_id', 'name' )

project_id_to_program_name = dict()

for project_id in project_id_to_program_id:
    
    project_id_to_program_name[project_id] = program_id_to_program_name[project_id_to_program_id[project_id]]

###

aliquot_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Aliquot', 'Aliquot.case_id.tsv' ), 'aliquot_id', 'case_id' )

aliquot_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Aliquot', 'Aliquot.study_id.tsv' ), 'aliquot_id', 'study_id' )

sample_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Sample', 'Sample.case_id.tsv' ), 'sample_id', 'case_id' )

sample_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Sample', 'Sample.study_id.tsv' ), 'sample_id', 'study_id' )

diagnosis_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.diagnosis_id.tsv' ), 'diagnosis_id', 'case_id' )

diagnosis_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Diagnosis', 'Diagnosis.study_id.tsv' ), 'diagnosis_id', 'study_id' )

demographic_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.demographic_id.tsv' ), 'demographic_id', 'case_id' )

demographic_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Demographic', 'Demographic.study_id.tsv' ), 'demographic_id', 'study_id' )

case_id_to_study_id = map_columns_one_to_many( path.join( input_root, 'Case', 'Case.study_id.tsv' ), 'case_id', 'study_id' )

case_id_to_project_id = map_columns_one_to_one( path.join( input_root, 'Case', 'Case.project_id.tsv' ), 'case_id', 'project_id' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

with open( out_file, 'w' ) as OUT:
    
    print( *[ 'program_name', 'project_submitter_id', 'study_submitter_id', 'pdc_study_id', 'entity_submitter_id', 'entity_id', 'entity_type' ], sep='\t', end='\n', file=OUT )

    for aliquot_id in sorted( aliquot_id_to_submitter_id ):
        
        case_id = aliquot_id_to_case_id[aliquot_id]
        
        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        program_name = project_id_to_program_name[project_id]

        for study_id in sorted( aliquot_id_to_study_id[aliquot_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            print( *[ program_name, project_submitter_id, study_submitter_id, study_id_to_pdc_id[study_id], aliquot_id_to_submitter_id[aliquot_id], aliquot_id, 'aliquot' ], sep='\t', end='\n', file=OUT )

    for sample_id in sorted( sample_id_to_submitter_id ):
        
        case_id = sample_id_to_case_id[sample_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        program_name = project_id_to_program_name[project_id]

        for study_id in sorted( sample_id_to_study_id[sample_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            print( *[ program_name, project_submitter_id, study_submitter_id, study_id_to_pdc_id[study_id], sample_id_to_submitter_id[sample_id], sample_id, 'sample' ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_submitter_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        program_name = project_id_to_program_name[project_id]

        for study_id in sorted( diagnosis_id_to_study_id[diagnosis_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_name, project_submitter_id, study_submitter_id, pdc_study_id, diagnosis_id_to_submitter_id[diagnosis_id], diagnosis_id, 'diagnosis' ], sep='\t', end='\n', file=OUT )

    for demographic_id in sorted( demographic_id_to_submitter_id ):
        
        case_id = demographic_id_to_case_id[demographic_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        program_name = project_id_to_program_name[project_id]

        for study_id in sorted( demographic_id_to_study_id[demographic_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_name, project_submitter_id, study_submitter_id, pdc_study_id, demographic_id_to_submitter_id[demographic_id], demographic_id, 'demographic' ], sep='\t', end='\n', file=OUT )

    for case_id in sorted( case_id_to_project_id ):
        
        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        program_name = project_id_to_program_name[project_id]

        for study_id in sorted( case_id_to_study_id[case_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_name, project_submitter_id, study_submitter_id, pdc_study_id, case_id_to_submitter_id[case_id], case_id, 'case' ], sep='\t', end='\n', file=OUT )



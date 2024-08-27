#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

aliquot_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.tsv' )

sample_input_tsv = path.join( input_root, 'Sample', 'Sample.tsv' )

diagnosis_input_tsv = path.join( input_root, 'Diagnosis', 'Diagnosis.tsv' )

case_input_tsv = path.join( input_root, 'Case', 'Case.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

project_input_tsv = path.join( input_root, 'Project', 'Project.tsv' )

program_input_tsv = path.join( input_root, 'Program', 'Program.tsv' )

program_project_input_tsv = path.join( input_root, 'Program', 'Program.project_id.tsv' )

aliquot_case_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.case_id.tsv' )

aliquot_study_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.study_id.tsv' )

sample_case_input_tsv = path.join( input_root, 'Sample', 'Sample.case_id.tsv' )

sample_study_input_tsv = path.join( input_root, 'Sample', 'Sample.study_id.tsv' )

case_diagnosis_input_tsv = path.join( input_root, 'Case', 'Case.diagnosis_id.tsv' )

diagnosis_study_input_tsv = path.join( input_root, 'Diagnosis', 'Diagnosis.study_id.tsv' )

case_study_input_tsv = path.join( input_root, 'Case', 'Case.study_id.tsv' )

case_project_input_tsv = path.join( input_root, 'Case', 'Case.project_id.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__PDC_supplemental_metadata' )

out_file = path.join( output_dir, 'PDC_entities_by_program_project_and_study.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

aliquot_id_to_submitter_id = map_columns_one_to_one( aliquot_input_tsv, 'aliquot_id', 'aliquot_submitter_id' )

sample_id_to_submitter_id = map_columns_one_to_one( sample_input_tsv, 'sample_id', 'sample_submitter_id' )

diagnosis_id_to_submitter_id = map_columns_one_to_one( diagnosis_input_tsv, 'diagnosis_id', 'diagnosis_submitter_id' )

case_id_to_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'case_submitter_id' )

study_id_to_submitter_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'study_submitter_id' )

study_id_to_pdc_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'pdc_study_id' )

project_id_to_submitter_id = map_columns_one_to_one( project_input_tsv, 'project_id', 'project_submitter_id' )

project_id_to_name = map_columns_one_to_one( project_input_tsv, 'project_id', 'name' )

program_id_to_submitter_id = map_columns_one_to_one( program_input_tsv, 'program_id', 'program_submitter_id' )

program_id_to_name = map_columns_one_to_one( program_input_tsv, 'program_id', 'name' )

project_id_to_program_id = map_columns_one_to_one( program_project_input_tsv, 'project_id', 'program_id' )

aliquot_id_to_case_id = map_columns_one_to_one( aliquot_case_input_tsv, 'aliquot_id', 'case_id' )

aliquot_id_to_study_id = map_columns_one_to_many( aliquot_study_input_tsv, 'aliquot_id', 'study_id' )

sample_id_to_case_id = map_columns_one_to_one( sample_case_input_tsv, 'sample_id', 'case_id' )

sample_id_to_study_id = map_columns_one_to_many( sample_study_input_tsv, 'sample_id', 'study_id' )

diagnosis_id_to_case_id = map_columns_one_to_one( case_diagnosis_input_tsv, 'diagnosis_id', 'case_id' )

diagnosis_id_to_study_id = map_columns_one_to_many( diagnosis_study_input_tsv, 'diagnosis_id', 'study_id' )

# The next two are verified one-per-case at time of writing (August 2024). Worth re-checking.

case_id_to_study_id = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'study_id' )

case_id_to_project_id = map_columns_one_to_one( case_project_input_tsv, 'case_id', 'project_id' )

with open( out_file, 'w' ) as OUT:
    
    print( *[ 'program.program_id', 'program.program_submitter_id', 'program.name', 'project.project_id', 'project.project_submitter_id', 'project.name', \
        'study.study_id', 'study.study_submitter_id', 'study.pdc_study_id', 'entity_submitter_id', 'entity_id', 'entity_type' ], sep='\t', end='\n', file=OUT )

    for case_id in sorted( case_id_to_project_id ):
        
        case_submitter_id = case_id_to_submitter_id[case_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        project_name = project_id_to_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_submitter_id = program_id_to_submitter_id[program_id]

        program_name = program_id_to_name[program_id]

        for study_id in sorted( case_id_to_study_id[case_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_id, program_submitter_id, program_name, project_id, project_submitter_id, project_name, \
                study_id, study_submitter_id, pdc_study_id, case_submitter_id, case_id, 'case' ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_submitter_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        diagnosis_submitter_id = diagnosis_id_to_submitter_id[diagnosis_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        project_name = project_id_to_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_submitter_id = program_id_to_submitter_id[program_id]

        program_name = program_id_to_name[program_id]

        for study_id in sorted( diagnosis_id_to_study_id[diagnosis_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_id, program_submitter_id, program_name, project_id, project_submitter_id, project_name, \
                study_id, study_submitter_id, pdc_study_id, diagnosis_submitter_id, diagnosis_id, 'diagnosis' ], sep='\t', end='\n', file=OUT )

    for aliquot_id in sorted( aliquot_id_to_submitter_id ):
        
        case_id = aliquot_id_to_case_id[aliquot_id]
        
        aliquot_submitter_id = aliquot_id_to_submitter_id[aliquot_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        project_name = project_id_to_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_submitter_id = program_id_to_submitter_id[program_id]

        program_name = program_id_to_name[program_id]

        for study_id in sorted( aliquot_id_to_study_id[aliquot_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_id, program_submitter_id, program_name, project_id, project_submitter_id, project_name, \
                study_id, study_submitter_id, pdc_study_id, aliquot_submitter_id, aliquot_id, 'aliquot' ], sep='\t', end='\n', file=OUT )

    for sample_id in sorted( sample_id_to_submitter_id ):
        
        case_id = sample_id_to_case_id[sample_id]

        sample_submitter_id = sample_id_to_submitter_id[sample_id]

        project_id = case_id_to_project_id[case_id]

        project_submitter_id = project_id_to_submitter_id[project_id]

        project_name = project_id_to_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_submitter_id = program_id_to_submitter_id[program_id]

        program_name = program_id_to_name[program_id]

        for study_id in sorted( sample_id_to_study_id[sample_id] ):
            
            study_submitter_id = study_id_to_submitter_id[study_id]

            pdc_study_id = study_id_to_pdc_id[study_id]

            print( *[ program_id, program_submitter_id, program_name, project_id, project_submitter_id, project_name, \
                study_id, study_submitter_id, pdc_study_id, sample_submitter_id, sample_id, 'sample' ], sep='\t', end='\n', file=OUT )



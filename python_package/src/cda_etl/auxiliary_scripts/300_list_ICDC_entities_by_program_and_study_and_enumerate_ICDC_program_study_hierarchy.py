#!/usr/bin/env python -u

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'icdc' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.tsv' )

case_input_tsv = path.join( input_root, 'case', 'case.tsv' )

program_input_tsv = path.join( input_root, 'program', 'program.tsv' )

sample_input_tsv = path.join( input_root, 'sample', 'sample.tsv' )

study_input_tsv = path.join( input_root, 'study', 'study.tsv' )

diagnosis_of_case_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

case_in_study_input_tsv = path.join( input_root, 'case', 'case.clinical_study_designation.tsv' )

sample_from_case_input_tsv = path.join( input_root, 'sample', 'sample.case_id.tsv' )

program_has_study_input_tsv = path.join( input_root, 'program', 'program.clinical_study_designation.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__ICDC_supplemental_metadata' )

entity_output_file = path.join( output_dir, 'ICDC_entities_by_program_and_study.tsv' )

project_output_file = path.join( output_dir, 'ICDC_all_programs_and_studies.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

case = load_tsv_as_dict( case_input_tsv )

# Key is 'program_name'

program = load_tsv_as_dict( program_input_tsv )

program_acronym_to_name = map_columns_one_to_one( program_input_tsv, 'program_acronym', 'program_name' )

sample = load_tsv_as_dict( sample_input_tsv )

# Key is 'clinical_study_designation'

study = load_tsv_as_dict( study_input_tsv )

diagnosis_of_case = map_columns_one_to_one( diagnosis_of_case_input_tsv, 'diagnosis_id', 'case_id' )

case_in_study = map_columns_one_to_many( case_in_study_input_tsv, 'case_id', 'clinical_study_designation' )

# This is one-to-one at time of writing (August 2024), but it's easy to generalize, so we do.

sample_from_case = map_columns_one_to_many( sample_from_case_input_tsv, 'sample_id', 'case_id' )

study_in_program = map_columns_one_to_one( program_has_study_input_tsv, 'clinical_study_designation', 'program_acronym' )

with open( project_output_file, 'w' ) as OUT:
    
    print( *[ 'program.program_name', 'program.program_acronym', 'study.clinical_study_designation', 'study.clinical_study_id', 'study.accession_id', 'study.clinical_study_name' ], sep='\t', file=OUT )

    for program_name in sorted( program ):
        
        program_acronym = program[program_name]['program_acronym']

        for study_id in sorted( study_in_program ):
            
            if program_acronym in study_in_program[study_id]:
                
                study_accession_id = study[study_id]['accession_id']

                study_clinical_study_id = study[study_id]['clinical_study_id']

                study_clinical_study_name = study[study_id]['clinical_study_name']

                print( *[ program_name, program_acronym, study_id, study_clinical_study_id, study_accession_id, study_clinical_study_name ], sep='\t', file=OUT )

with open( entity_output_file, 'w' ) as OUT:
    
    print( *[ 'program.program_name', 'program.program_acronym', 'study.clinical_study_designation', 'study.clinical_study_id', 'study.accession_id', 'study.clinical_study_name', 'entity_id', 'entity_type' ], sep='\t', file=OUT )

    for case_id in sorted( case ):
        
        for study_id in sorted( case_in_study[case_id] ):
            
            program_acronym = study_in_program[study_id]

            program_name = program_acronym_to_name[program_acronym]

            study_accession_id = study[study_id]['accession_id']

            study_clinical_study_id = study[study_id]['clinical_study_id']

            study_clinical_study_name = study[study_id]['clinical_study_name']

            print( *[ program_name, program_acronym, study_id, study_clinical_study_id, study_accession_id, study_clinical_study_name, case_id, 'case' ], sep='\t', file=OUT )

    for diagnosis_id in sorted( diagnosis ):
        
        case_id = diagnosis_of_case[diagnosis_id]

        for study_id in sorted( case_in_study[case_id] ):
            
            program_acronym = study_in_program[study_id]

            program_name = program_acronym_to_name[program_acronym]

            study_accession_id = study[study_id]['accession_id']

            study_clinical_study_id = study[study_id]['clinical_study_id']

            study_clinical_study_name = study[study_id]['clinical_study_name']

            print( *[ program_name, program_acronym, study_id, study_clinical_study_id, study_accession_id, study_clinical_study_name, diagnosis_id, 'diagnosis' ], sep='\t', file=OUT )

    for sample_id in sorted( sample ):
        
        for case_id in sorted( sample_from_case[sample_id] ):
            
            for study_id in sorted( case_in_study[case_id] ):
                
                program_acronym = study_in_program[study_id]

                program_name = program_acronym_to_name[program_acronym]

                study_accession_id = study[study_id]['accession_id']

                study_clinical_study_id = study[study_id]['clinical_study_id']

                study_clinical_study_name = study[study_id]['clinical_study_name']

                print( *[ program_name, program_acronym, study_id, study_clinical_study_id, study_accession_id, study_clinical_study_name, sample_id, 'sample' ], sep='\t', file=OUT )



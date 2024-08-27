#!/usr/bin/env python -u

import shutil
import sys

from cda_etl.lib import map_columns_one_to_one

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

program_input_tsv = path.join( input_root, 'program.tsv' )

project_input_tsv = path.join( input_root, 'project.tsv' )

project_in_program_input_tsv = path.join( input_root, 'project_in_program.tsv' )

aliquot_input_tsv = path.join( input_root, 'aliquot.tsv' )

analyte_input_tsv = path.join( input_root, 'analyte.tsv' )

portion_input_tsv = path.join( input_root, 'portion.tsv' )

slide_input_tsv = path.join( input_root, 'slide.tsv' )

sample_input_tsv = path.join( input_root, 'sample.tsv' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis.tsv' )

case_input_tsv = path.join( input_root, 'case.tsv' )

aliquot_from_case_input_tsv = path.join( input_root, 'aliquot_from_case.tsv' )

analyte_from_case_input_tsv = path.join( input_root, 'analyte_from_case.tsv' )

portion_from_case_input_tsv = path.join( input_root, 'portion_from_case.tsv' )

slide_from_case_input_tsv = path.join( input_root, 'slide_from_case.tsv' )

sample_from_case_input_tsv = path.join( input_root, 'sample_from_case.tsv' )

diagnosis_of_case_input_tsv = path.join( input_root, 'diagnosis_of_case.tsv' )

case_in_project_input_tsv = path.join( input_root, 'case_in_project.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__GDC_supplemental_metadata' )

out_file = path.join( output_dir, 'GDC_entities_by_program_and_project.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

project_id_to_project_name = map_columns_one_to_one( project_input_tsv, 'project_id', 'name' )

program_id_to_program_name = map_columns_one_to_one( program_input_tsv, 'program_id', 'name' )

project_id_to_program_id = map_columns_one_to_one( project_in_program_input_tsv, 'project_id', 'program_id' )

aliquot_id_to_submitter_id = map_columns_one_to_one( aliquot_input_tsv, 'aliquot_id', 'submitter_id' )

analyte_id_to_submitter_id = map_columns_one_to_one( analyte_input_tsv, 'analyte_id', 'submitter_id' )

portion_id_to_submitter_id = map_columns_one_to_one( portion_input_tsv, 'portion_id', 'submitter_id' )

slide_id_to_submitter_id = map_columns_one_to_one( slide_input_tsv, 'slide_id', 'submitter_id' )

sample_id_to_submitter_id = map_columns_one_to_one( sample_input_tsv, 'sample_id', 'submitter_id' )

diagnosis_id_to_submitter_id = map_columns_one_to_one( diagnosis_input_tsv, 'diagnosis_id', 'submitter_id' )

case_id_to_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'submitter_id' )

aliquot_id_to_case_id = map_columns_one_to_one( aliquot_from_case_input_tsv, 'aliquot_id', 'case_id' )

analyte_id_to_case_id = map_columns_one_to_one( analyte_from_case_input_tsv, 'analyte_id', 'case_id' )

portion_id_to_case_id = map_columns_one_to_one( portion_from_case_input_tsv, 'portion_id', 'case_id' )

slide_id_to_case_id = map_columns_one_to_one( slide_from_case_input_tsv, 'slide_id', 'case_id' )

sample_id_to_case_id = map_columns_one_to_one( sample_from_case_input_tsv, 'sample_id', 'case_id' )

diagnosis_id_to_case_id = map_columns_one_to_one( diagnosis_of_case_input_tsv, 'diagnosis_id', 'case_id' )

case_id_to_project_id = map_columns_one_to_one( case_in_project_input_tsv, 'case_id', 'project_id' )

with open( out_file, 'w' ) as OUT:
    
    print( *[ 'program.program_id', 'program.name', 'project.project_id', 'project.name', 'entity_submitter_id', 'entity_id', 'entity_type' ], sep='\t', end='\n', file=OUT )

    for case_id in sorted( case_id_to_project_id ):
        
        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        case_submitter_id = case_id_to_submitter_id[case_id]

        print( *[ program_id, program_name, project_id, project_name, case_submitter_id, case_id, 'case' ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_submitter_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        diagnosis_submitter_id = diagnosis_id_to_submitter_id[diagnosis_id]

        print( *[ program_id, program_name, project_id, project_name, diagnosis_submitter_id, diagnosis_id, 'diagnosis' ], sep='\t', end='\n', file=OUT )

    for aliquot_id in sorted( aliquot_id_to_submitter_id ):
        
        case_id = aliquot_id_to_case_id[aliquot_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        aliquot_submitter_id = aliquot_id_to_submitter_id[aliquot_id]

        print( *[ program_id, program_name, project_id, project_name, aliquot_submitter_id, aliquot_id, 'aliquot' ], sep='\t', end='\n', file=OUT )

    for analyte_id in sorted( analyte_id_to_submitter_id ):
        
        case_id = analyte_id_to_case_id[analyte_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        analyte_submitter_id = analyte_id_to_submitter_id[analyte_id]

        print( *[ program_id, program_name, project_id, project_name, analyte_submitter_id, analyte_id, 'analyte' ], sep='\t', end='\n', file=OUT )

    for portion_id in sorted( portion_id_to_submitter_id ):
        
        case_id = portion_id_to_case_id[portion_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        portion_submitter_id = portion_id_to_submitter_id[portion_id]

        print( *[ program_id, program_name, project_id, project_name, portion_submitter_id, portion_id, 'portion' ], sep='\t', end='\n', file=OUT )

    for slide_id in sorted( slide_id_to_submitter_id ):
        
        case_id = slide_id_to_case_id[slide_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        slide_submitter_id = slide_id_to_submitter_id[slide_id]

        print( *[ program_id, program_name, project_id, project_name, slide_submitter_id, slide_id, 'slide' ], sep='\t', end='\n', file=OUT )

    for sample_id in sorted( sample_id_to_submitter_id ):
        
        case_id = sample_id_to_case_id[sample_id]

        project_id = case_id_to_project_id[case_id]

        project_name = project_id_to_project_name[project_id]

        program_id = project_id_to_program_id[project_id]

        program_name = program_id_to_program_name[program_id]

        sample_submitter_id = sample_id_to_submitter_id[sample_id]

        print( *[ program_id, program_name, project_id, project_name, sample_submitter_id, sample_id, 'sample' ], sep='\t', end='\n', file=OUT )



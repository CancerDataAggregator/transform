#!/usr/bin/env python -u

import shutil
import sys

from cda_etl.lib import map_columns_one_to_one

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

output_dir = path.join( 'auxiliary_metadata', '__GDC_supplemental_metadata' )

out_file = path.join( output_dir, 'GDC_entity_submitter_id_to_program_name_and_project_id.tsv' )

project_id_to_program_id = map_columns_one_to_one( path.join( input_root, 'project_in_program.tsv' ), 'project_id', 'program_id' )

program_id_to_program_name = map_columns_one_to_one( path.join( input_root, 'program.tsv' ), 'program_id', 'name' )

project_id_to_program_name = dict()

for project_id in project_id_to_program_id:
    
    project_id_to_program_name[project_id] = program_id_to_program_name[project_id_to_program_id[project_id]]

###

aliquot_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'aliquot.tsv' ), 'aliquot_id', 'submitter_id' )

analyte_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'analyte.tsv' ), 'analyte_id', 'submitter_id' )

portion_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'portion.tsv' ), 'portion_id', 'submitter_id' )

slide_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'slide.tsv' ), 'slide_id', 'submitter_id' )

sample_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'sample.tsv' ), 'sample_id', 'submitter_id' )

diagnosis_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'diagnosis.tsv' ), 'diagnosis_id', 'submitter_id' )

case_id_to_submitter_id = map_columns_one_to_one( path.join( input_root, 'case.tsv' ), 'case_id', 'submitter_id' )

###

aliquot_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'aliquot_from_case.tsv' ), 'aliquot_id', 'case_id' )

analyte_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'analyte_from_case.tsv' ), 'analyte_id', 'case_id' )

portion_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'portion_from_case.tsv' ), 'portion_id', 'case_id' )

slide_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'slide_from_case.tsv' ), 'slide_id', 'case_id' )

sample_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'sample_from_case.tsv' ), 'sample_id', 'case_id' )

diagnosis_id_to_case_id = map_columns_one_to_one( path.join( input_root, 'diagnosis_of_case.tsv' ), 'diagnosis_id', 'case_id' )

case_id_to_project_id = map_columns_one_to_one( path.join( input_root, 'case_in_project.tsv' ), 'case_id', 'project_id' )


# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

with open( out_file, 'w' ) as OUT:
    
    print( *[ 'entity_type', 'entity_submitter_id', 'GDC_project_id', 'GDC_program_name' ], sep='\t', end='\n', file=OUT )

    for aliquot_id in sorted( aliquot_id_to_submitter_id ):
        
        case_id = aliquot_id_to_case_id[aliquot_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'aliquot', aliquot_id_to_submitter_id[aliquot_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for analyte_id in sorted( analyte_id_to_submitter_id ):
        
        case_id = analyte_id_to_case_id[analyte_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'analyte', analyte_id_to_submitter_id[analyte_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for portion_id in sorted( portion_id_to_submitter_id ):
        
        case_id = portion_id_to_case_id[portion_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'portion', portion_id_to_submitter_id[portion_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for slide_id in sorted( slide_id_to_submitter_id ):
        
        case_id = slide_id_to_case_id[slide_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'slide', slide_id_to_submitter_id[slide_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for sample_id in sorted( sample_id_to_submitter_id ):
        
        case_id = sample_id_to_case_id[sample_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'sample', sample_id_to_submitter_id[sample_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_submitter_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ 'diagnosis', diagnosis_id_to_submitter_id[diagnosis_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )

    for case_id in sorted( case_id_to_project_id ):
        
        project_id = case_id_to_project_id[case_id]

        print( *[ 'case', case_id_to_submitter_id[case_id], project_id, project_id_to_program_name[project_id] ], sep='\t', end='\n', file=OUT )



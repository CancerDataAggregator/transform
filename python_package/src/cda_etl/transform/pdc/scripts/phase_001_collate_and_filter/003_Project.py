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

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Project' )

input_files_to_output_files = {
    'Project.tsv': 'Project.tsv',
    'Project.studies.tsv': 'Project.study_id.tsv'
}

experiment_project_tsv = path.join( input_root, 'ExperimentProject', 'ExperimentProject.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Project' )

project_experiment_type_tsv = path.join( output_dir, 'Project.experiment_type.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

for input_file in input_files_to_output_files:
    
    shutil.copy2( path.join( input_dir, input_file ), path.join( output_dir, input_files_to_output_files[input_file] ) )

project_submitter_id_to_project_id = map_columns_one_to_one( path.join( input_dir, 'Project.tsv' ), 'project_submitter_id', 'project_id' )

project_id_to_experiment_type = dict()

with open( experiment_project_tsv ) as IN:
    
    header = next(IN).rstrip('\n')

    column_names = header.split('\t')

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        values = line.split('\t')

        current_row = dict()

        for i in range( 0, len( column_names ) ):
            
            current_row[column_names[i]] = values[i]

        project_id = project_submitter_id_to_project_id[current_row['project_submitter_id']]

        experiment_type = current_row['experiment_type']

        if project_id not in project_id_to_experiment_type:
            
            project_id_to_experiment_type[project_id] = set()

        project_id_to_experiment_type[project_id].add(experiment_type)

with open( project_experiment_type_tsv, 'w' ) as OUT:
    
    print( *[ 'project_id', 'experiment_type' ], sep='\t', end='\n', file=OUT )

    for project_id in sorted( project_id_to_experiment_type ):
        
        for experiment_type in sorted( project_id_to_experiment_type[project_id] ):
            
            print( *[ project_id, experiment_type ], sep='\t', end='\n', file=OUT )



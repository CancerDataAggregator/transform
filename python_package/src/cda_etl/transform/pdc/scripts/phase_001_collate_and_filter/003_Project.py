#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one

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



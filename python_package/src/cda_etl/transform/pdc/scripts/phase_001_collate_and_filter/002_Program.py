#!/usr/bin/env python -u

import shutil

from os import path, makedirs

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Program' )

input_files_to_output_files = {
    'Program.tsv': 'Program.tsv',
    'Program.projects.tsv': 'Program.project_id.tsv'
}

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Program' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

for input_file in input_files_to_output_files:
    
    shutil.copy2( path.join( input_dir, input_file ), path.join( output_dir, input_files_to_output_files[input_file] ) )



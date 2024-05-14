#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc'

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

input_dir = path.join( input_root, 'Protocol' )

protocol_input_tsv = path.join( input_dir, 'Protocol.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Protocol' )

protocol_output_tsv = path.join( output_dir, 'Protocol.tsv' )

protocol_study_output_tsv = path.join( output_dir, 'Protocol.study_id.tsv' )

input_files_to_scan = {
    'Protocol': protocol_input_tsv
}

fields_to_ignore = {
    'Protocol': {
        'study_id',
        'study_submitter_id',
        'pdc_study_id',
        'project_submitter_id',
        'program_id',
        'program_submitter_id'
    }
}

values_to_ignore = {
}

processing_order = [
    'Protocol'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main Protocol data.

protocols = dict()

protocol_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for i in range( 0, len( column_names ) ):
            
            column_name = column_names[i]

            if column_name not in fields_to_ignore[input_file_label]:
                
                protocol_fields.append( column_name )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            if current_id not in protocols:
                
                protocols[current_id] = dict()

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        protocols[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        protocols[current_id][current_column_name] = ''

# Write the main Protocol table.

with open( protocol_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *protocol_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( protocols ):
        
        output_row = [ current_id ] + [ protocols[current_id][field_name] for field_name in protocol_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Load the Protocol->study_id association.

protocol_id_to_study_id = map_columns_one_to_many( protocol_input_tsv, 'protocol_id', 'study_id' )

# Sanity check: load Study IDs that exist.

study_id_to_study_submitter_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'study_submitter_id' )

# Write the Protocol->study_id association.

with open( protocol_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'protocol_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for protocol_id in sorted( protocol_id_to_study_id ):
        
        for study_id in sorted( protocol_id_to_study_id[protocol_id] ):
            
            if study_id in study_id_to_study_submitter_id:
                
                print( *[ protocol_id, study_id ], sep='\t', end='\n', file=OUT )

            else:
                
                print( f"WARNING: Skipping unfound Protocol-associated Study ID '{study_id}'", file=sys.stderr )



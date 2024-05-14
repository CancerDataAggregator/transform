#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc'

protocol_input_tsv = path.join( input_root, 'Protocol', 'Protocol.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

input_dir = path.join( input_root, 'WorkflowMetadata' )

workflow_metadata_input_tsv = path.join( input_dir, 'WorkflowMetadata.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'WorkflowMetadata' )

workflow_metadata_output_tsv = path.join( output_dir, 'WorkflowMetadata.tsv' )

workflow_metadata_study_output_tsv = path.join( output_dir, 'WorkflowMetadata.study_id.tsv' )

workflow_metadata_protocol_output_tsv = path.join( output_dir, 'WorkflowMetadata.protocol_id.tsv' )

input_files_to_scan = {
    'WorkflowMetadata': workflow_metadata_input_tsv
}

fields_to_ignore = {
    'WorkflowMetadata': {
        'protocol_id',
        'protocol_submitter_id',
        'study_id',
        'study_submitter_id',
        'study_submitter_name',
        'pdc_study_id'
    }
}

values_to_ignore = {
}

processing_order = [
    'WorkflowMetadata'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main WorkflowMetadata data.

workflow_metadata = dict()

workflow_metadata_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for i in range( 0, len( column_names ) ):
            
            column_name = column_names[i]

            if column_name not in fields_to_ignore[input_file_label]:
                
                workflow_metadata_fields.append( column_name )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            if current_id not in workflow_metadata:
                
                workflow_metadata[current_id] = dict()

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        workflow_metadata[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        workflow_metadata[current_id][current_column_name] = ''

# Write the main WorkflowMetadata table.

with open( workflow_metadata_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *workflow_metadata_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( workflow_metadata ):
        
        output_row = [ current_id ] + [ workflow_metadata[current_id][field_name] for field_name in workflow_metadata_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Load the WorkflowMetadata->study_id association.

workflow_metadata_id_to_study_id = map_columns_one_to_many( workflow_metadata_input_tsv, 'workflow_metadata_id', 'study_id' )

# Sanity check: load Study IDs that exist.

study_id_to_study_submitter_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'study_submitter_id' )

# Write the WorkflowMetadata->study_id association.

with open( workflow_metadata_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'workflow_metadata_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for workflow_metadata_id in sorted( workflow_metadata_id_to_study_id ):
        
        for study_id in sorted( workflow_metadata_id_to_study_id[workflow_metadata_id] ):
            
            if study_id in study_id_to_study_submitter_id:
                
                print( *[ workflow_metadata_id, study_id ], sep='\t', end='\n', file=OUT )

            else:
                
                print( f"WARNING: Skipping unfound WorkflowMetadata-associated Study ID '{study_id}'", file=sys.stderr )

# Load the WorkflowMetadata->protocol_id association.

workflow_metadata_id_to_protocol_id = map_columns_one_to_many( workflow_metadata_input_tsv, 'workflow_metadata_id', 'protocol_id' )

# Sanity check: load Protocol IDs that exist.

protocol_id_to_protocol_submitter_id = map_columns_one_to_one( protocol_input_tsv, 'protocol_id', 'protocol_submitter_id' )

# Write the WorkflowMetadata->protocol_id association.

with open( workflow_metadata_protocol_output_tsv, 'w' ) as OUT:
    
    print( *[ 'workflow_metadata_id', 'protocol_id' ], sep='\t', end='\n', file=OUT )

    for workflow_metadata_id in sorted( workflow_metadata_id_to_protocol_id ):
        
        for protocol_id in sorted( workflow_metadata_id_to_protocol_id[workflow_metadata_id] ):
            
            if protocol_id in protocol_id_to_protocol_submitter_id:
                
                print( *[ workflow_metadata_id, protocol_id ], sep='\t', end='\n', file=OUT )

            else:
                
                print( f"ABSOLUTELY NON-FATAL WARNING: Skipping unfound WorkflowMetadata-associated Protocol ID '{protocol_id}'", file=sys.stderr )


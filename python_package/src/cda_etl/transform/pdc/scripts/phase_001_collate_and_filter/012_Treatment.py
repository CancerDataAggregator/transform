#!/usr/bin/env python -u

import sys

from os import path, makedirs

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Case' )

treatment_input_tsv = path.join( input_dir, 'Treatment.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Treatment' )

treatment_output_tsv = path.join( output_dir, 'Treatment.tsv' )

input_files_to_scan = {
    'Treatment': treatment_input_tsv
}

fields_to_ignore = {
    'Treatment': {
        'case_id',
        'case_submitter_id',
        'project_id',
        'project_submitter_id'
    }
}

values_to_ignore = {
}

processing_order = [
    'Treatment'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main Treatment data.

treatments = dict()

treatment_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for i in range( 0, len( column_names ) ):
            
            column_name = column_names[i]

            if column_name not in fields_to_ignore[input_file_label]:
                
                treatment_fields.append( column_name )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            if current_id not in treatments:
                
                treatments[current_id] = dict()

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        treatments[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        treatments[current_id][current_column_name] = ''

# Write the main Treatment table.

with open( treatment_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *treatment_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( treatments ):
        
        output_row = [ current_id ] + [ treatments[current_id][field_name] for field_name in treatment_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )



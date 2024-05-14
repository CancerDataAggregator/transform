#!/usr/bin/env python -u

import sys

from os import path, makedirs

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Case' )

exposure_input_tsv = path.join( input_dir, 'Exposure.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Exposure' )

exposure_output_tsv = path.join( output_dir, 'Exposure.tsv' )

input_files_to_scan = {
    'Exposure': exposure_input_tsv
}

fields_to_ignore = {
    'Exposure': {
        'case_id',
        'case_submitter_id',
        'project_id',
        'project_submitter_id'
    }
}

values_to_ignore = {
}

processing_order = [
    'Exposure'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main Exposure data.

exposures = dict()

exposure_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            if current_id not in exposures:
                
                exposures[current_id] = dict()

            if id_field_name not in exposure_fields:
                
                exposure_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name not in exposure_fields:
                        
                        exposure_fields.append( current_column_name )

                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        exposures[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        exposures[current_id][current_column_name] = ''

# Write the main Exposure table.

with open( exposure_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *exposure_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( exposures ):
        
        output_row = [ current_id ] + [ exposures[current_id][field_name] for field_name in exposure_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )



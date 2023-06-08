#!/usr/bin/env python -u

import sys

from os import path, makedirs

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Diagnosis' )

diagnosis_input_tsv = path.join( input_dir, 'Diagnosis.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Diagnosis' )

diagnosis_output_tsv = path.join( output_dir, 'Diagnosis.tsv' )

input_files_to_scan = {
    'Diagnosis': diagnosis_input_tsv
}

fields_to_ignore = {
    'Diagnosis': {
        'cases_count',
        'case_id',
        'case_submitter_id',
        'project_id',
        'project_submitter_id'
    }
}

values_to_ignore = {
    'N/A',
    '--',
    'Not Reported',
    'Not reported',
    'Unknown',
    'unknown',
    'Unknown tumor status',
    'Unknown tumor Status',
    'Unknown Tumor Status',
    'Not Reported/Unknown',
    'Not Reported/ Unknown'
}

processing_order = [
    'Diagnosis'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main Diagnosis data.

diagnoses = dict()

diagnosis_fields = list()

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

            if current_id not in diagnoses:
                
                diagnoses[current_id] = dict()

            if id_field_name not in diagnosis_fields:
                
                diagnosis_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name not in diagnosis_fields:
                        
                        diagnosis_fields.append( current_column_name )

                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        diagnoses[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        diagnoses[current_id][current_column_name] = ''

# Write the main Diagnosis table.

with open( diagnosis_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *diagnosis_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( diagnoses ):
        
        output_row = [ current_id ] + [ diagnoses[current_id][field_name] for field_name in diagnosis_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )



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

def map_columns_one_to_many( input_file, from_field, to_field ):
    
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

            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add(current_to)

    return return_map

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Case' )

case_input_tsv = path.join( input_dir, 'Case.tsv' )

case_id_to_demographic_id_input_tsv = path.join( input_dir, 'Case.demographics.tsv' )

case_id_to_diagnosis_id_input_tsv = path.join( input_dir, 'Case.diagnoses.tsv' )

case_id_to_sample_id_input_tsv = path.join( input_dir, 'Case.samples.tsv' )

biospecimen_input_tsv = path.join( input_root, 'Biospecimen', 'Biospecimen.tsv' )

clinical_study_input_tsv = path.join( input_root, 'Clinical', 'Clinical.Study.tsv' )

project_input_tsv = path.join( input_root, 'Project', 'Project.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

output_root = 'merged_data/pdc'

output_dir = path.join( output_root, 'Case' )

case_output_tsv = path.join( output_dir, 'Case.tsv' )

case_id_to_demographic_id_output_tsv = path.join( output_dir, 'Case.demographic_id.tsv' )

case_id_to_diagnosis_id_output_tsv = path.join( output_dir, 'Case.diagnosis_id.tsv' )

case_id_to_project_id_output_tsv = path.join( output_dir, 'Case.project_id.tsv' )

case_id_to_sample_id_output_tsv = path.join( output_dir, 'Case.sample_id.tsv' )

case_id_to_study_id_output_tsv = path.join( output_dir, 'Case.study_id.tsv' )

demographic_id_to_project_id_output_tsv = path.join( output_root, 'Demographic', 'Demographic.project_id.tsv' )

diagnosis_id_to_project_id_output_tsv = path.join( output_root, 'Diagnosis', 'Diagnosis.project_id.tsv' )

input_files_to_output_files = {
    case_id_to_demographic_id_input_tsv: case_id_to_demographic_id_output_tsv,
    case_id_to_diagnosis_id_input_tsv: case_id_to_diagnosis_id_output_tsv,
    case_id_to_sample_id_input_tsv: case_id_to_sample_id_output_tsv
}

input_files_to_scan = {
    'Case': case_input_tsv
}

fields_to_ignore = {
    'Case': {
        'project_id',
        'project_submitter_id'
    }
}

values_to_ignore = {
    'N/A',
    'Not Reported',
    'Unknown',
    'Other'
}

processing_order = [
    'Case'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Copy over any input files that (at most) require renaming.

for input_file, output_file in input_files_to_output_files.items():
    
    shutil.copy2( input_file, output_file )

# Load the map from case_id to case_status from `biospecimen_input_tsv`.

case_id_to_case_status = map_columns_one_to_one( biospecimen_input_tsv, 'case_id', 'case_status' )

# Load the map from pdc_study_id to (current) study_id.

pdc_study_id_to_study_id = map_columns_one_to_one( study_input_tsv, 'pdc_study_id', 'study_id' )

# Load the map from case_id to pdc_study_id.

case_id_to_pdc_study_id = map_columns_one_to_many( clinical_study_input_tsv, 'case_id', 'pdc_study_id' )

# Load the map from project_submitter_id to project_id.

project_submitter_id_to_project_id = map_columns_one_to_one( project_input_tsv, 'project_submitter_id', 'project_id' )

# Load the map from case_id to project_submitter_id.

case_id_to_project_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'project_submitter_id' )

# Load the main Case data.

cases = dict()

case_fields = list()

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

            if current_id not in cases:
                
                cases[current_id] = dict()

            if id_field_name not in case_fields:
                
                case_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name not in case_fields:
                        
                        case_fields.append( current_column_name )

                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        cases[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        cases[current_id][current_column_name] = ''

# Add case_status, as loaded from the Biospecimen TSV above, to all Case records.

for current_id in cases:
    
    if current_id in case_id_to_case_status:
        
        cases[current_id]['case_status'] = case_id_to_case_status[current_id]

    else:
        
        cases[current_id]['case_status'] = ''

case_fields.append( 'case_status' )

# Write the main Case table.

with open( case_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *case_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( cases ):
        
        output_row = [ current_id ] + [ cases[current_id][field_name] for field_name in case_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Write the map between case_id and study_id.

with open( case_id_to_study_id_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'case_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for case_id in sorted( case_id_to_pdc_study_id ):
        
        for pdc_study_id in sorted( case_id_to_pdc_study_id[case_id] ):
            
            # We want this to break with a KeyError if the target doesn't exist, because it should exist.

            study_id = pdc_study_id_to_study_id[pdc_study_id]

            print( *[ case_id, study_id ], sep='\t', end='\n', file=OUT )

# Write the map between case_id and project_id.

with open( case_id_to_project_id_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'case_id', 'project_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for case_id in sorted( case_id_to_project_submitter_id ):
        
        project_submitter_id = case_id_to_project_submitter_id[case_id]
            
        # We want this to break with a KeyError if the target doesn't exist, because it should exist.

        project_id = project_submitter_id_to_project_id[project_submitter_id]

        print( *[ case_id, project_id ], sep='\t', end='\n', file=OUT )

# Load the map we just wrote.

case_id_to_project_id = map_columns_one_to_one( case_id_to_project_id_output_tsv, 'case_id', 'project_id' )

# Write the map between demographic_id and project_id.

demographic_id_to_case_id = map_columns_one_to_one( case_id_to_demographic_id_output_tsv, 'demographic_id', 'case_id' )

with open( demographic_id_to_project_id_output_tsv, 'w' ) as OUT:
    
    print( *[ 'demographic_id', 'project_id' ], sep='\t', end='\n', file=OUT )

    for demographic_id in sorted( demographic_id_to_case_id ):
        
        case_id = demographic_id_to_case_id[demographic_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ demographic_id, project_id ], sep='\t', end='\n', file=OUT )

# Write the map between diagnosis_id and project_id.

diagnosis_id_to_case_id = map_columns_one_to_one( case_id_to_diagnosis_id_output_tsv, 'diagnosis_id', 'case_id' )

with open( diagnosis_id_to_project_id_output_tsv, 'w' ) as OUT:
    
    print( *[ 'diagnosis_id', 'project_id' ], sep='\t', end='\n', file=OUT )

    for diagnosis_id in sorted( diagnosis_id_to_case_id ):
        
        case_id = diagnosis_id_to_case_id[diagnosis_id]

        project_id = case_id_to_project_id[case_id]

        print( *[ diagnosis_id, project_id ], sep='\t', end='\n', file=OUT )



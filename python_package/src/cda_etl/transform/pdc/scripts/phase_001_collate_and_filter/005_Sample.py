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

sample_input_tsv = path.join( input_dir, 'Sample.tsv' )

sample_aliquot_input_tsv = path.join( input_dir, 'Sample.aliquots.tsv' )

sample_diagnosis_input_tsv = path.join( input_dir, 'Sample.diagnoses.tsv' )

biospecimen_input_tsv = path.join( input_root, 'Biospecimen', 'Biospecimen.tsv' )

biospecimen_study_input_tsv = path.join( input_root, 'Biospecimen', 'Biospecimen.Study.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Sample' )

project_study_input_tsv = path.join( output_root, 'Project', 'Project.study_id.tsv' )

sample_output_tsv = path.join( output_dir, 'Sample.tsv' )

sample_aliquot_output_tsv = path.join( output_dir, 'Sample.aliquot_id.tsv' )

sample_diagnosis_output_tsv = path.join( output_dir, 'Sample.diagnosis_id.tsv' )

sample_id_to_case_id_output_tsv = path.join( output_dir, 'Sample.case_id.tsv' )

sample_id_to_study_id_output_tsv = path.join( output_dir, 'Sample.study_id.tsv' )

sample_id_to_project_id_output_tsv = path.join( output_dir, 'Sample.project_id.tsv' )

log_dir = path.join( output_root, '__filtration_logs' )

sample_multi_diagnosis_filtration_log = path.join( log_dir, 'Sample.multiple_distinct_Diagnosis_links.unlinked_sample_ids.txt' )

input_files_to_output_files = {
    sample_aliquot_input_tsv: sample_aliquot_output_tsv
}

input_files_to_scan = {
    'Sample': sample_input_tsv
}

fields_to_ignore = {
    'Sample': {
        'case_submitter_id'
    }
}

values_to_ignore = {
    'N/A',
    'Not Reported',
    'NULL',
    'Unknown'
}

processing_order = [
    'Sample'
]

# EXECUTION

for target_dir in [ output_dir, log_dir ]:
    
    if not path.exists( target_dir ):
        
        makedirs( target_dir )

# Load the map between study_id and project_id.

study_id_to_project_id = map_columns_one_to_one( project_study_input_tsv, 'study_id', 'project_id' )

# Load the map from pdc_study_id to (current) study_id.

pdc_study_id_to_study_id = map_columns_one_to_one( study_input_tsv, 'pdc_study_id', 'study_id' )

# Load the map between aliquot_id and pdc_study_id.

aliquot_id_to_pdc_study_id = map_columns_one_to_many( biospecimen_study_input_tsv, 'aliquot_id', 'pdc_study_id' )

# Load the map between sample_id and aliquot_id (via a different route than using `sample_aliquot_input_tsv`, for validation/cross-checking porpoises).

sample_id_to_aliquot_id = map_columns_one_to_many( biospecimen_input_tsv, 'sample_id', 'aliquot_id' )

# Load the map between sample_id and case_id (via a different route than using Case/Case.samples.tsv, again for validation porpoises).

sample_id_to_case_id = map_columns_one_to_one( biospecimen_input_tsv, 'sample_id', 'case_id' )

# Copy over any input files that (at most) require renaming.

for input_file, output_file in input_files_to_output_files.items():
    
    shutil.copy2( input_file, output_file )

# Filter Sample.diagnoses.tsv to exclude any records whose sample_ids are linked to multiple diagnosis_ids.
# (See ingest policy document for rationale.)

seen_sample_ids = set()

sample_ids_to_filter = set()

with open( sample_diagnosis_input_tsv ) as IN:
    
    header = next(IN)

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        values = line.split('\t')

        sample_id = values[0]

        if sample_id in seen_sample_ids:
            
            sample_ids_to_filter.add( sample_id )

        seen_sample_ids.add( sample_id )

with open( sample_diagnosis_input_tsv ) as IN, open( sample_multi_diagnosis_filtration_log, 'w' ) as LOG:
    
    with open( sample_diagnosis_output_tsv, 'w' ) as OUT:
        
        header = next(IN).rstrip('\n')

        print( header, file=OUT )

        printed = set()

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            sample_id = values[0]

            if sample_id not in sample_ids_to_filter:
                
                print( line, file=OUT )

            elif sample_id not in printed:
                
                print( sample_id, file=LOG )

                printed.add( sample_id )

# Load the main Sample data.

samples = dict()

sample_fields = list()

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

            samples[current_id] = dict()

            if id_field_name not in sample_fields:
                
                sample_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name not in sample_fields:
                        
                        sample_fields.append( current_column_name )

                    if current_value != '' and current_value not in values_to_ignore:
                        
                        # If the current value isn't null or set to be skipped, record it.

                        samples[current_id][current_column_name] = current_value

                    else:
                        
                        # If the current value IS null, save an empty string.

                        samples[current_id][current_column_name] = ''

# Attach any available species information to each loaded sample.

sample_id_to_taxon = map_columns_one_to_one( biospecimen_input_tsv, 'sample_id', 'taxon' )

for sample_id in samples:
    
    if sample_id in sample_id_to_taxon:
        
        samples[sample_id]['taxon'] = sample_id_to_taxon[sample_id]

sample_fields.append( 'taxon' )

# Write the main Sample table.

with open( sample_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *sample_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( samples ):
        
        output_row = [ current_id ] + [ samples[current_id][field_name] for field_name in sample_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Write the map between sample_id and case_id.

with open( sample_id_to_case_id_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'sample_id', 'case_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for sample_id in sorted( sample_id_to_case_id ):
        
        if sample_id in samples:
            
            print( *[ sample_id, sample_id_to_case_id[sample_id] ], sep='\t', end='\n', file=OUT )

# Write the map between sample_id and study_id and the map between sample_id and project_id.

printed_sample_project_pair = dict()

with open( sample_id_to_study_id_output_tsv, 'w' ) as SAMPLE_STUDY, open( sample_id_to_project_id_output_tsv, 'w' ) as SAMPLE_PROJECT:
    
    # Header rows.

    print( *[ 'sample_id', 'study_id' ], sep='\t', end='\n', file=SAMPLE_STUDY )
    print( *[ 'sample_id', 'project_id' ], sep='\t', end='\n', file=SAMPLE_PROJECT )

    # Data rows.

    for sample_id in sorted( sample_id_to_aliquot_id ):
        
        if sample_id in samples:
            
            target_study_ids = set()

            # We want this to break with a KeyError if the target doesn't exist, because it should exist.

            for aliquot_id in sorted( sample_id_to_aliquot_id[sample_id] ):
                
                # We want this to break with a KeyError if the target doesn't exist, because it should exist.

                for pdc_study_id in sorted( aliquot_id_to_pdc_study_id[aliquot_id] ):
                    
                    # We want this to break with a KeyError if the target doesn't exist, because it should exist.

                    study_id = pdc_study_id_to_study_id[pdc_study_id]

                    target_study_ids.add( study_id )

            for study_id in sorted( target_study_ids ):
                
                print( *[ sample_id, study_id ], sep='\t', end='\n', file=SAMPLE_STUDY )

                project_id = study_id_to_project_id[study_id]

                if sample_id not in printed_sample_project_pair:
                    
                    printed_sample_project_pair[sample_id] = set()

                if project_id not in printed_sample_project_pair[sample_id]:
                    
                    print( *[ sample_id, project_id ], sep='\t', end='\n', file=SAMPLE_PROJECT )

                    printed_sample_project_pair[sample_id].add( project_id )



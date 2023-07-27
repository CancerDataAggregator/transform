#!/usr/bin/env python -u

import sys

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

from os import path, makedirs, rename

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'CasesSamplesAliquots' )

aliquot_input_tsv = path.join( input_dir, 'Aliquot.tsv' )

aliquot_aliquot_run_metadata_input_tsv = path.join( input_dir, 'Aliquot.aliquot_run_metadata.tsv' )

biospecimen_input_tsv = path.join( input_root, 'Biospecimen', 'Biospecimen.tsv' )

biospecimen_study_input_tsv = path.join( input_root, 'Biospecimen', 'Biospecimen.Study.tsv' )

file_metadata_aliquot_input_tsv = path.join( input_root, 'FileMetadata', 'Aliquot.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Aliquot' )

project_study_input_tsv = path.join( output_root, 'Project', 'Project.study_id.tsv' )

sample_input_tsv = path.join( output_root, 'Sample', 'Sample.tsv' )

sample_study_input_tsv = path.join( output_root, 'Sample', 'Sample.study_id.tsv' )

file_aliquot_output_tsv = path.join( output_root, 'File', 'File.aliquot_id.tsv' )

sample_aliquot_output_tsv = path.join( output_root, 'Sample', 'Sample.aliquot_id.tsv' )

aliquot_output_tsv = path.join( output_dir, 'Aliquot.tsv' )

aliquot_id_to_aliquot_run_metadata_id_tsv = path.join( output_dir, 'Aliquot.aliquot_run_metadata_id.tsv' )

aliquot_id_to_study_id_output_tsv = path.join( output_dir, 'Aliquot.study_id.tsv' )

aliquot_id_to_case_id_output_tsv = path.join( output_dir, 'Aliquot.case_id.tsv' )

aliquot_id_to_project_id_output_tsv = path.join( output_dir, 'Aliquot.project_id.tsv' )

log_dir = path.join( output_root, '__filtration_logs' )

sample_overlap_filtration_log = path.join( log_dir, 'Aliquot.same_study_sample_submitter_ID_duplicates.removed_ids.txt' )

input_files_to_scan = {
    'CasesSamplesAliquots_Aliquot': aliquot_input_tsv,
    'Biospecimen': biospecimen_input_tsv
}

fields_to_ignore = {
    'CasesSamplesAliquots_Aliquot': {
        'sample_id',
        'sample_submitter_id',
        'case_id',
        'case_submitter_id'
    },
    'Biospecimen': {
        'aliquot_status',
        'sample_status',
        'sample_type',
        'case_is_ref',
        'case_status',
        'disease_type',
        'primary_site',
        'sample_id',
        'sample_submitter_id',
        'case_id',
        'case_submitter_id',
        'project_name'
    }
}

values_to_ignore = {
    'N/A',
    'Not Reported'
}

processing_order = [
    'CasesSamplesAliquots_Aliquot',
    'Biospecimen'
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

# Load the map between aliquot_id and aliquot_run_metadata_id (for ISB-CGC downstream consumption only;
# we don't use this association and we don't load AliquotRunMetadata records).

aliquot_id_to_aliquot_run_metadata_id = map_columns_one_to_many( aliquot_aliquot_run_metadata_input_tsv, 'aliquot_id', 'aliquot_run_metadata_id' )

# Load the map between aliquot_id and case_id, from fileMetadata(): file_id <-> aliquot_id <-> case_id
# (noted in ingest policy document).

aliquot_id_to_case_id = map_columns_one_to_one( file_metadata_aliquot_input_tsv, 'aliquot_id', 'case_id' )

# Load the main Aliquot data.

aliquots = dict()

aliquot_fields = list()

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

            if current_id not in aliquots:
                
                aliquots[current_id] = dict()

            if id_field_name not in aliquot_fields:
                
                aliquot_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name not in aliquot_fields:
                        
                        aliquot_fields.append( current_column_name )

                    # If we scanned this before,

                    if current_column_name in aliquots[current_id]:
                        
                        # If the current value isn't null or set to be skipped,

                        if current_value != '' and current_value not in values_to_ignore:
                            
                            # If the old value isn't null,

                            if aliquots[current_id][current_column_name] != '':
                                
                                # Make sure the two values match.

                                if current_value != aliquots[current_id][current_column_name]:
                                    
                                    sys.exit(f"FATAL: Multiple non-null values ('{aliquots[current_id][current_column_name]}', '{current_value}') seen for field '{current_column_name}', {id_field_name} '{current_id}'. Aborting.\n")

                            # If the old value IS null,

                            else:
                                
                                # Overwrite it with the new one.

                                aliquots[current_id][current_column_name] = current_value

                        # ( If we've scanned this before and the current value is null, do nothing. )

                    else:
                        
                        # If we haven't scanned this yet,

                        if current_value != '' and current_value not in values_to_ignore:
                            
                            # If the current value isn't null or set to be skipped, record it.

                            aliquots[current_id][current_column_name] = current_value

                        else:
                            
                            # If the current value IS null, save an empty string.

                            aliquots[current_id][current_column_name] = ''

# Load the map from Sample submitter ID to Study ID. submitter_id gets duplicated across Samples and Aliquots;
# we ignore any Aliquot records whose submitter_id matches the submitter_id of any Sample in the same
# Study (see ingest policy document for rationale).

sample_id_to_submitter_id = map_columns_one_to_one( sample_input_tsv, 'sample_id', 'sample_submitter_id' )

sample_id_to_study_id = map_columns_one_to_one( sample_study_input_tsv, 'sample_id', 'study_id' )

sample_submitter_id_to_study_id = dict()

for sample_id in sample_id_to_submitter_id:
    
    sample_submitter_id = sample_id_to_submitter_id[sample_id]

    study_id = sample_id_to_study_id[sample_id]

    if sample_submitter_id not in sample_submitter_id_to_study_id:
        
        sample_submitter_id_to_study_id[sample_submitter_id] = set()

    sample_submitter_id_to_study_id[sample_submitter_id].add(study_id)

# Write the main Aliquot table. Keep track of removed records by ID.

removed_overlaps = set()

with open( aliquot_output_tsv, 'w' ) as OUT, open( aliquot_id_to_aliquot_run_metadata_id_tsv, 'w' ) as ARM:
    
    # Header rows.

    print( *aliquot_fields, sep='\t', end='\n', file=OUT )
    print( *[ 'aliquot_id', 'aliquot_run_metadata_id' ], sep='\t', end='\n', file=ARM )

    # Data rows.

    for current_id in sorted( aliquots ):
        
        aliquot_submitter_id = aliquots[current_id]['aliquot_submitter_id']

        conflict_clear = True

        # We want this to break with a KeyError if the target doesn't exist, because it should exist.

        for pdc_study_id in aliquot_id_to_pdc_study_id[current_id]:
            
            aliquot_study_id = pdc_study_id_to_study_id[pdc_study_id]

            # Filter duplicate study_id+submitter_id pairs.

            if aliquot_submitter_id in sample_submitter_id_to_study_id and aliquot_study_id in sample_submitter_id_to_study_id[aliquot_submitter_id]:
                
                conflict_clear = False

        if conflict_clear:
            
            output_row = [ current_id ] + [ aliquots[current_id][field_name] for field_name in aliquot_fields[1:] ]

            print( *output_row, sep='\t', end='\n', file=OUT )

            if current_id in aliquot_id_to_aliquot_run_metadata_id:
                
                for arm_id in sorted( aliquot_id_to_aliquot_run_metadata_id[current_id] ):
                    
                    print( *[ current_id, arm_id ], sep='\t', end='\n', file=ARM )

        else:
            
            removed_overlaps.add( current_id )

# Log the removed Aliquot records by aliquot_id.

with open( sample_overlap_filtration_log, 'w' ) as OUT:
    
    for aliquot_id in sorted( removed_overlaps ):
        
        print( aliquot_id, file=OUT )

# Filter the File->aliquot_id map to remove banned IDs.

with open( file_aliquot_output_tsv ) as IN, open( path.join( output_root, 'temp' ), 'w' ) as OUT:
    
    header = next(IN).rstrip('\n')

    print( header, end='\n', file=OUT )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        [ file_id, aliquot_id ] = line.split('\t')

        if aliquot_id not in removed_overlaps:
            
            print( *[ file_id, aliquot_id ], sep='\t', end='\n', file=OUT )

rename( path.join( output_root, 'temp' ), file_aliquot_output_tsv )

# Filter the Sample->aliquot_id map to remove banned IDs.

with open( sample_aliquot_output_tsv ) as IN, open( path.join( output_root, 'temp' ), 'w' ) as OUT:
    
    header = next(IN).rstrip('\n')

    print( header, end='\n', file=OUT )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        [ sample_id, aliquot_id ] = line.split('\t')

        if aliquot_id not in removed_overlaps:
            
            print( *[ sample_id, aliquot_id ], sep='\t', end='\n', file=OUT )

rename( path.join( output_root, 'temp' ), sample_aliquot_output_tsv )

# Write the map between aliquot_id and study_id and the map between aliquot_id and project_id.

printed_aliquot_project = dict()

with open( aliquot_id_to_study_id_output_tsv, 'w' ) as ALIQUOT_STUDY, open( aliquot_id_to_project_id_output_tsv, 'w' ) as ALIQUOT_PROJECT:
    
    # Header rows.

    print( *[ 'aliquot_id', 'study_id' ], sep='\t', end='\n', file=ALIQUOT_STUDY )
    print( *[ 'aliquot_id', 'project_id' ], sep='\t', end='\n', file=ALIQUOT_PROJECT )

    # Data rows.

    for aliquot_id in sorted( aliquot_id_to_pdc_study_id ):
        
        if aliquot_id not in removed_overlaps:
            
            for pdc_study_id in sorted( aliquot_id_to_pdc_study_id[aliquot_id] ):
                
                # We want this to break with a KeyError if the target doesn't exist, because it should exist.

                study_id = pdc_study_id_to_study_id[pdc_study_id]

                print( *[ aliquot_id, study_id ], sep='\t', end='\n', file=ALIQUOT_STUDY )

                project_id = study_id_to_project_id[study_id]

                if aliquot_id not in printed_aliquot_project:
                    
                    printed_aliquot_project[aliquot_id] = project_id

                    print( *[ aliquot_id, project_id ], sep='\t', end='\n', file=ALIQUOT_PROJECT )

# Write the map between aliquot_id and case_id.

with open( aliquot_id_to_case_id_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *[ 'aliquot_id', 'case_id' ], sep='\t', end='\n', file=OUT )

    # Data rows.

    for aliquot_id in sorted( aliquot_id_to_case_id ):
        
        if aliquot_id in aliquots and aliquot_id not in removed_overlaps:
            
            print( *[ aliquot_id, aliquot_id_to_case_id[aliquot_id] ], sep='\t', end='\n', file=OUT )



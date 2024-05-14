#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc'

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

arm_input_tsv = path.join( input_root, 'CasesSamplesAliquots', 'AliquotRunMetadata.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

aliquot_input_tsv = path.join( output_root, 'Aliquot', 'Aliquot.tsv' )

aliquot_study_input_tsv = path.join( output_root, 'Aliquot', 'Aliquot.study_id.tsv' )

srm_arm_input_tsv = path.join( output_root, 'StudyRunMetadata', 'StudyRunMetadata.aliquot_run_metadata_id.tsv' )

srm_study_input_tsv = path.join( output_root, 'StudyRunMetadata', 'StudyRunMetadata.study_id.tsv' )

output_dir = path.join( output_root, 'AliquotRunMetadata' )

aliquot_run_metadata_output_tsv = path.join( output_dir, 'AliquotRunMetadata.tsv' )

aliquot_run_metadata_study_output_tsv = path.join( output_dir, 'AliquotRunMetadata.study_id.tsv' )

input_files_to_scan = {
    'AliquotRunMetadata': arm_input_tsv,
    'SRM_ARM' : srm_arm_input_tsv,
    'SRM_Study': srm_study_input_tsv
}

fields_to_ignore = {
    'AliquotRunMetadata': {
        # This relationship is stored under Aliquot.
        'aliquot_id'
    }
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main AliquotRunMetadata data.

aliquot_run_metadata = dict()

aliquot_run_metadata_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

# Pass 1: Load AliquotRunMetadata records.

with open( input_files_to_scan['AliquotRunMetadata'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for i in range( 0, len( column_names ) ):
        
        column_name = column_names[i]

        if column_name not in fields_to_ignore['AliquotRunMetadata']:
            
            aliquot_run_metadata_fields.append( column_name )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        values = line.split( '\t' )

        id_field_name = column_names[0]

        current_id = values[0]

        if current_id not in aliquot_run_metadata:
            
            aliquot_run_metadata[current_id] = dict()

        for i in range( 1, len( column_names ) ):
            
            current_column_name = column_names[i]

            current_value = values[i]

            if current_column_name not in fields_to_ignore['AliquotRunMetadata']:
                
                if current_value != '':
                    
                    # If the current value isn't null, record it.

                    aliquot_run_metadata[current_id][current_column_name] = current_value

                else:
                    
                    # If the current value IS null, save an empty string.

                    aliquot_run_metadata[current_id][current_column_name] = ''

# Pass 2: Load ARM<->SRM relationships.

arm_to_srm = dict()

with open( input_files_to_scan['SRM_ARM'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        ( srm_id, arm_id ) = line.split( '\t' )

        if arm_id not in aliquot_run_metadata:
            
            sys.exit( f"FATAL: AliquotRunMetadata ID {arm_id} was not loaded from the main table; aborting, please fix." )

        if arm_id not in arm_to_srm:
            
            arm_to_srm[arm_id] = set()

        arm_to_srm[arm_id].add( srm_id )

# Pass 3: Load SRM->Study relationships.

srm_to_study = dict()

with open( input_files_to_scan['SRM_Study'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        ( srm_id, study_id ) = line.split( '\t' )

        if srm_id not in srm_to_study:
            
            srm_to_study[srm_id] = set()

        srm_to_study[srm_id].add( study_id )

# Pass 4: Transitively compute ARM->Study relationships.

arm_to_study = dict()

# As written, this will link no Studies to ARM objects that aren't tied to SRM objects.
# Right now, 10 ARM objects are unlinked.

for arm_id in arm_to_srm:
    
    arm_to_study[arm_id] = set()

    for srm_id in arm_to_srm[arm_id]:
        
        for study_id in srm_to_study[srm_id]:
            
            arm_to_study[arm_id].add( study_id )

# Pass 5: Find the missing Study links, via Aliquot submitter IDs.

# ...so help me, I have to hard-code this because it's ambigous -- the associated aliquot_submitter_id is 'Internal Reference - Pooled Sample', which occurs twice across two different sets of Studies.

arm_to_study['7b3d5f77-6630-11e8-bcf1-0a2705229b82'] = {
    'b91a0a02-f3a0-11ea-b1fd-0aad30af8a83',
    'b91a0e5f-f3a0-11ea-b1fd-0aad30af8a83',
    'b91a0119-f3a0-11ea-b1fd-0aad30af8a83'
}

aliquot_submitter_id_to_aliquot_id = map_columns_one_to_many( aliquot_input_tsv, 'aliquot_submitter_id', 'aliquot_id' )

aliquot_id_to_study_id = map_columns_one_to_many( aliquot_study_input_tsv, 'aliquot_id', 'study_id' )

patched_count = 0

for arm_id in aliquot_run_metadata:
    
    if arm_id not in arm_to_study:
        
        arm_to_study[arm_id] = set()

        aliquot_submitter_id = aliquot_run_metadata[arm_id]['aliquot_submitter_id']

        if len( aliquot_submitter_id_to_aliquot_id[aliquot_submitter_id] ) == 1:
            
            aliquot_id = sorted( aliquot_submitter_id_to_aliquot_id[aliquot_submitter_id] )[0]

            for study_id in aliquot_id_to_study_id[aliquot_id]:
                
                arm_to_study[arm_id].add( study_id )

            patched_count = patched_count + 1

        else:
            
            sys.exit( f"FATAL (for now): ARM ID {arm_id} is linked to aliquot_submitter_id {aliquot_submitter_id} which is linked to more than one aliquot_id: cannot resolve ARM->Study association. Please compensate." )

print( f"Added {patched_count} new Study links to ARM records. Association now complete.", file=sys.stderr )

# Write the main AliquotRunMetadata table.

with open( aliquot_run_metadata_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *aliquot_run_metadata_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( aliquot_run_metadata ):
        
        output_row = [ current_id ] + [ aliquot_run_metadata[current_id][field_name] for field_name in aliquot_run_metadata_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Sanity check: load Study IDs that exist.

study_id_to_study_submitter_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'study_submitter_id' )

# Write the AliquotRunMetadata->study_id association.

with open( aliquot_run_metadata_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'aliquot_run_metadata_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for aliquot_run_metadata_id in sorted( arm_to_study ):
        
        for study_id in sorted( arm_to_study[aliquot_run_metadata_id] ):
            
            if study_id in study_id_to_study_submitter_id:
                
                print( *[ aliquot_run_metadata_id, study_id ], sep='\t', end='\n', file=OUT )

            else:
                
                print( f"WARNING: Skipping unfound AliquotRunMetadata-associated Study ID '{study_id}' for AliquotRunMetadata ID {aliquot_run_metadata_id}", file=sys.stderr )



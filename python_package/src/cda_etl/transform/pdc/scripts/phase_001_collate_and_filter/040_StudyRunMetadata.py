#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc'

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

srm_input_tsv = path.join( input_root, 'ExperimentalMetadata', 'StudyRunMetadata.tsv' )

srm_study_input_tsv = path.join( input_root, 'ExperimentalMetadata', 'ExperimentalMetadata.study_run_metadata.tsv' )

srm_arm_aux_input_tsv = path.join( input_root, 'ExperimentalMetadata', 'StudyRunMetadata.aliquot_run_metadata.tsv' )

sed_main_input_tsv = path.join( input_root, 'StudyExperimentalDesign', 'StudyExperimentalDesign.tsv' )

sed_aux_input_tsv = path.join( input_root, 'StudyExperimentalDesign', 'StudyExperimentalDesign.LabelAliquots.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'StudyRunMetadata' )

study_run_metadata_output_tsv = path.join( output_dir, 'StudyRunMetadata.tsv' )

study_run_metadata_study_output_tsv = path.join( output_dir, 'StudyRunMetadata.study_id.tsv' )

study_run_metadata_aliquot_run_metadata_output_tsv = path.join( output_dir, 'StudyRunMetadata.aliquot_run_metadata_id.tsv' )

input_files_to_scan = {
    'StudyRunMetadata': srm_input_tsv,
    'StudyExperimentalDesign': sed_main_input_tsv,
    'SRM_Study': srm_study_input_tsv,
    'SRM_ARM_main': sed_aux_input_tsv,
    'SRM_ARM_supplemental': srm_arm_aux_input_tsv
}

fields_to_ignore = {
    'StudyRunMetadata': {
        # These are all already bound to related AliquotRunMetadata objects.
        'label_free',
        'itraq_113',
        'itraq_114',
        'itraq_115',
        'itraq_116',
        'itraq_117',
        'itraq_118',
        'itraq_119',
        'itraq_121',
        'tmt_126',
        'tmt_127n',
        'tmt_127c',
        'tmt_128n',
        'tmt_128c',
        'tmt_129n',
        'tmt_129c',
        'tmt_129cc',
        'tmt_130n',
        'tmt_131',
        'tmt_131c'
    }
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main StudyRunMetadata data.

study_run_metadata = dict()

study_run_metadata_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

# Pass 1: Load the bulk of the StudyRunMetadata records.

with open( input_files_to_scan['StudyRunMetadata'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for i in range( 0, len( column_names ) ):
        
        column_name = column_names[i]

        if column_name not in fields_to_ignore['StudyRunMetadata']:
            
            study_run_metadata_fields.append( column_name )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        values = line.split( '\t' )

        id_field_name = column_names[0]

        current_id = values[0]

        if current_id not in study_run_metadata:
            
            study_run_metadata[current_id] = dict()

        for i in range( 1, len( column_names ) ):
            
            current_column_name = column_names[i]

            current_value = values[i]

            if current_column_name not in fields_to_ignore['StudyRunMetadata']:
                
                if current_value != '':
                    
                    # If the current value isn't null, record it.

                    study_run_metadata[current_id][current_column_name] = current_value

                else:
                    
                    # If the current value IS null, save an empty string.

                    study_run_metadata[current_id][current_column_name] = ''

print( f"Loaded {len( study_run_metadata )} records from base StudyRunMetadata table", file=sys.stderr )

# Pass 2: Catch metadata for StudyRunMetadata records that weren't in the
# table fetched via experimentalMetadata(). Note: the table we're loading
# from is an amalgam of joined data from several other tables and does
# not contain all the StudyRunMetadata fields, so any records loaded from
# this table will necessarily have nulls for fields not present
# (at time of writing, these are `folder_name`, `fraction`, `date`,
# `alias` and `replicate_number`, plus the redundant ones we skipped
# via fields_to_ignore['StudyRunMetadata'].

new_record_count = 0

# Track SRM->Study relationships.

srm_to_study = dict()

with open( input_files_to_scan['StudyExperimentalDesign'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        id_field_name = column_names[0]

        current_id = record[id_field_name]

        # Thing 1: If this is a new StudyRunMetadata record, add it to the main set.

        if current_id not in study_run_metadata:
            
            new_record_count = new_record_count + 1

            study_run_metadata[current_id] = dict()

            for field_name in study_run_metadata_fields:
                
                if field_name not in record:
                    
                    study_run_metadata[current_id][field_name] = ''

                else:
                    
                    study_run_metadata[current_id][field_name] = record[field_name]

        else:
            
            # Thing 1a: If the ID isn't new, see if there's any new metadata.

            for field_name in study_run_metadata_fields:
                
                if field_name in record and record[field_name] != '' and field_name != id_field_name and study_run_metadata[current_id][field_name] == '':
                    
                    study_run_metadata[current_id][field_name] = record[field_name]

        # Thing 2: Populate the SRM->Study relationship.

        if current_id not in srm_to_study:
            
            srm_to_study[current_id] = set()

        srm_to_study[current_id].add( record['study_id'] )

print( f"Loaded {new_record_count} new (partial!) StudyRunMetadata records from StudyExperimentalDesign table", file=sys.stderr )

# Pass 3: Complete the SRM->Study relationship.

with open( input_files_to_scan['SRM_Study'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        srm_id = record['study_run_metadata_id']

        study_id = record['study_id']

        if srm_id not in srm_to_study:
            
            srm_to_study[srm_id] = set()

        srm_to_study[srm_id].add( study_id )

# Pass 4: Initialize the SRM->ARM relationship data.

srm_to_arm = dict()

with open( input_files_to_scan['SRM_ARM_main'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        srm_id = record['StudyExperimentalDesign.study_run_metadata_id']

        arm_id = record['aliquot_run_metadata_id']

        if srm_id not in srm_to_arm:
            
            srm_to_arm[srm_id] = set()

        srm_to_arm[srm_id].add( arm_id )

# Pass 5: Complete the SRM->ARM relationship data.

with open( input_files_to_scan['SRM_ARM_supplemental'] ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        srm_id = record['study_run_metadata_id']

        arm_id = record['aliquot_run_metadata_id']

        if srm_id not in srm_to_arm:
            
            srm_to_arm[srm_id] = set()

        srm_to_arm[srm_id].add( arm_id )

# Write the main StudyRunMetadata table.

with open( study_run_metadata_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *study_run_metadata_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( study_run_metadata ):
        
        output_row = [ current_id ] + [ study_run_metadata[current_id][field_name] for field_name in study_run_metadata_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Sanity check: load Study IDs that exist.

study_id_to_study_submitter_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'study_submitter_id' )

# Write the StudyRunMetadata->study_id association.

with open( study_run_metadata_study_output_tsv, 'w' ) as OUT:
    
    print( *[ 'study_run_metadata_id', 'study_id' ], sep='\t', end='\n', file=OUT )

    for study_run_metadata_id in sorted( srm_to_study ):
        
        for study_id in sorted( srm_to_study[study_run_metadata_id] ):
            
            if study_id in study_id_to_study_submitter_id:
                
                print( *[ study_run_metadata_id, study_id ], sep='\t', end='\n', file=OUT )

            else:
                
                print( f"WARNING: Skipping unfound StudyRunMetadata-associated Study ID '{study_id}' for StudyRunMetadata ID {study_run_metadata_id}", file=sys.stderr )

# Write the StudyRunMetadata->AliquotRunMetadata association.

with open( study_run_metadata_aliquot_run_metadata_output_tsv, 'w' ) as OUT:
    
    print( *[ 'study_run_metadata_id', 'aliquot_run_metadata_id' ], sep='\t', end='\n', file=OUT )

    for study_run_metadata_id in sorted( srm_to_arm ):
        
        for aliquot_run_metadata_id in sorted( srm_to_arm[study_run_metadata_id] ):
            
            print( *[ study_run_metadata_id, aliquot_run_metadata_id ], sep='\t', end='\n', file=OUT )



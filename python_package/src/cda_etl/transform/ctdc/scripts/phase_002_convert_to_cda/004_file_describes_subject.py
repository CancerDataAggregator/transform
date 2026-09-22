#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CTDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated.
tsv_input_root = path.join( 'extracted_data', f"{upstream_data_source.lower()}_postprocessed" )
data_file_participant_input_tsv = path.join( tsv_input_root, 'DataFile', 'DataFile.participant_id.tsv' )

# CDA TSVs.
tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )
upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )
file_describes_subject_output_tsv = path.join( tsv_output_root, 'file_describes_subject.tsv' )

# EXECUTION

# Load CDA subject IDs for participant_id values.
participant_id_to_cda_subject_id = dict()

with open( upstream_identifiers_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )
    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )
        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'Participant.participant_id':
            participant_id_to_cda_subject_id[value] = entity_id

# Load associations between DataFiles and Participants.
data_file_uuid_participant_id = map_columns_one_to_many( data_file_participant_input_tsv, 'data_file_uuid', 'participant_id' )

# Map that association to CDA.
cda_file_describes_subject = dict()

for file_id in data_file_uuid_participant_id:
    for participant_id in data_file_uuid_participant_id[file_id]:
        cda_subject_id = participant_id_to_cda_subject_id[participant_id]
        if file_id not in cda_file_describes_subject:
            cda_file_describes_subject[file_id] = set()
        cda_file_describes_subject[file_id].add( cda_subject_id )

print( f"[{get_current_timestamp()}] Writing {file_describes_subject_output_tsv}...", end='', file=sys.stderr )

# Write the output map TSV.
with open( file_describes_subject_output_tsv, 'w' ) as OUT:
    print( *[ 'file_id', 'subject_id' ], sep='\t', file=OUT )
    for file_id in sorted( cda_file_describes_subject ):
        for subject_id in sorted( cda_file_describes_subject[file_id] ):
            print( *[ file_id, subject_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CDS'

# Unmodified extracted data, converted directly from a Neo4j JSONL dump.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

treatment_input_tsv = path.join( tsv_input_root, 'treatment.tsv' )

treatment_of_participant_input_tsv = path.join( tsv_input_root, 'treatment_of_participant.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

treatment_output_tsv = path.join( tsv_output_root, 'treatment.tsv' )

# Table header sequences.

cda_treatment_fields = [
    
    # Note: this `id` will not survive aggregation: final versions of CDA
    # treatment records are keyed only by id_alias, which is generated
    # only for aggregated data and will replace `id` here.

    'id',
    'subject_id',
    'anatomic_site',
    'type',
    'therapeutic_agent'
]

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

# Load CDA subject IDs for participant_uuid values and vice versa.

participant_uuid_to_cda_subject_id = dict()

original_participant_uuids = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'participant.uuid':
            
            participant_uuid = value

            subject_id = entity_id

            participant_uuid_to_cda_subject_id[participant_uuid] = subject_id

            if subject_id not in original_participant_uuids:
                
                original_participant_uuids[subject_id] = set()

            original_participant_uuids[subject_id].add( participant_uuid )

# Load (CDA) treatment.therapeutic_agent from (CDS) treatment.therapeutic_agents and create a CDA treatment record for each CDS treatment record.

print( f"[{get_current_timestamp()}] Loading (CDA) treatment.therapeutic_agent metadata from (CDS) treatment.therapeutic_agents...", end='', file=sys.stderr )

participant_treatment = map_columns_one_to_many( treatment_of_participant_input_tsv, 'participant_uuid', 'treatment_uuid' )

treatment = load_tsv_as_dict( treatment_input_tsv )

cda_treatment_records = dict()

for subject_id in original_participant_uuids:
    
    therapeutic_agents = set()

    for participant_uuid in original_participant_uuids[subject_id]:
        
        if participant_uuid in participant_treatment:
            
            for treatment_uuid in participant_treatment[participant_uuid]:
                
                new_value = treatment[treatment_uuid]['therapeutic_agents']

                if new_value != '':
                    
                    therapeutic_agents.add( new_value )

    i = 0

    for therapeutic_agent in sorted( therapeutic_agents ):
        
        treatment_id = f"{upstream_data_source}.{subject_id}.therapeutic_agents.{i}"

        i = i + 1

        cda_treatment_records[treatment_id] = {
            
            'id': treatment_id,
            'subject_id': subject_id,
            'anatomic_site': '',
            'type': '',
            'therapeutic_agent': therapeutic_agent
        }

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSV to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA treatment records.

with open( treatment_output_tsv, 'w' ) as OUT:
    
    print( *cda_treatment_fields, sep='\t', file=OUT )

    for treatment_id in sorted( cda_treatment_records ):
        
        output_row = list()

        treatment_record = cda_treatment_records[treatment_id]

        for cda_treatment_field in cda_treatment_fields:
            
            output_row.append( treatment_record[cda_treatment_field] )

        print( *output_row, sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



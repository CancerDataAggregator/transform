#!/usr/bin/env python3 -u

import json, re, sys

from os import path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CTDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated.
tsv_input_root = path.join( 'extracted_data', f"{upstream_data_source.lower()}_postprocessed" )

study_participant_input_tsv = path.join( tsv_input_root, 'Study', 'Study.participant_id.tsv' )
# At time of writing (2026-09), no Participant has more than one Demographic record. If future clashes are observed, a trap is set.
participant_demographic_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.demographic_record_id.tsv' )
demographic_input_tsv = path.join( tsv_input_root, 'Demographic', 'Demographic.tsv' )
participant_status_input_tsv = path.join( tsv_input_root, 'ParticipantStatus', 'ParticipantStatus.tsv' )
participant_participant_status_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.participant_status_record_id.tsv' )

# CDA TSVs.
tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )
upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )
project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )
subject_output_tsv = path.join( tsv_output_root, 'subject.tsv' )
subject_in_project_output_tsv = path.join( tsv_output_root, 'subject_in_project.tsv' )

# Table header sequences.
cda_subject_fields = [
    'id',
    'crdc_id',
    'species',
    'year_of_birth',
    'year_of_death',
    'cause_of_death',
    'race',
    'ethnicity'
]

upstream_identifiers_fields = [
    'cda_table',
    'id',
    'upstream_source',
    'upstream_field',
    'upstream_id'
]

# EXECUTION

print( f"[{get_current_timestamp()}] Computing subject identifier metadata and loading Study->Participant associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Get submitter IDs.

# Load CDA IDs for studies and programs. Don't use any of the canned loader
# functions to load this data structure; it's got multiplicities that are
# generally mishandled if certain keys are assumed to be unique (e.g. a CDA
# subject can have multiple case_id values from the same data source, rendering any attempt
# to key this information on the first X columns incorrect or so cumbersome as to
# be pointless).

upstream_identifiers = dict()

with open( upstream_identifiers_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table not in upstream_identifiers:
            upstream_identifiers[cda_table] = dict()

        if entity_id not in upstream_identifiers[cda_table]:
            upstream_identifiers[cda_table][entity_id] = dict()

        if data_source not in upstream_identifiers[cda_table][entity_id]:
            upstream_identifiers[cda_table][entity_id][data_source] = dict()

        if source_field not in upstream_identifiers[cda_table][entity_id][data_source]:
            upstream_identifiers[cda_table][entity_id][data_source][source_field] = set()

        upstream_identifiers[cda_table][entity_id][data_source][source_field].add( value )

cda_project_id = dict()

for project_id in upstream_identifiers['project']:
    # Some of these are from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.
    if upstream_data_source in upstream_identifiers['project'][project_id]:
        if 'Study.study_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Study.study_id']:
                if value not in cda_project_id:
                    cda_project_id[value] = project_id
                elif cda_project_id[value] != project_id:
                    sys.exit( f"FATAL: {upstream_data_source} Study.study_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project record metadata and inter-project containment.
cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Create CDA subject records and associate them with CDA project records.
participant_study = map_columns_one_to_many( study_participant_input_tsv, 'participant_id', 'study_id' )

cda_subject_id = dict()
original_participant_id = dict()
cda_subject_in_project = dict()

for participant_id in participant_study:
    for study_id in participant_study[participant_id]:
        new_cda_subject_id = f"{study_id}.{participant_id}"

        if participant_id in cda_subject_id and cda_subject_id[participant_id] != new_cda_subject_id:
            sys.exit( f"FATAL: participant_id {participant_id} unexpectedly assigned to both CDA subject IDs {cda_subject_id[participant_id]} and {new_cda_subject_id}; cannot continue, aborting." )
        cda_subject_id[participant_id] = new_cda_subject_id

        # Track original participant_ids for CDA subjects.
        if new_cda_subject_id not in original_participant_id:
            original_participant_id[new_cda_subject_id] = set()
        original_participant_id[new_cda_subject_id].add( participant_id )

        if new_cda_subject_id not in cda_subject_in_project:
            cda_subject_in_project[new_cda_subject_id] = set()
        cda_subject_in_project[new_cda_subject_id].add( cda_project_id[study_id] )

        for ancestor_project_id in get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] ):
            cda_subject_in_project[new_cda_subject_id].add( ancestor_project_id )

        # Record upstream participant_id values for CDA subjects.
        if 'subject' not in upstream_identifiers:
            upstream_identifiers['subject'] = dict()

        if new_cda_subject_id not in upstream_identifiers['subject']:
            upstream_identifiers['subject'][new_cda_subject_id] = dict()

        if upstream_data_source not in upstream_identifiers['subject'][new_cda_subject_id]:
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source] = dict()

        if 'Participant.participant_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['Participant.participant_id'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['Participant.participant_id'].add( participant_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} Participant and Study metadata for CDA subject records...", end='', file=sys.stderr )

# Load ParticipantStatus and Demographic metadata and create CDA subject records.
demographic = load_tsv_as_dict( demographic_input_tsv )
participant_demographic = map_columns_one_to_many( participant_demographic_input_tsv, 'participant_id', 'demographic_record_id' )
participant_status = load_tsv_as_dict( participant_status_input_tsv )
participant_participant_status = map_columns_one_to_many( participant_participant_status_input_tsv, 'participant_id', 'participant_status_record_id' )

cda_subject_records = dict()

for subject_id in sorted( original_participant_id ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = ''
    cda_subject_records[subject_id]['year_of_birth'] = ''
    cda_subject_records[subject_id]['year_of_death'] = ''
    cda_subject_records[subject_id]['cause_of_death'] = ''
    cda_subject_records[subject_id]['race'] = ''
    cda_subject_records[subject_id]['ethnicity'] = ''

    for participant_id in sorted( original_participant_id[subject_id] ):
        
        if participant_id in participant_participant_status:
            for participant_status_record_id in participant_participant_status[participant_id]:
                if participant_status[participant_status_record_id]['primary_cause_of_death'] is not None and participant_status[participant_status_record_id]['primary_cause_of_death'] != '':
                    if cda_subject_record[subject_id]['cause_of_death'] == '' or cda_subject_record[subject_id]['cause_of_death'] == participant_status[participant_status_record_id]['primary_cause_of_death']:
                        cda_subject_records[subject_id]['cause_of_death'] = participant_status[participant_status_record_id]['primary_cause_of_death']
                    else:
                        sys.exit( f"FATAL: ParticipantStatus records give conflicting 'primary_cause_of_death' values for CDA subject ID '{subject_id}' (current participant_id '{participant_id}'); cannot continue, please handle." )

        if participant_id in participant_demographic:
            for demographic_record_id in sorted( participant_demographic[participant_id] ):
                for input_field_name in [ 'ncbi_taxonomy_name', 'race', 'ethnicity' ]:
                    if demographic[demographic_record_id][input_field_name] not in { '', '[]' }:
                        current_value = demographic[demographic_record_id][input_field_name]
                        output_field_name = 'species' if input_field_name == 'ncbi_taxonomy_name' else input_field_name
                        if cda_subject_records[subject_id][output_field_name] == '':
                            cda_subject_records[subject_id][output_field_name] = current_value
                        elif cda_subject_records[subject_id][output_field_name] != current_value:
                            # Complain. This violates assumptions which held at time of development: build a conflict-logger and handle clashes explicitly if they arise in the future.
                            sys.exit( "FATAL: Demographic.{input_field_name} metadata clash for CDA subject ID '{subject_id}', participant_id '{participant_id}': loaded '{cda_subject_records[subject_id][output_field_name]}', now seeing '{current_value}'; cannot continue, please handle." )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA subject records.
with open( subject_output_tsv, 'w' ) as OUT:
    print( *cda_subject_fields, sep='\t', file=OUT )
    for subject_id in sorted( cda_subject_records ):
        output_row = list()
        subject_record = cda_subject_records[subject_id]
        for cda_subject_field in cda_subject_fields:
            output_row.append( subject_record[cda_subject_field] )
        print( *output_row, sep='\t', file=OUT )

# Write the subject<->project association.
with open( subject_in_project_output_tsv, 'w' ) as OUT:
    print( *[ 'subject_id', 'project_id' ], sep='\t', file=OUT )
    for subject_id in sorted( cda_subject_in_project ):
        for project_id in sorted( cda_subject_in_project[subject_id] ):
            print( *[ subject_id, project_id ], sep='\t', file=OUT )

# Update upstream_identifiers.

# upstream_identifiers_fields = [
#     
#     'cda_table',
#     'id',
#     'upstream_source',
#     'upstream_field',
#     'upstream_id'
# ]

# upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['Participant.participant_id'].add( participant_id] )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    print( *upstream_identifiers_fields, sep='\t', file=OUT )
    for cda_table in sorted( upstream_identifiers ):
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



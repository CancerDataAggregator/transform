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

participant_non_targeted_therapy_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.non_targeted_therapy_record_id.tsv' )
participant_radiotherapy_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.radiological_procedure_record_id.tsv' )
participant_surgery_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.surgical_procedure_record_id.tsv' )
participant_targeted_therapy_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.targeted_therapy_record_id.tsv' )

non_targeted_therapy_input_tsv = path.join( tsv_input_root, 'NonTargetedTherapy', 'NonTargetedTherapy.tsv' )
radiotherapy_input_tsv = path.join( tsv_input_root, 'Radiotherapy', 'Radiotherapy.tsv' )
surgery_input_tsv = path.join( tsv_input_root, 'Surgery', 'Surgery.tsv' )
targeted_therapy_input_tsv = path.join( tsv_input_root, 'TargetedTherapy', 'TargetedTherapy.tsv' )

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

# EXECUTION

# Load CDA subject IDs for participant_id values and vice versa.
participant_id_to_cda_subject_id = dict()
original_participant_ids = dict()

with open( upstream_identifiers_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )
    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )
        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'Participant.participant_id':
            participant_id = value
            subject_id = entity_id
            participant_id_to_cda_subject_id[participant_id] = subject_id
            if subject_id not in original_participant_ids:
                original_participant_ids[subject_id] = set()
            original_participant_ids[subject_id].add( participant_id )

cda_treatment_records = dict()

# Load NonTargetedTherapy.non_targeted_therapy -> treatment.therapeutic_agent and create CDA treatment records accordingly.
print( f"[{get_current_timestamp()}] Loading treatment metadata from NonTargetedTherapy.non_targeted_therapy...", end='', file=sys.stderr )

participant_non_targeted_therapy = map_columns_one_to_many( participant_non_targeted_therapy_input_tsv, 'participant_id', 'non_targeted_therapy_record_id' )
non_targeted_therapy = load_tsv_as_dict( non_targeted_therapy_input_tsv )

non_targeted_therapy_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_non_targeted_therapy:
            for non_targeted_therapy_record_id in sorted( participant_non_targeted_therapy[participant_id] ):
                if non_targeted_therapy[non_targeted_therapy_record_id]['non_targeted_therapy'] is not None and non_targeted_therapy[non_targeted_therapy_record_id]['non_targeted_therapy'] != '':
                    # Make a new treatment record with just this info.
                    cda_treatment_records[f"{upstream_data_source}.{subject_id}.non_targeted_therapy_tmt.{non_targeted_therapy_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.non_targeted_therapy_tmt.{non_targeted_therapy_disambiguator}",
                        'subject_id': subject_id,
                        'anatomic_site': '',
                        'type': 'non-targeted drug therapy',
                        'therapeutic_agent': non_targeted_therapy[non_targeted_therapy_record_id]['non_targeted_therapy']
                    }
                    non_targeted_therapy_disambiguator = non_targeted_therapy_disambiguator + 1

print( 'done.', file=sys.stderr )

# Load TargetedTherapy.targeted_therapy -> treatment.therapeutic_agent and create CDA treatment records accordingly.
print( f"[{get_current_timestamp()}] Loading treatment metadata from TargetedTherapy.targeted_therapy...", end='', file=sys.stderr )

participant_targeted_therapy = map_columns_one_to_many( participant_targeted_therapy_input_tsv, 'participant_id', 'targeted_therapy_record_id' )
targeted_therapy = load_tsv_as_dict( targeted_therapy_input_tsv )

targeted_therapy_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_targeted_therapy:
            for targeted_therapy_record_id in sorted( participant_targeted_therapy[participant_id] ):
                if targeted_therapy[targeted_therapy_record_id]['targeted_therapy'] is not None and targeted_therapy[targeted_therapy_record_id]['targeted_therapy'] != '':
                    # Make a new treatment record with just this info.
                    cda_treatment_records[f"{upstream_data_source}.{subject_id}.targeted_therapy_tmt.{targeted_therapy_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.targeted_therapy_tmt.{targeted_therapy_disambiguator}",
                        'subject_id': subject_id,
                        'anatomic_site': '',
                        'type': 'targeted drug therapy',
                        'therapeutic_agent': targeted_therapy[targeted_therapy_record_id]['targeted_therapy']
                    }
                    targeted_therapy_disambiguator = targeted_therapy_disambiguator + 1

print( 'done.', file=sys.stderr )

# Make CDA treatment records from Radiotherapy records.
print( f"[{get_current_timestamp()}] Creating treatment records from Radiotherapy records...", end='', file=sys.stderr )

participant_radiotherapy = map_columns_one_to_many( participant_radiotherapy_input_tsv, 'participant_id', 'radiological_procedure_record_id' )
radiotherapy = load_tsv_as_dict( radiotherapy_input_tsv )

radiotherapy_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_radiotherapy:
            for radiological_procedure_record_id in sorted( participant_radiotherapy[participant_id] ):
                if \
                    ( radiotherapy[radiological_procedure_record_id]['radiological_procedure_anatomical_location'] is not None and radiotherapy[radiological_procedure_record_id]['radiological_procedure_anatomical_location'] != '' ) or \
                    ( radiotherapy[radiological_procedure_record_id]['radiological_procedure'] is not None and radiotherapy[radiological_procedure_record_id]['radiological_procedure'] != '' ):
                    # Make a new treatment record with Radiotherapy info.
                    radiological_procedure_anatomical_location_value = ''
                    if radiotherapy[radiological_procedure_record_id]['radiological_procedure_anatomical_location'] is not None and radiotherapy[radiological_procedure_record_id]['radiological_procedure_anatomical_location'] != '':
                        radiological_procedure_anatomical_location_value = radiotherapy[radiological_procedure_record_id]['radiological_procedure_anatomical_location']
                    radiological_procedure_value = ''
                    if radiotherapy[radiological_procedure_record_id]['radiological_procedure'] is not None and radiotherapy[radiological_procedure_record_id]['radiological_procedure'] != '':
                        radiological_procedure_value = radiotherapy[radiological_procedure_record_id]['radiological_procedure']
                    cda_treatment_records[f"{upstream_data_source}.{subject_id}.radiotherapy_tmt.{radiotherapy_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.radiotherapy_tmt.{radiotherapy_disambiguator}",
                        'subject_id': subject_id,
                        'anatomic_site': radiological_procedure_anatomical_location_value,
                        'type': radiological_procedure_value,
                        'therapeutic_agent': ''
                    }
                    radiotherapy_disambiguator = radiotherapy_disambiguator + 1

print( 'done.', file=sys.stderr )

# Make CDA treatment records from Surgery records.
print( f"[{get_current_timestamp()}] Creating treatment records from Surgery records...", end='', file=sys.stderr )

participant_surgery = map_columns_one_to_many( participant_surgery_input_tsv, 'participant_id', 'surgical_procedure_record_id' )
surgery = load_tsv_as_dict( surgery_input_tsv )

surgery_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_surgery:
            for surgical_procedure_record_id in sorted( participant_surgery[participant_id] ):
                if \
                    ( surgery[surgical_procedure_record_id]['surgical_procedure_anatomical_location'] is not None and surgery[surgical_procedure_record_id]['surgical_procedure_anatomical_location'] != '' ) or \
                    ( surgery[surgical_procedure_record_id]['surgical_procedure'] is not None and surgery[surgical_procedure_record_id]['surgical_procedure'] != '' ):
                    # Make a new treatment record with Surgery info.
                    surgical_procedure_anatomical_location_value = ''
                    if surgery[surgical_procedure_record_id]['surgical_procedure_anatomical_location'] is not None and surgery[surgical_procedure_record_id]['surgical_procedure_anatomical_location'] != '':
                        surgical_procedure_anatomical_location_value = surgery[surgical_procedure_record_id]['surgical_procedure_anatomical_location']
                    surgical_procedure_value = ''
                    if surgery[surgical_procedure_record_id]['surgical_procedure'] is not None and surgery[surgical_procedure_record_id]['surgical_procedure'] != '':
                        surgical_procedure_value = surgery[surgical_procedure_record_id]['surgical_procedure']
                    # Don't record Surgery records as 'treatments' if `surgical_procedure_therapeutic` is not 'Yes'. Biopsies should be excluded from the treatment table.
                    if surgery[surgical_procedure_record_id]['surgical_procedure_therapeutic'] is not None and surgery[surgical_procedure_record_id]['surgical_procedure_therapeutic'] == 'Yes':
                        cda_treatment_records[f"{upstream_data_source}.{subject_id}.surgery_tmt.{surgery_disambiguator}"] = {
                            'id': f"{upstream_data_source}.{subject_id}.surgery_tmt.{surgery_disambiguator}",
                            'subject_id': subject_id,
                            'anatomic_site': surgical_procedure_anatomical_location_value,
                            'type': surgical_procedure_value,
                            'therapeutic_agent': ''
                        }
                        surgery_disambiguator = surgery_disambiguator + 1

print( 'done.', file=sys.stderr )

# Write the new CDA treatment records.
print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

with open( treatment_output_tsv, 'w' ) as OUT:
    print( *cda_treatment_fields, sep='\t', file=OUT )
    for treatment_id in sorted( cda_treatment_records ):
        output_row = list()
        treatment_record = cda_treatment_records[treatment_id]
        for cda_treatment_field in cda_treatment_fields:
            output_row.append( treatment_record[cda_treatment_field] )
        print( *output_row, sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



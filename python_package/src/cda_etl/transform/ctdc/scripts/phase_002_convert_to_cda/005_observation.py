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

participant_demographic_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.demographic_record_id.tsv' )
participant_diagnosis_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.diagnosis_record_id.tsv' )
participant_participant_status_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.participant_status_record_id.tsv' )
participant_specimen_input_tsv = path.join( tsv_input_root, 'Participant', 'Participant.specimen_record_id.tsv' )

demographic_input_tsv = path.join( tsv_input_root, 'Demographic', 'Demographic.tsv' )
diagnosis_input_tsv = path.join( tsv_input_root, 'Diagnosis', 'Diagnosis.tsv' )
participant_status_input_tsv = path.join( tsv_input_root, 'ParticipantStatus', 'ParticipantStatus.tsv' )
specimen_input_tsv = path.join( tsv_input_root, 'Specimen', 'Specimen.tsv' )

# CDA TSVs.
tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )
upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )
observation_output_tsv = path.join( tsv_output_root, 'observation.tsv' )

# Table header sequences.
cda_observation_fields = [
    # Note: this `id` will not survive aggregation: final versions of CDA
    # observation records are keyed only by id_alias, which is generated
    # only for aggregated data and will replace `id` here.
    'id',
    'subject_id',
    'vital_status',
    'sex',
    'year_of_observation',
    'age_at_observation',
    'diagnosis',
    'morphology',
    'grade',
    'stage',
    'observed_anatomic_site',
    'resection_anatomic_site'
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

cda_observation_records = dict()

# Load ParticipantStatus.survival_status -> observation.vital_status and create CDA observation records accordingly.
print( f"[{get_current_timestamp()}] Loading observation metadata from ParticipantStatus.survival_status...", end='', file=sys.stderr )

participant_participant_status = map_columns_one_to_many( participant_participant_status_input_tsv, 'participant_id', 'participant_status_record_id' )
participant_status = load_tsv_as_dict( participant_status_input_tsv )

vital_status_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_participant_status:
            for participant_status_record_id in sorted( participant_participant_status[participant_id] ):
                if participant_status[participant_status_record_id]['survival_status'] is not None and participant_status[participant_status_record_id]['survival_status'] != '':
                    # Make a new observation record with just this info.
                    cda_observation_records[f"{upstream_data_source}.{subject_id}.vital_status_obs.{vital_status_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.vital_status_obs.{vital_status_disambiguator}",
                        'subject_id': subject_id,
                        'vital_status': participant_status[participant_status_record_id]['survival_status'],
                        'sex': '',
                        'year_of_observation': '',
                        'age_at_observation': '',
                        'diagnosis': '',
                        'morphology': '',
                        'grade': '',
                        'stage': '',
                        'observed_anatomic_site': '',
                        'resection_anatomic_site': ''
                    }
                    vital_status_disambiguator = vital_status_disambiguator + 1

print( 'done.', file=sys.stderr )

# Make CDA observation records from Demographic records.
print( f"[{get_current_timestamp()}] Creating observation records from Demographic records...", end='', file=sys.stderr )

participant_demographic = map_columns_one_to_many( participant_demographic_input_tsv, 'participant_id', 'demographic_record_id' )
demographic = load_tsv_as_dict( demographic_input_tsv )

demographic_disambiguator = 0

# Save age_at_enrollment for downstream computations.
participant_age_at_enrollment = dict()

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_demographic:
            for demographic_record_id in sorted( participant_demographic[participant_id] ):
                if \
                    ( demographic[demographic_record_id]['sex'] is not None and demographic[demographic_record_id]['sex'] != '' ) or \
                    ( demographic[demographic_record_id]['age_at_enrollment'] is not None and demographic[demographic_record_id]['age_at_enrollment'] != '' ):
                    # Make a new observation record with Demographic info.
                    sex_value = ''
                    if demographic[demographic_record_id]['sex'] is not None and demographic[demographic_record_id]['sex'] != '':
                        sex_value = demographic[demographic_record_id]['sex']
                    age_at_enrollment_value = ''
                    if demographic[demographic_record_id]['age_at_enrollment'] is not None and demographic[demographic_record_id]['age_at_enrollment'] != '':
                        age_at_enrollment_value = int( float( demographic[demographic_record_id]['age_at_enrollment'] ) )
                        # Sanity check on expected units.
                        if demographic[demographic_record_id]['age_at_enrollment_unit'] != 'years':
                            sys.exit( f"FATAL: demographic_record_id '{demographic_record_id}' had unexpected age_at_enrollment_unit '{demographic[demographic_record_id]['age_at_enrollment_unit']}' (expected 'years'): cannot continue, please handle." )
                        if participant_id in participant_age_at_enrollment and participant_age_at_enrollment[participant_id] != age_at_enrollment_value:
                            sys.exit( f"FATAL: Different Demographic.age_at_enrollment values detected for CDA subject {subject_id}, current participant_id {participant_id}; cannot continue. Please handle." )
                        participant_age_at_enrollment[participant_id] = age_at_enrollment_value
                    cda_observation_records[f"{upstream_data_source}.{subject_id}.demographic_obs.{demographic_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.demographic_obs.{demographic_disambiguator}",
                        'subject_id': subject_id,
                        'vital_status': '',
                        'sex': sex_value,
                        'year_of_observation': '',
                        'age_at_observation': age_at_enrollment_value,
                        'diagnosis': '',
                        'morphology': '',
                        'grade': '',
                        'stage': '',
                        'observed_anatomic_site': '',
                        'resection_anatomic_site': ''
                    }
                    demographic_disambiguator = demographic_disambiguator + 1

print( 'done.', file=sys.stderr )

# Make CDA observation records from Diagnosis records.
print( f"[{get_current_timestamp()}] Creating observation records from Diagnosis records...", end='', file=sys.stderr )

participant_diagnosis = map_columns_one_to_many( participant_diagnosis_input_tsv, 'participant_id', 'diagnosis_record_id' )
diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

diagnosis_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_diagnosis:
            for diagnosis_record_id in sorted( participant_diagnosis[participant_id] ):
                if \
                    ( diagnosis[diagnosis_record_id]['ctep_disease_term'] is not None and diagnosis[diagnosis_record_id]['ctep_disease_term'] != '' ) or \
                    ( diagnosis[diagnosis_record_id]['histology'] is not None and diagnosis[diagnosis_record_id]['histology'] != '' ) or \
                    ( diagnosis[diagnosis_record_id]['tumor_grade'] is not None and diagnosis[diagnosis_record_id]['tumor_grade'] != '' ) or \
                    ( diagnosis[diagnosis_record_id]['stage_of_disease'] is not None and diagnosis[diagnosis_record_id]['stage_of_disease'] != '' ) or \
                    ( diagnosis[diagnosis_record_id]['primary_disease_site'] is not None and diagnosis[diagnosis_record_id]['primary_disease_site'] != '' ):
                    # Make a new observation record with Diagnosis info.
                    ctep_disease_term_value = ''
                    if diagnosis[diagnosis_record_id]['ctep_disease_term'] is not None and diagnosis[diagnosis_record_id]['ctep_disease_term'] != '':
                        ctep_disease_term_value = diagnosis[diagnosis_record_id]['ctep_disease_term']
                    histology_value = ''
                    if diagnosis[diagnosis_record_id]['histology'] is not None and diagnosis[diagnosis_record_id]['histology'] != '':
                        histology_value = diagnosis[diagnosis_record_id]['histology']
                    tumor_grade_value = ''
                    if diagnosis[diagnosis_record_id]['tumor_grade'] is not None and diagnosis[diagnosis_record_id]['tumor_grade'] != '':
                        tumor_grade_value = diagnosis[diagnosis_record_id]['tumor_grade']
                    stage_of_disease_value = ''
                    if diagnosis[diagnosis_record_id]['stage_of_disease'] is not None and diagnosis[diagnosis_record_id]['stage_of_disease'] != '':
                        stage_of_disease_value = diagnosis[diagnosis_record_id]['stage_of_disease']
                    primary_disease_site_value = ''
                    if diagnosis[diagnosis_record_id]['primary_disease_site'] is not None and diagnosis[diagnosis_record_id]['primary_disease_site'] != '':
                        primary_disease_site_value = diagnosis[diagnosis_record_id]['primary_disease_site']
                    cda_observation_records[f"{upstream_data_source}.{subject_id}.diagnosis_obs.{diagnosis_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.diagnosis_obs.{diagnosis_disambiguator}",
                        'subject_id': subject_id,
                        'vital_status': '',
                        'sex': '',
                        'year_of_observation': '',
                        'age_at_observation': '',
                        'diagnosis': ctep_disease_term_value,
                        'morphology': histology_value,
                        'grade': tumor_grade_value,
                        'stage': stage_of_disease_value,
                        'observed_anatomic_site': primary_disease_site_value,
                        'resection_anatomic_site': ''
                    }
                    diagnosis_disambiguator = diagnosis_disambiguator + 1

print( 'done.', file=sys.stderr )

# Make CDA observation records from Specimen records.
print( f"[{get_current_timestamp()}] Creating observation records from Specimen records...", end='', file=sys.stderr )

participant_specimen = map_columns_one_to_many( participant_specimen_input_tsv, 'participant_id', 'specimen_record_id' )
specimen = load_tsv_as_dict( specimen_input_tsv )

specimen_disambiguator = 0

for subject_id in original_participant_ids:
    for participant_id in original_participant_ids[subject_id]:
        if participant_id in participant_specimen:
            for specimen_record_id in sorted( participant_specimen[participant_id] ):
                if \
                    ( specimen[specimen_record_id]['collection_date'] is not None and specimen[specimen_record_id]['collection_date'] != '' ) or \
                    ( specimen[specimen_record_id]['anatomical_collection_site'] is not None and specimen[specimen_record_id]['anatomical_collection_site'] != '' ):
                    # Make a new observation record with Specimen info.
                    anatomical_collection_site_value = ''
                    if specimen[specimen_record_id]['anatomical_collection_site'] is not None and specimen[specimen_record_id]['anatomical_collection_site'] != '':
                        anatomical_collection_site_value = specimen[specimen_record_id]['anatomical_collection_site']
                    age_at_observation_value = ''
                    if specimen[specimen_record_id]['collection_date'] is not None and specimen[specimen_record_id]['collection_date'] != '':
                        # Sanity check on expected units.
                        if specimen[specimen_record_id]['collection_date_unit'] != 'days':
                            sys.exit( f"FATAL: specimen_record_id '{specimen_record_id}' had unexpected collection_date_unit '{specimen[specimen_record_id]['collection_date_unit']}' (expected 'days'): cannot continue, please handle." )
                        if participant_id in participant_age_at_enrollment:
                            # Compute age_at_observation by shifting age_at_enrollment by the (approximate!) indicated offset.
                            age_at_enrollment_value = participant_age_at_enrollment[participant_id]
                            collection_date_in_years = float( float( specimen[specimen_record_id]['collection_date'] ) / float( 365.25 ) )
                            collection_date_integer_part = int( collection_date_in_years )
                            collection_date_fractional_part = float( float( collection_date_in_years ) - float( collection_date_integer_part ) )
                            # Date offsets can be positive or negative or zero.
                            if collection_date_fractional_part >= 0.5:
                                collection_date_in_years = collection_date_in_years + 1.0
                            elif collection_date_fractional_part <= -0.5:
                                collection_date_in_years = collection_date_in_years - 1.0
                            age_at_observation_value = age_at_enrollment_value + int( collection_date_in_years )
                    cda_observation_records[f"{upstream_data_source}.{subject_id}.specimen_obs.{specimen_disambiguator}"] = {
                        'id': f"{upstream_data_source}.{subject_id}.specimen_obs.{specimen_disambiguator}",
                        'subject_id': subject_id,
                        'vital_status': '',
                        'sex': '',
                        'year_of_observation': '',
                        'age_at_observation': age_at_observation_value,
                        'diagnosis': '',
                        'morphology': '',
                        'grade': '',
                        'stage': '',
                        'observed_anatomic_site': anatomical_collection_site_value,
                        'resection_anatomic_site': ''
                    }
                    specimen_disambiguator = specimen_disambiguator + 1

print( 'done.', file=sys.stderr )

# Write the new CDA observation records.
print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

with open( observation_output_tsv, 'w' ) as OUT:
    print( *cda_observation_fields, sep='\t', file=OUT )
    for observation_id in sorted( cda_observation_records ):
        output_row = list()
        observation_record = cda_observation_records[observation_id]
        for cda_observation_field in cda_observation_fields:
            output_row.append( observation_record[cda_observation_field] )
        print( *output_row, sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CDS'

# Unmodified extracted data, converted directly from a Neo4j JSONL dump.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

participant_input_tsv = path.join( tsv_input_root, 'participant.tsv' )

diagnosis_input_tsv = path.join( tsv_input_root, 'diagnosis.tsv' )

diagnosis_of_participant_input_tsv = path.join( tsv_input_root, 'diagnosis_of_participant.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample.tsv' )

# This has been checked to ensure it covers every sample (August 2024). Recommend re-checking future datasets.

sample_from_participant_tsv = path.join( tsv_input_root, 'sample_from_participant.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

observation_output_tsv = path.join( tsv_output_root, 'observation.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_value_output_dir = path.join( aux_output_root, 'values' )

subject_sex_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_participant_clashes.gender.tsv" )

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
    'diagnosis',
    'morphology',
    'grade',
    'stage',
    'observed_anatomic_site',
    'resection_anatomic_site'
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

# Load observation.sex from participant.gender and log internal data clashes.

print( f"[{get_current_timestamp()}] Loading observation metadata from participant.gender...", end='', file=sys.stderr )

participant = load_tsv_as_dict( participant_input_tsv )

cda_observation_records = dict()

subject_sex_data_clashes = dict()

for subject_id in original_participant_uuids:
    
    seen_gender = ''

    for participant_uuid in original_participant_uuids[subject_id]:
        
        new_value = participant[participant_uuid]['gender']

        if new_value != '':
            
            if seen_gender == '':

                seen_gender = new_value

            elif seen_gender != new_value:
                
                # Set up clash-tracking data structures.

                original_value = seen_gender

                if subject_id not in subject_sex_data_clashes:
                    
                    subject_sex_data_clashes[subject_id] = dict()

                if original_value not in subject_sex_data_clashes[subject_id]:
                    
                    subject_sex_data_clashes[subject_id][original_value] = dict()

                # Does the existing value match a pattern we know will be deleted later?

                if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                    
                    # Is the new value any better?

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                        subject_sex_data_clashes[subject_id][original_value][new_value] = False

                    else:
                        
                        # Replace the old value with the new one.

                        seen_gender = new_value

                        subject_sex_data_clashes[subject_id][original_value][new_value] = True

                else:
                    
                    # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                    subject_sex_data_clashes[subject_id][original_value][new_value] = False

    cda_observation_records[f"{upstream_data_source}.{subject_id}.sex_obs"] = {
        
        'id': f"{upstream_data_source}.{subject_id}.sex_obs",
        'subject_id': subject_id,
        'vital_status': '',
        'sex': seen_gender,
        'year_of_observation': '',
        'diagnosis': '',
        'morphology': '',
        'grade': '',
        'stage': '',
        'observed_anatomic_site': '',
        'resection_anatomic_site': ''
    }

print( 'done.', file=sys.stderr )

# Load observation.resection_anatomic_site from participant->sample.sample_anatomic_site and create a separate CDA observation record for each distinct site.

print( f"[{get_current_timestamp()}] Loading observation.resection_anatomic_site metadata from sample.sample_anatomic_site...", end='', file=sys.stderr )

participant_sample = map_columns_one_to_many( sample_from_participant_tsv, 'participant_uuid', 'sample_uuid' )

sample = load_tsv_as_dict( sample_input_tsv )

for subject_id in original_participant_uuids:
    
    sites_of_resection = set()

    for participant_uuid in original_participant_uuids[subject_id]:
        
        if participant_uuid in participant_sample:
            
            for sample_uuid in participant_sample[participant_uuid]:
                
                new_value = sample[sample_uuid]['sample_anatomic_site']

                if new_value != '':
                    
                    sites_of_resection.add( new_value )

    i = 0

    for site in sorted( sites_of_resection ):
        
        observation_id = f"{upstream_data_source}.{subject_id}.resection_anatomic_site.{i}"

        i = i + 1

        cda_observation_records[observation_id] = {
            
            'id': observation_id,
            'subject_id': subject_id,
            'vital_status': '',
            'sex': '',
            'year_of_observation': '',
            'diagnosis': '',
            'morphology': '',
            'grade': '',
            'stage': '',
            'observed_anatomic_site': '',
            'resection_anatomic_site': site
        }

print( 'done.', file=sys.stderr )

# Make CDA observation records from diagnosis records.

participant_has_diagnosis = map_columns_one_to_many( diagnosis_of_participant_input_tsv, 'participant_uuid', 'diagnosis_uuid' )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

for subject_id in original_participant_uuids:
    
    for participant_uuid in original_participant_uuids[subject_id]:
        
        if participant_uuid in participant_has_diagnosis:
            
            for diagnosis_uuid in participant_has_diagnosis[participant_uuid]:
                
                # Are all relevant source fields empty? If not, save as an observation record for this subject_id.

                [ vital_status, obs_diagnosis, morphology, grade, stage, observed_anatomic_site ] = [ diagnosis[diagnosis_uuid]['vital_status'], diagnosis[diagnosis_uuid]['disease_type'], diagnosis[diagnosis_uuid]['morphology'], diagnosis[diagnosis_uuid]['tumor_grade'], diagnosis[diagnosis_uuid]['tumor_stage_clinical_m'], diagnosis[diagnosis_uuid]['primary_site'] ]

                if vital_status != '' or obs_diagnosis != '' or morphology != '' or grade != '' or stage != '' or observed_anatomic_site != '':
                    
                    # (Safe) assumption: no diagnosis_id corresponds to multiple subjects, so
                    # we will not have seen this diagnosis_id before.

                    observation_id = f"{upstream_data_source}.{subject_id}.diagnosis_uuid.{diagnosis_uuid}"

                    cda_observation_records[observation_id] = {
                        
                        'id': observation_id,
                        'subject_id': subject_id,
                        'vital_status': vital_status,
                        'sex': '',
                        'year_of_observation': '',
                        'diagnosis': obs_diagnosis,
                        'morphology': morphology,
                        'grade': grade,
                        'stage': stage,
                        'observed_anatomic_site': observed_anatomic_site,
                        'resection_anatomic_site': ''
                    }

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root} and logging data clashes...", end='', file=sys.stderr )

# Write the new CDA observation records.

with open( observation_output_tsv, 'w' ) as OUT:
    
    print( *cda_observation_fields, sep='\t', file=OUT )

    for observation_id in sorted( cda_observation_records ):
        
        output_row = list()

        observation_record = cda_observation_records[observation_id]

        for cda_observation_field in cda_observation_fields:
            
            output_row.append( observation_record[cda_observation_field] )

        print( *output_row, sep='\t', file=OUT )

# Log data clashes within CDA subjects.

# subject_sex_data_clashes[subject_id][original_value][new_value] = True

with open( subject_sex_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_participant_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_sex_data_clashes ):
        
        for original_value in sorted( subject_sex_data_clashes[subject_id] ):
            
            for new_value in sorted( subject_sex_data_clashes[subject_id][original_value] ):
                
                if subject_sex_data_clashes[subject_id][original_value][new_value] == False:
                    
                    print( *[ subject_id, 'gender', original_value, new_value, original_value ], sep='\t', file=OUT )

                else:
                    
                    print( *[ subject_id, 'gender', original_value, new_value, new_value ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



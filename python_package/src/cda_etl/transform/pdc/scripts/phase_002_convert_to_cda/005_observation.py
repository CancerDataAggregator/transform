#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

# PARAMETERS

upstream_data_source = 'PDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated; cases without containing studies removed;
# groups of multiple files sharing the same FileMetadata.file_location removed.

tsv_input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

case_demographic_input_tsv = path.join( tsv_input_root, 'Case', 'Case.demographic_id.tsv' )

demographic_input_tsv = path.join( tsv_input_root, 'Demographic', 'Demographic.tsv' )

case_diagnosis_input_tsv = path.join( tsv_input_root, 'Case', 'Case.diagnosis_id.tsv' )

diagnosis_input_tsv = path.join( tsv_input_root, 'Diagnosis', 'Diagnosis.tsv' )

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
    'diagnosis',
    'morphology',
    'grade',
    'stage',
    'observed_anatomic_site',
    'resection_anatomic_site'
]

# EXECUTION

# Load value patterns that will always be nulled during harmonization.

delete_everywhere = get_universal_value_deletion_patterns()

# Load CDA subject IDs for case_id values.

case_id_to_cda_subject_id = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'Case.case_id':
            
            case_id_to_cda_subject_id[value] = entity_id

print( f"[{get_current_timestamp()}] Loading observation metadata from Demographic and Diagnosis...", end='', file=sys.stderr )

# Load demographic records and case associations, and map the latter to CDA subjects.
# 
# See note in master field map table explaining why we separate observation
# records with ( vital_status, sex ) data from observation records with diagnostic information.

demographic = load_tsv_as_dict( demographic_input_tsv )

case_has_demographic = map_columns_one_to_many( case_demographic_input_tsv, 'case_id', 'demographic_id' )

subject_has_demographic = dict()

for case_id in case_has_demographic:
    
    cda_subject_id = case_id_to_cda_subject_id[case_id]

    if cda_subject_id not in subject_has_demographic:
        
        subject_has_demographic[cda_subject_id] = set()

    for demographic_id in case_has_demographic[case_id]:
        
        subject_has_demographic[cda_subject_id].add( demographic_id )

cda_observation_records = dict()

for subject_id in subject_has_demographic:
    
    # Don't repeat identical demographic-metadata combos for the same subject
    # when creating CDA observation records.

    seen_demographic_combos = dict()

    for demographic_id in subject_has_demographic[subject_id]:
        
        vital_status = demographic[demographic_id]['vital_status']

        sex = demographic[demographic_id]['gender']

        if vital_status not in seen_demographic_combos:
            
            seen_demographic_combos[vital_status] = set()

        if sex not in seen_demographic_combos[vital_status]:
            
            # We haven't seen this ( vital_status, sex ) tuple yet for this subject_id.

            seen_demographic_combos[vital_status].add( sex )

            # Are both fields empty? If not, save as an observation record for this subject_id.

            if vital_status != '' or sex != '':
                
                # (Safe) assumption: no demographic_id corresponds to multiple subjects, so
                # we will not have seen this demographic_id before.

                cda_observation_records[f"{upstream_data_source}.demographic_id.{demographic_id}"] = {
                    
                    'id': f"{upstream_data_source}.demographic_id.{demographic_id}",
                    'subject_id': subject_id,
                    'vital_status': vital_status,
                    'sex': sex,
                    'year_of_observation': '',
                    'diagnosis': '',
                    'morphology': '',
                    'grade': '',
                    'stage': '',
                    'observed_anatomic_site': '',
                    'resection_anatomic_site': ''
                }

# Load diagnosis records and case associations, and map the latter to CDA subjects.
# 
# See note in master field map table explaining why we separate observation
# records with ( vital_status, sex ) data from observation records with diagnostic information.

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

case_has_diagnosis = map_columns_one_to_many( case_diagnosis_input_tsv, 'case_id', 'diagnosis_id' )

subject_has_diagnosis = dict()

for case_id in case_has_diagnosis:
    
    cda_subject_id = case_id_to_cda_subject_id[case_id]

    if cda_subject_id not in subject_has_diagnosis:
        
        subject_has_diagnosis[cda_subject_id] = set()

    for diagnosis_id in case_has_diagnosis[case_id]:
        
        subject_has_diagnosis[cda_subject_id].add( diagnosis_id )

for subject_id in subject_has_diagnosis:
    
    for diagnosis_id in subject_has_diagnosis[subject_id]:
        
        year_of_observation = diagnosis[diagnosis_id]['year_of_diagnosis']
        p_diagnosis = diagnosis[diagnosis_id]['primary_diagnosis']
        morphology = diagnosis[diagnosis_id]['morphology']
        grade = diagnosis[diagnosis_id]['tumor_grade']
        stage = diagnosis[diagnosis_id]['tumor_stage']

        if stage is None or stage == '' or re.sub( r'\s', r'', stage.strip().lower() ) in delete_everywhere:
            
            stage = ''

            if diagnosis[diagnosis_id]['ajcc_pathologic_stage'] is not None and diagnosis[diagnosis_id]['ajcc_pathologic_stage'] != '':
                
                stage = diagnosis[diagnosis_id]['ajcc_pathologic_stage']

        observed_anatomic_site = diagnosis[diagnosis_id]['tissue_or_organ_of_origin']
        resection_anatomic_site = diagnosis[diagnosis_id]['site_of_resection_or_biopsy']

        # Are all fields empty? If not, save as an observation record for this subject_id.

        if year_of_observation != '' or diagnosis != '' or morphology != '' or grade != '' or stage != '' or observed_anatomic_site != '' or resection_anatomic_site != '':
            
            # (Safe) assumption: no diagnosis_id corresponds to multiple subjects, so
            # we will not have seen this diagnosis_id before.

            cda_observation_records[f"{upstream_data_source}.diagnosis_id.{diagnosis_id}"] = {
                
                'id': f"{upstream_data_source}.diagnosis_id.{diagnosis_id}",
                'subject_id': subject_id,
                'vital_status': '',
                'sex': '',
                'year_of_observation': year_of_observation,
                'diagnosis': p_diagnosis,
                'morphology': morphology,
                'grade': grade,
                'stage': stage,
                'observed_anatomic_site': observed_anatomic_site,
                'resection_anatomic_site': resection_anatomic_site
            }

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA observation records.

with open( observation_output_tsv, 'w' ) as OUT:
    
    print( *cda_observation_fields, sep='\t', file=OUT )

    for observation_id in sorted( cda_observation_records ):
        
        output_row = list()

        observation_record = cda_observation_records[observation_id]

        for cda_observation_field in cda_observation_fields:
            
            output_row.append( observation_record[cda_observation_field] )

        print( *output_row, sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



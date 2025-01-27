#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'GDC'

# Extracted TSVs.

tsv_input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

demographic_of_case_input_tsv = path.join( tsv_input_root, 'demographic_of_case.tsv' )

demographic_input_tsv = path.join( tsv_input_root, 'demographic.tsv' )

diagnosis_of_case_input_tsv = path.join( tsv_input_root, 'diagnosis_of_case.tsv' )

diagnosis_input_tsv = path.join( tsv_input_root, 'diagnosis.tsv' )

diagnosis_input_tsv = path.join( tsv_input_root, 'diagnosis.tsv' )

treatment_of_diagnosis_input_tsv = path.join( tsv_input_root, 'treatment_of_diagnosis.tsv' )

treatment_input_tsv = path.join( tsv_input_root, 'treatment.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

observation_output_tsv = path.join( tsv_output_root, 'observation.tsv' )

treatment_output_tsv = path.join( tsv_output_root, 'treatment.tsv' )

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

for output_dir in [ tsv_output_root ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Loading CDA subject IDs...", end='', file=sys.stderr )

# Load CDA subject IDs for all case_ids.

cda_subject_id = map_columns_one_to_one( upstream_identifiers_tsv, 'data_source_id_value', 'id', where_field='data_source_id_field_name', where_value='case.case_id' )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading observation metadata from demographic and diagnosis...", end='', file=sys.stderr )

# Load demographic records and case associations, and map the latter to CDA subjects.
# 
# See note in master field map table explaining why we separate observation
# records with ( vital_status, sex ) data from observation records with diagnostic information.

demographic = load_tsv_as_dict( demographic_input_tsv )

case_has_demographic = map_columns_one_to_many( demographic_of_case_input_tsv, 'case_id', 'demographic_id' )

subject_has_demographic = dict()

for case_id in case_has_demographic:
    
    if cda_subject_id[case_id] not in subject_has_demographic:
        
        subject_has_demographic[cda_subject_id[case_id]] = set()

    for demographic_id in case_has_demographic[case_id]:
        
        subject_has_demographic[cda_subject_id[case_id]].add( demographic_id )

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

case_has_diagnosis = map_columns_one_to_many( diagnosis_of_case_input_tsv, 'case_id', 'diagnosis_id' )

subject_has_diagnosis = dict()

for case_id in case_has_diagnosis:
    
    if cda_subject_id[case_id] not in subject_has_diagnosis:
        
        subject_has_diagnosis[cda_subject_id[case_id]] = set()

    for diagnosis_id in case_has_diagnosis[case_id]:
        
        subject_has_diagnosis[cda_subject_id[case_id]].add( diagnosis_id )

for subject_id in subject_has_diagnosis:
    
    for diagnosis_id in subject_has_diagnosis[subject_id]:
        
        year_of_observation = diagnosis[diagnosis_id]['year_of_diagnosis']
        p_diagnosis = diagnosis[diagnosis_id]['primary_diagnosis']
        morphology = diagnosis[diagnosis_id]['morphology']
        grade = diagnosis[diagnosis_id]['tumor_grade']
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

print( f"[{get_current_timestamp()}] Loading treatment metadata...", end='', file=sys.stderr )

# Load treatment records and case associations, and map the latter to CDA subjects.

treatment = load_tsv_as_dict( treatment_input_tsv )

diagnosis_has_treatment = map_columns_one_to_many( treatment_of_diagnosis_input_tsv, 'diagnosis_id', 'treatment_id' )

subject_has_treatment = dict()

for subject_id in subject_has_diagnosis:
    
    for diagnosis_id in subject_has_diagnosis[subject_id]:
        
        if diagnosis_id in diagnosis_has_treatment:
            
            for treatment_id in diagnosis_has_treatment[diagnosis_id]:
                
                if subject_id not in subject_has_treatment:
                    
                    subject_has_treatment[subject_id] = set()

                subject_has_treatment[subject_id].add( treatment_id )

cda_treatment_records = dict()

for subject_id in subject_has_treatment:
    
    for treatment_id in subject_has_treatment[subject_id]:
        
        anatomic_site = treatment[treatment_id]['treatment_anatomic_site']

        t_type = treatment[treatment_id]['treatment_type']

        therapeutic_agent = treatment[treatment_id]['therapeutic_agents']

        # Are all fields empty? If not, save as a treatment record for this subject_id.

        if anatomic_site != '' or t_type != '' or therapeutic_agent != '':
            
            # (Safe) assumption: no treatment_id corresponds to multiple subjects, so
            # we will not have seen this treatment_id before.

            cda_treatment_records[f"{upstream_data_source}.treatment_id.{treatment_id}"] = {
                
                'id': f"{upstream_data_source}.treatment_id.{treatment_id}",
                'subject_id': subject_id,
                'anatomic_site': anatomic_site,
                'type': t_type,
                'therapeutic_agent': therapeutic_agent
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



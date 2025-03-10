#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'ICDC'

# Unmodified extracted data, converted directly from JSON to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

# Beware compound keys: demographic_id is always null. At time of writing, ( case_id, demographic_id ) tuples are unique within demographic.tsv (irrespective of whether or not demographic_id is null).

demographic_input_tsv = path.join( tsv_input_root, 'demographic', 'demographic.tsv' )

diagnosis_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.tsv' )

diagnosis_case_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample', 'sample.tsv' )

sample_case_input_tsv = path.join( tsv_input_root, 'sample', 'sample.case_id.tsv' )

vital_signs_input_tsv = path.join( tsv_input_root, 'vital_signs', 'vital_signs.tsv' )

visit_case_input_tsv = path.join( tsv_input_root, '__redundant_relationship_validation', 'visit.cycle_case_id_and_cycle_number.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

observation_output_tsv = path.join( tsv_output_root, 'observation.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_value_output_dir = path.join( aux_output_root, 'values' )

subject_sex_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_demographic_clashes.sex.tsv" )

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

# Load CDA subject IDs for case_id values and vice versa.

case_id_to_cda_subject_id = dict()

original_case_ids = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'case.case_id':
            
            case_id = value

            subject_id = entity_id

            case_id_to_cda_subject_id[case_id] = subject_id

            if subject_id not in original_case_ids:
                
                original_case_ids[subject_id] = set()

            original_case_ids[subject_id].add( case_id )

# Load subject.sex from demographic.sex and log internal data clashes.

print( f"[{get_current_timestamp()}] Loading observation metadata from case.sex...", end='', file=sys.stderr )

demographic = load_tsv_as_dict( demographic_input_tsv, id_column_count=2 )

sex = dict()

subject_sex_data_clashes = dict()

for subject_id in original_case_ids:
    
    sex[subject_id] = ''

    for case_id in original_case_ids[subject_id]:
        
        if case_id in demographic:
            
            for demographic_id in demographic[case_id]:
                
                new_value = demographic[case_id][demographic_id]['sex']

                if new_value != '':
                    
                    if sex[subject_id] == '':

                        sex[subject_id] = new_value

                    elif sex[subject_id] != new_value:
                        
                        # Set up clash-tracking data structures.

                        original_value = sex[subject_id]

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

                                sex[subject_id] = new_value

                                subject_sex_data_clashes[subject_id][original_value][new_value] = True

                        else:
                            
                            # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                            subject_sex_data_clashes[subject_id][original_value][new_value] = False

print( 'done.', file=sys.stderr )

# Load observation.vital_status and observation.year_of_observation from case->vital_signs[.date_of_vital_signs] and create a separate CDA observation record for each distinct combination.

print( f"[{get_current_timestamp()}] Loading observation.vital_status and observation.year_of_observation metadata from vital_signs...", end='', file=sys.stderr )

case_visit = map_columns_one_to_many( visit_case_input_tsv, 'case_id', 'visit_id' )

# Won't work: no unique keys. Load file line by line.
# 
# vital_signs = load_tsv_as_dict( vital_signs_input_tsv )

visit_id_to_year_of_observation = dict()

with open( vital_signs_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip() for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        year_of_observation = re.sub( r'^([0-9]+)-.*$', r'\1', record['date_of_vital_signs'] )

        if year_of_observation != '':
            
            visit_id = record['visit_id']

            if visit_id not in visit_id_to_year_of_observation:
                
                visit_id_to_year_of_observation[visit_id] = set()

            visit_id_to_year_of_observation[visit_id].add( year_of_observation )

cda_observation_records = dict()

for subject_id in original_case_ids:
    
    vital_status_to_year_of_obs = dict()

    for case_id in original_case_ids[subject_id]:
        
        if case_id in case_visit:
            
            for visit_id in case_visit[case_id]:
                
                if visit_id in visit_id_to_year_of_observation:
                    
                    if 'Alive' not in vital_status_to_year_of_obs:
                        
                        vital_status_to_year_of_obs['Alive'] = set()

                    for year_of_observation in visit_id_to_year_of_observation[visit_id]:
                        
                        vital_status_to_year_of_obs['Alive'].add( year_of_observation )

    i = 0

    for vital_status in sorted( vital_status_to_year_of_obs ):
        
        for year_of_observation in sorted( vital_status_to_year_of_obs[vital_status] ):
            
            observation_id = f"{upstream_data_source}.{subject_id}.vital_status_and_year.{i}"

            cda_observation_records[observation_id] = {
                
                'id': observation_id,
                'subject_id': subject_id,
                'vital_status': vital_status,
                'sex': sex[subject_id] if subject_id in sex else '',
                'year_of_observation': year_of_observation,
                'diagnosis': '',
                'morphology': '',
                'grade': '',
                'stage': '',
                'observed_anatomic_site': '',
                'resection_anatomic_site': ''
            }

            i = i + 1

print( 'done.', file=sys.stderr )

# Make CDA observation records from sample records.

print( f"[{get_current_timestamp()}] Loading vital_status, year_of_observation, morphology and resection_anatomic_site metadata from sample...", end='', file=sys.stderr )

sample = load_tsv_as_dict( sample_input_tsv )

case_sample = map_columns_one_to_many( sample_case_input_tsv, 'case_id', 'sample_id' )

for subject_id in original_case_ids:
    
    vital_to_year_to_site_to_grade_to_morphology = dict()

    for case_id in original_case_ids[subject_id]:
        
        if case_id in case_sample:
            
            for sample_id in case_sample[case_id]:
                
                necropsy_sample = sample[sample_id]['necropsy_sample']

                vital_status = 'Alive'

                if re.search( r'^yes$', necropsy_sample, re.IGNORECASE ) is not None:
                    
                    vital_status = 'Dead'

                year_of_observation = re.sub( r'^([0-9]+)-.*$', r'\1', sample[sample_id]['date_of_sample_collection'] )

                resection_anatomic_site = sample[sample_id]['sample_site']

                grade = sample[sample_id]['tumor_grade']

                morphology = sample[sample_id]['specific_sample_pathology']

                if vital_status not in vital_to_year_to_site_to_grade_to_morphology:
                    
                    vital_to_year_to_site_to_grade_to_morphology[vital_status] = dict()

                if year_of_observation not in vital_to_year_to_site_to_grade_to_morphology[vital_status]:
                    
                    vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation] = dict()

                if resection_anatomic_site not in vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation]:
                    
                    vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site] = dict()

                if grade not in vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site]:
                    
                    vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site][grade] = set()

                vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site][grade].add( morphology )

    i = 0

    for vital_status in sorted( vital_to_year_to_site_to_grade_to_morphology ):
        
        for year_of_observation in sorted( vital_to_year_to_site_to_grade_to_morphology[vital_status] ):
            
            for resection_anatomic_site in sorted( vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation] ):
                
                for grade in sorted( vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site] ):
                    
                    for morphology in sorted( vital_to_year_to_site_to_grade_to_morphology[vital_status][year_of_observation][resection_anatomic_site][grade] ):
                        
                        observation_id = f"{upstream_data_source}.{subject_id}.vital_status_and_year_and_grade_and_morphology_and_resection_anatomic_site.{i}"

                        cda_observation_records[observation_id] = {
                            
                            'id': observation_id,
                            'subject_id': subject_id,
                            'vital_status': vital_status,
                            'sex': sex[subject_id] if subject_id in sex else '',
                            'year_of_observation': year_of_observation,
                            'diagnosis': '',
                            'morphology': morphology,
                            'grade': grade,
                            'stage': '',
                            'observed_anatomic_site': '',
                            'resection_anatomic_site': resection_anatomic_site
                        }

                        i = i + 1

print( 'done.', file=sys.stderr )

# Make CDA observation records from diagnosis records.

print( f"[{get_current_timestamp()}] Loading vital_status, year_of_observation and resection_anatomic_site metadata from sample...", end='', file=sys.stderr )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

case_diagnosis = map_columns_one_to_many( diagnosis_case_input_tsv, 'case_id', 'diagnosis_id' )

for subject_id in original_case_ids:
    
    year_to_diag_to_morph_to_grade_to_stage_to_site= dict()

    for case_id in original_case_ids[subject_id]:
        
        if case_id in case_diagnosis:
            
            for diagnosis_id in case_diagnosis[case_id]:
                
                year_of_observation = re.sub( r'^([0-9]+)-.*$', r'\1', diagnosis[diagnosis_id]['date_of_diagnosis'] )
        
                diagnosis_term = diagnosis[diagnosis_id]['disease_term']

                morphology = diagnosis[diagnosis_id]['histology_cytopathology']

                grade = diagnosis[diagnosis_id]['histological_grade']

                stage = diagnosis[diagnosis_id]['stage_of_disease']

                observed_anatomic_site = diagnosis[diagnosis_id]['primary_disease_site']

                if year_of_observation not in year_to_diag_to_morph_to_grade_to_stage_to_site:
                    
                    year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation] = dict()

                if diagnosis_term not in year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation]:
                    
                    year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term] = dict()

                if morphology not in year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term]:
                    
                    year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology] = dict()

                if grade not in year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology]:
                    
                    year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade] = dict()

                if stage not in year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade]:
                    
                    year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade][stage] = set()

                year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade][stage].add( observed_anatomic_site )

    i = 0

    for year_of_observation in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site ):
        
        for diagnosis_term in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation] ):
            
            for morphology in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term] ):
                
                for grade in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology] ):
                    
                    for stage in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade] ):
                        
                        for observed_anatomic_site in sorted( year_to_diag_to_morph_to_grade_to_stage_to_site[year_of_observation][diagnosis_term][morphology][grade][stage] ):
                            
                            observation_id = f"{upstream_data_source}.{subject_id}.diagnosis.{i}"

                            cda_observation_records[observation_id] = {
                                
                                'id': observation_id,
                                'subject_id': subject_id,
                                'vital_status': '',
                                'sex': sex[subject_id] if subject_id in sex else '',
                                'year_of_observation': year_of_observation,
                                'diagnosis': diagnosis_term,
                                'morphology': morphology,
                                'grade': grade,
                                'stage': stage,
                                'observed_anatomic_site': observed_anatomic_site,
                                'resection_anatomic_site': ''
                            }

                            i = i + 1

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
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_demographic_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_sex_data_clashes ):
        
        for original_value in sorted( subject_sex_data_clashes[subject_id] ):
            
            for new_value in sorted( subject_sex_data_clashes[subject_id][original_value] ):
                
                if subject_sex_data_clashes[subject_id][original_value][new_value] == False:
                    
                    print( *[ subject_id, 'sex', original_value, new_value, original_value ], sep='\t', file=OUT )

                else:
                    
                    print( *[ subject_id, 'sex', original_value, new_value, new_value ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



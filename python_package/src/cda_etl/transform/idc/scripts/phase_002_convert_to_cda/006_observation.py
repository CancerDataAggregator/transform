#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_tcga_study_name, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'IDC'

# Extracted data, converted from JSONL to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

dicom_series_input_tsv = path.join( tsv_input_root, 'dicom_series.tsv' )

idc_case_input_tsv = path.join( tsv_input_root, 'idc_case.tsv' )

original_collections_metadata_input_tsv = path.join( tsv_input_root, 'original_collections_metadata.tsv' )

tcga_clinical_input_tsv = path.join( tsv_input_root, 'tcga_clinical_rel9.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

observation_output_tsv = path.join( tsv_output_root, 'observation.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_value_output_dir = path.join( aux_output_root, 'values' )

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

# Load CDA subject IDs for idc_case_id values and vice versa.

print( f"[{get_current_timestamp()}] Loading subject ID maps...", end='', file=sys.stderr )

idc_case_id_to_cda_subject_id = dict()

original_idc_case_ids = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'dicom_all.idc_case_id':
            
            idc_case_id = value

            subject_id = entity_id

            idc_case_id_to_cda_subject_id[idc_case_id] = subject_id

            if subject_id not in original_idc_case_ids:
                
                original_idc_case_ids[subject_id] = set()

            original_idc_case_ids[subject_id].add( idc_case_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading observation metadata from dicom_all and original_collections_metadata...", end='', file=sys.stderr )

cda_observation_records = dict()

case = load_tsv_as_dict( idc_case_input_tsv )

collection_cancer_type_string = map_columns_one_to_one( original_collections_metadata_input_tsv, 'collection_id', 'CancerTypes' )

collection_tumor_location = map_columns_one_to_one( dicom_series_input_tsv, 'collection_id', 'collection_tumorLocation' )

for subject_id in original_idc_case_ids:
    
    for idc_case_id in original_idc_case_ids[subject_id]:
        
        cda_observation_records[f"{upstream_data_source}.{subject_id}.{idc_case_id}.sex_and_anatomy_obs"] = {
            
            'id': f"{upstream_data_source}.{subject_id}.{idc_case_id}.sex_and_anatomy_obs",
            'subject_id': subject_id,
            'vital_status': '',
            'sex': case[idc_case_id]['PatientSex'] if case[idc_case_id]['PatientSex'] is not None else '',
            'year_of_observation': '',
            'diagnosis': collection_cancer_type_string[case[idc_case_id]['collection_id']],
            'morphology': '',
            'grade': '',
            'stage': '',
            'observed_anatomic_site': collection_tumor_location[case[idc_case_id]['collection_id']],
            'resection_anatomic_site': ''
        }

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading observation metadata from tcga_clinical_rel9...", end='', file=sys.stderr )

# Confirmed: exactly one row per submitter_case_id at time of writing (Sep 2024).

tcga_clinical = load_tsv_as_dict( tcga_clinical_input_tsv )

for subject_id in original_idc_case_ids:
    
    for idc_case_id in original_idc_case_ids[subject_id]:
        
        submitter_case_id = case[idc_case_id]['submitter_case_id']

        if submitter_case_id in tcga_clinical:
            
            tcga_record = tcga_clinical[submitter_case_id]

            stage_value = ''

            if tcga_record['pathologic_stage'] is not None and tcga_record['pathologic_stage'] != '':
                
                stage_value = tcga_record['pathologic_stage']

            elif tcga_record['clinical_stage'] is not None and tcga_record['clinical_stage'] != '':
                
                stage_value = tcga_record['clinical_stage']

            cda_observation_records[f"{upstream_data_source}.{subject_id}.{idc_case_id}.tcga_clinical_obs"] = {
                
                'id': f"{upstream_data_source}.{subject_id}.{idc_case_id}.tcga_clinical_obs",
                'subject_id': subject_id,
                'vital_status': tcga_record['vital_status'] if tcga_record['vital_status'] is not None else '',
                'sex': tcga_record['gender'] if tcga_record['gender'] is not None else '',
                'year_of_observation': tcga_record['year_of_diagnosis'] if tcga_record['year_of_diagnosis'] is not None else '',
                'diagnosis': get_tcga_study_name( tcga_record['disease_code'] ) if tcga_record['disease_code'] is not None else '',
                'morphology': tcga_record['icd_o_3_histology'] if tcga_record['icd_o_3_histology'] is not None else '',
                'grade': tcga_record['neoplasm_histologic_grade'] if tcga_record['neoplasm_histologic_grade'] is not None else '',
                'stage': stage_value,
                'observed_anatomic_site': '',
                'resection_anatomic_site': ''
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



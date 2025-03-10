#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'IDC'

# Extracted data, converted from JSONL to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

series_input_tsv = path.join( tsv_input_root, 'dicom_series.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

file_describes_subject_output_tsv = path.join( tsv_output_root, 'file_describes_subject.tsv' )

# EXECUTION

# Load CDA subject IDs for idc_case_id values.

idc_case_id_to_cda_subject_id = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'dicom_all.idc_case_id':
            
            idc_case_id_to_cda_subject_id[value] = entity_id

# These are confirmed one-to-one at time of writing (Sep 2024).

series_case = map_columns_one_to_one( series_input_tsv, 'crdc_series_uuid', 'idc_case_id' )

cda_dicom_series_describes_subject = dict()

for series_id in series_case:
    
    idc_case_id = series_case[series_id]

    cda_subject_id = idc_case_id_to_cda_subject_id[idc_case_id]

    if series_id not in cda_dicom_series_describes_subject:
        
        cda_dicom_series_describes_subject[series_id] = set()

    cda_dicom_series_describes_subject[series_id].add( cda_subject_id )

print( f"[{get_current_timestamp()}] Writing {file_describes_subject_output_tsv}...", end='', file=sys.stderr )

with open( file_describes_subject_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', file=OUT )

    for series_id in sorted( cda_dicom_series_describes_subject ):
        
        for subject_id in sorted( cda_dicom_series_describes_subject[series_id] ):
            
            print( *[ series_id, subject_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



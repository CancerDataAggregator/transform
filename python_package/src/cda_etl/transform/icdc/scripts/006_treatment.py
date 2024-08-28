#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'ICDC'

# Unmodified extracted data, converted directly from JSON to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

prior_surgery_input_tsv = path.join( tsv_input_root, 'prior_surgery', 'prior_surgery.tsv' )

case_enrollment_input_tsv = path.join( tsv_input_root, 'case', 'case.enrollment_id.tsv' )

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

# Load treatment.anatomic_site and treatment.type from prior_surgery.anatomical_site_of_surgery and prior_surgery.procedure and create CDA treatment records to match.

print( f"[{get_current_timestamp()}] Loading treatment.anatomic_site and treatment.type metadata from prior_surgery...", end='', file=sys.stderr )

enrollment_to_case = map_columns_one_to_many( case_enrollment_input_tsv, 'enrollment_id', 'case_id' )

# Won't work. No unique IDs. Load file one row at a time.
# 
# prior_surgery = load_tsv_as_dict( prior_surgery_input_tsv )

cda_treatment_records = dict()

with open( prior_surgery_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    i = 0

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        if record['anatomical_site_of_surgery'] != '' or record['procedure'] != '':
            
            enrollment_id = record['enrollment_id']

            # This should break with a key error if it's not present.

            for case_id in enrollment_to_case[enrollment_id]:
                
                # This should break with a key error if it's not present.

                subject_id = case_id_to_cda_subject_id[case_id]

                treatment_id = f"{upstream_data_source}.{subject_id}.treatment.{i}"

                cda_treatment_records[treatment_id] = {
                    
                    'id': treatment_id,
                    'subject_id': subject_id,
                    'anatomic_site': record['anatomical_site_of_surgery'],
                    'type': record['procedure'],
                    'therapeutic_agent': ''
                }

                i = i + 1

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

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



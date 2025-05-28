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

case_treatment_input_tsv = path.join( tsv_input_root, 'Case', 'Case.treatment_id.tsv' )

treatment_input_tsv = path.join( tsv_input_root, 'Treatment', 'Treatment.tsv' )

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

print( f"[{get_current_timestamp()}] Loading treatment metadata from Treatment...", end='', file=sys.stderr )

# Load treatment records and case associations, and map the latter to CDA subjects.

treatment = load_tsv_as_dict( treatment_input_tsv )

case_has_treatment = map_columns_one_to_many( case_treatment_input_tsv, 'case_id', 'treatment_id' )

subject_has_treatment = dict()

for case_id in case_has_treatment:
    
    cda_subject_id = case_id_to_cda_subject_id[case_id]

    if cda_subject_id not in subject_has_treatment:
        
        subject_has_treatment[cda_subject_id] = set()

    for treatment_id in case_has_treatment[case_id]:
        
        subject_has_treatment[cda_subject_id].add( treatment_id )

cda_treatment_records = dict()

for subject_id in subject_has_treatment:
    
    for treatment_id in subject_has_treatment[subject_id]:
        
        anatomic_site = treatment[treatment_id]['treatment_anatomic_site']

        treatment_type = treatment[treatment_id]['treatment_type']

        therapeutic_agent = treatment[treatment_id]['therapeutic_agents']

        # Are all fields empty? If not, save as a treatment record for this subject_id.

        if anatomic_site != '' or treatment_type != '' or therapeutic_agent != '':
                
                # (Safe: 2025-05-28) assumption: no treatment_id corresponds to multiple subjects, so
                # we will not have seen this treatment_id before.

                cda_treatment_records[f"{upstream_data_source}.treatment_id.{treatment_id}"] = {
                    
                    'id': f"{upstream_data_source}.treatment_id.{treatment_id}",
                    'subject_id': subject_id,
                    'anatomic_site': anatomic_site,
                    'type': treatment_type,
                    'therapeutic_agent': therapeutic_agent
                }

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



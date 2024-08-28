#!/usr/bin/env python3 -u

import re
import sys

from os import makedirs, path
from time import sleep

from cda_etl.lib import get_dbgap_study_metadata, load_tsv_as_dict, map_columns_one_to_one

# PARAMETERS

upstream_data_source = 'ICDC'

debug = True

# Unmodified extracted data, converted directly from JSON to TSV.

extracted_tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

program_input_tsv = path.join( extracted_tsv_input_root, 'program', 'program.tsv' )

study_input_tsv = path.join( extracted_tsv_input_root, 'study', 'study.tsv' )

program_study_input_tsv = path.join( extracted_tsv_input_root, 'program', 'program.clinical_study_designation.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

project_output_tsv = path.join( tsv_output_root, 'project.tsv' )

project_in_project_output_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

upstream_identifiers_output_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

# Table header sequences.

cda_project_fields = [
    
    'id',
    'crdc_id',
    'type',
    'name',
    'short_name'
]

upstream_identifiers_fields = [
    
    'cda_table',
    'id',
    'data_source',
    'data_source_id_field_name',
    'data_source_id_value'
]

# Caches for loaded/processed data.

cda_project_records = dict()

cda_project_in_project = dict()

upstream_identifiers = dict()

# EXECUTION

if not path.exists( tsv_output_root ):
    
    makedirs( tsv_output_root )

# Load data for programs.

program = load_tsv_as_dict( program_input_tsv )

for program_name in program:
    
    program_acronym = program[program_name]['program_acronym']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.program.{program_acronym}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'program'

    cda_project_record['name'] = program_name

    cda_project_record['short_name'] = program_acronym

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['program.program_acronym'] = program_acronym

    upstream_identifiers[cda_id]['program.program_name'] = program_name

# Load data for studies and process dbGaP references.

study = load_tsv_as_dict( study_input_tsv )

for study_id in study:
    
    study_name = study[study_id]['clinical_study_name']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.study.{study_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'study'

    cda_project_record['name'] = study_name

    cda_project_record['short_name'] = study_id

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['study.clinical_study_designation'] = study_id

    upstream_identifiers[cda_id]['study.clinical_study_name'] = study_name

# Load study-in-program containment metadata as published from
# program_study_input_tsv.

study_in_program = map_columns_one_to_one( program_study_input_tsv, 'clinical_study_designation', 'program_acronym' )

for study_id in study_in_program:
    
    cda_study_id = f"{upstream_data_source}.study.{study_id}"

    program_acronym = study_in_program[study_id]

    cda_program_id = f"{upstream_data_source}.program.{program_acronym}"

    if cda_study_id not in cda_project_in_project:
        
        cda_project_in_project[cda_study_id] = set()

    cda_project_in_project[cda_study_id].add( cda_program_id )

# Write the new CDA project records.

with open( project_output_tsv, 'w' ) as OUT:
    
    print( *cda_project_fields, sep='\t', file=OUT )

    for project_id in sorted( cda_project_records ):
        
        output_row = list()

        project_record = cda_project_records[project_id]

        for cda_project_field in cda_project_fields:
            
            output_row.append( project_record[cda_project_field] )

        print( *output_row, sep='\t', file=OUT )

# Write the CDA-level project-in-project relation.

with open( project_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'child_project_id', 'parent_project_id' ], sep='\t', file=OUT )

    for child_project_id in sorted( cda_project_in_project ):
        
        for parent_project_id in sorted( cda_project_in_project[child_project_id] ):
            
            print( *[ child_project_id, parent_project_id ], sep='\t', file=OUT )

# Save provenance cross-references to the upstream_identifiers table.

with open( upstream_identifiers_output_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_project_id in sorted( upstream_identifiers ):
        
        for field_name in sorted( upstream_identifiers[cda_project_id] ):
            
            value = upstream_identifiers[cda_project_id][field_name]

            print( *[ 'project', cda_project_id, upstream_data_source, field_name, value ], sep='\t', file=OUT )



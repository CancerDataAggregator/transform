#!/usr/bin/env python3 -u

from os import makedirs, path

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

# PARAMETERS

upstream_data_source = 'IDC'

debug = True

# Extracted data, converted from JSONL to TSV.

extracted_tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

input_tsv = path.join( extracted_tsv_input_root, 'original_collections_metadata.tsv' )

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

program_to_collection_id = map_columns_one_to_many( input_tsv, 'Program', 'collection_id' )

collection_id_to_name = map_columns_one_to_one( input_tsv, 'collection_id', 'collection_name' )

# Construct CDA project records for programs and collections.

for program_name in program_to_collection_id:
    
    cda_project_record = dict()

    cda_program_id = f"{upstream_data_source}.program.{program_name}"

    cda_project_record['id'] = cda_program_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'program'

    cda_project_record['name'] = program_name

    cda_project_record['short_name'] = program_name

    cda_project_records[cda_program_id] = cda_project_record

    if cda_program_id not in upstream_identifiers:
        
        upstream_identifiers[cda_program_id] = dict()

    upstream_identifiers[cda_program_id]['original_collections_metadata.Program'] = program_name

    for collection_id in program_to_collection_id[program_name]:
        
        collection_name = collection_id_to_name[collection_id]

        cda_project_record = dict()

        cda_collection_id = f"{upstream_data_source}.collection.{collection_id}"

        cda_project_record['id'] = cda_collection_id

        # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

        cda_project_record['crdc_id'] = ''

        cda_project_record['type'] = 'collection'

        cda_project_record['name'] = collection_name

        cda_project_record['short_name'] = collection_id

        cda_project_records[cda_collection_id] = cda_project_record

        if cda_collection_id not in upstream_identifiers:
            
            upstream_identifiers[cda_collection_id] = dict()

        upstream_identifiers[cda_collection_id]['original_collections_metadata.collection_id'] = collection_id

        upstream_identifiers[cda_collection_id]['original_collections_metadata.collection_name'] = collection_name

        if cda_collection_id not in cda_project_in_project:
            
            cda_project_in_project[cda_collection_id] = set()

        cda_project_in_project[cda_collection_id].add( cda_program_id )

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



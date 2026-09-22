#!/usr/bin/env python3 -u

import json
import re
import sys

from os import path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CTDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated.
tsv_input_root = path.join( 'extracted_data', f"{upstream_data_source.lower()}_postprocessed" )

data_file_input_tsv = path.join( tsv_input_root, 'DataFile', 'DataFile.tsv' )
study_data_file_input_tsv = path.join( tsv_input_root, 'Study', 'Study.data_file_uuid.tsv' )
specimen_input_tsv = path.join( tsv_input_root, 'Specimen', 'Specimen.tsv' )
data_file_specimen_input_tsv = path.join( tsv_input_root, 'DataFile', 'DataFile.specimen_record_id.tsv' )

# CDA TSVs.
tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )
project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )
file_output_tsv = path.join( tsv_output_root, 'file.tsv' )
file_anatomic_site_output_tsv = path.join( tsv_output_root, 'file_anatomic_site.tsv' )
file_tumor_vs_normal_output_tsv = path.join( tsv_output_root, 'file_tumor_vs_normal.tsv' )
file_in_project_output_tsv = path.join( tsv_output_root, 'file_in_project.tsv' )

# Table header sequences.
cda_file_fields = [
    'id',
    'crdc_id',
    'name',
    'description',
    'drs_uri',
    'access',
    'size',
    'format',
    'type',
    'category'
]

# This is just here for reference when reading the code that parses this table.
upstream_identifiers_fields = [
    'cda_table',
    'id',
    'upstream_source',
    'upstream_field',
    'upstream_id'
]

debug = False

# EXECUTION

# Load metadata for files.
print( f"[{get_current_timestamp()}] Loading file metadata...", end='', file=sys.stderr )
cda_file_records = dict()
data_file = load_tsv_as_dict( data_file_input_tsv )

for data_file_uuid in data_file:
    cda_file_records[data_file_uuid] = dict()

    cda_file_records[data_file_uuid]['id'] = data_file_uuid
    cda_file_records[data_file_uuid]['crdc_id'] = ''
    cda_file_records[data_file_uuid]['name'] = data_file[data_file_uuid]['data_file_name']
    cda_file_records[data_file_uuid]['description'] = data_file[data_file_uuid]['data_file_description']
    cda_file_records[data_file_uuid]['drs_uri'] = f"drs://{data_file_uuid.lower()}"
    cda_file_records[data_file_uuid]['access'] = ''
    cda_file_records[data_file_uuid]['size'] = '' if data_file[data_file_uuid]['data_file_size'] is None else int( float( data_file[data_file_uuid]['data_file_size'] ) )
    cda_file_records[data_file_uuid]['format'] = data_file[data_file_uuid]['data_file_format']
    cda_file_records[data_file_uuid]['type'] = ''
    cda_file_records[data_file_uuid]['category'] = data_file[data_file_uuid]['data_file_type']

print( 'done.', file=sys.stderr )

# Load CDA IDs for Studies. Don't use any of the canned loader
# functions to load this data structure; it's got multiplicities that are
# generally mishandled if certain keys are assumed to be unique (e.g. a CDA
# subject can have multiple case_id values from the same data source, rendering any attempt
# to key this information on the first X columns incorrect or so cumbersome as to
# be pointless).
print( f"[{get_current_timestamp()}] Loading file<->study_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )
upstream_identifiers = dict()

with open( upstream_identifiers_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )
        if cda_table not in upstream_identifiers:
            upstream_identifiers[cda_table] = dict()
        if entity_id not in upstream_identifiers[cda_table]:
            upstream_identifiers[cda_table][entity_id] = dict()
        if data_source not in upstream_identifiers[cda_table][entity_id]:
            upstream_identifiers[cda_table][entity_id][data_source] = dict()
        if source_field not in upstream_identifiers[cda_table][entity_id][data_source]:
            upstream_identifiers[cda_table][entity_id][data_source][source_field] = set()
        upstream_identifiers[cda_table][entity_id][data_source][source_field].add( value )

cda_project_id = dict()

for project_id in upstream_identifiers['project']:
    # Some of these are from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.
    if upstream_data_source in upstream_identifiers['project'][project_id]:
        if 'Study.study_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Study.study_id']:
                if value not in cda_project_id:
                    cda_project_id[value] = project_id
                elif cda_project_id[value] != project_id:
                    sys.exit( f"FATAL: {upstream_data_source} Study.study_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.
cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

print( 'done.', file=sys.stderr )

# Compute CDA file<->project relationships.
print( f"[{get_current_timestamp()}] Associating files with projects...", end='', file=sys.stderr )
cda_file_in_project = dict()
data_file_study_id = map_columns_one_to_many( study_data_file_input_tsv, 'data_file_uuid', 'study_id' )

for data_file_uuid in data_file_study_id:
    for study_id in data_file_study_id[data_file_uuid]:
        
        if study_id not in cda_project_id:
            sys.exit( f"FATAL: DataFile {data_file_uuid} associated with Study.study_id {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

        if data_file_uuid not in cda_file_in_project:
            cda_file_in_project[data_file_uuid] = set()

        cda_file_in_project[data_file_uuid].add( cda_project_id[study_id] )
        containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

        for ancestor_project_id in containing_projects:
            cda_file_in_project[data_file_uuid].add( ancestor_project_id )

print( 'done.', file=sys.stderr )

# Load Specimen metadata.
print( f"[{get_current_timestamp()}] Loading sample metadata...", end='', file=sys.stderr )
specimen = load_tsv_as_dict( specimen_input_tsv )

print( 'done.', file=sys.stderr )

# Traverse DataFile<->Specimen associations and attach relevant metadata to corresponding CDA file records.
print( f"[{get_current_timestamp()}] Loading DataFile-Specimen associations and importing relevant metadata from associated samples...", end='', file=sys.stderr )
data_file_describes_specimen = map_columns_one_to_many( data_file_specimen_input_tsv, 'data_file_uuid', 'specimen_record_id' )
file_anatomic_site = dict()
file_tumor_vs_normal = dict()

for data_file_uuid in data_file_describes_specimen:
    for specimen_record_id in data_file_describes_specimen[data_file_uuid]:
        
        if specimen[specimen_record_id]['anatomical_collection_site'] is not None and specimen[specimen_record_id]['anatomical_collection_site'] != '':
            if data_file_uuid not in file_anatomic_site:
                file_anatomic_site[data_file_uuid] = set()
            file_anatomic_site[data_file_uuid].add( specimen[specimen_record_id]['anatomical_collection_site'] )

        tumor_normal_value = ''

        if specimen[specimen_record_id]['tissue_category'] is not None and specimen[specimen_record_id]['tissue_category'] != '':
            tumor_normal_value = specimen[specimen_record_id]['tissue_category']

        if tumor_normal_value != '':
            if data_file_uuid not in file_tumor_vs_normal:
                file_tumor_vs_normal[data_file_uuid] = set()
            file_tumor_vs_normal[data_file_uuid].add( tumor_normal_value )

print( 'done.', file=sys.stderr )

# Write the new CDA file records.
print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

with open( file_output_tsv, 'w' ) as OUT:
    print( *cda_file_fields, sep='\t', file=OUT )

    for file_id in sorted( cda_file_records ):
        output_row = list()
        file_record = cda_file_records[file_id]
        for cda_file_field in cda_file_fields:
            output_row.append( file_record[cda_file_field] )
        print( *output_row, sep='\t', file=OUT )

# Write the file<->project association.
with open( file_in_project_output_tsv, 'w' ) as OUT:
    print( *[ 'file_id', 'project_id' ], sep='\t', file=OUT )

    for file_id in sorted( cda_file_in_project ):
        for project_id in sorted( cda_file_in_project[file_id] ):
            print( *[ file_id, project_id ], sep='\t', file=OUT )

# Write file->anatomic_site.
with open( file_anatomic_site_output_tsv, 'w' ) as OUT:
    print( *[ 'file_id', 'anatomic_site' ], sep='\t', file=OUT )

    for file_id in sorted( file_anatomic_site ):
        for anatomic_site in sorted( file_anatomic_site[file_id] ):
            print( *[ file_id, anatomic_site ], sep='\t', file=OUT )

# Write file->tumor_vs_normal.
with open( file_tumor_vs_normal_output_tsv, 'w' ) as OUT:
    print( *[ 'file_id', 'tumor_vs_normal' ], sep='\t', file=OUT )

    for file_id in sorted( file_tumor_vs_normal ):
        for tumor_vs_normal in sorted( file_tumor_vs_normal[file_id] ):
            print( *[ file_id, tumor_vs_normal ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )



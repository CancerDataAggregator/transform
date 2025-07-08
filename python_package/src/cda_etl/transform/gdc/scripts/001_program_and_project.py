#!/usr/bin/env python3 -u

import re
import sys

from os import makedirs, path
from time import sleep

from cda_etl.lib import get_dbgap_study_metadata, load_tsv_as_dict, map_columns_one_to_one

# (OPTIONAL) ARGUMENT

refresh_dbgap_metadata = False

if len( sys.argv ) > 1:
    
    refresh_arg = sys.argv[1]

    if refresh_arg != 'refresh_dbgap':
        
        sys.exit( f"Usage: '{path.basename(__file__)}' or '{path.basename(__file__)} refresh_dbgap'" )

    else:
        
        refresh_dbgap_metadata = True

# PARAMETERS

upstream_data_source = 'GDC'

debug = True

# Extracted TSVs.

tsv_input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

program_input_tsv = path.join( tsv_input_root, 'program.tsv' )

project_input_tsv = path.join( tsv_input_root, 'project.tsv' )

project_in_program_input_tsv = path.join( tsv_input_root, 'project_in_program.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

project_output_tsv = path.join( tsv_output_root, 'project.tsv' )

project_in_project_output_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

upstream_identifiers_output_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

# ETL metadata.

aux_output_dir = path.join( 'auxiliary_metadata', '__dbGaP_supplemental_metadata' )

dbgap_study_cache = path.join( aux_output_dir, 'study_containment_metadata.tsv' )

# Table header sequences.

cda_project_fields = [
    
    'id',
    'crdc_id',
    'type',
    'name',
    'short_name'
]

dbgap_study_cache_fields = [
    
    'study_id',
    'parent_study_id',
    'substudy_ids',
    'name'
]

upstream_identifiers_fields = [
    
    'cda_table',
    'id',
    'upstream_source',
    'upstream_field',
    'upstream_id'
]

# Caches for loaded/processed data.

cda_project_records = dict()

cda_project_in_project = dict()

upstream_identifiers = dict()

cached_dbgap_data = dict()

# EXECUTION

for output_dir in [ tsv_output_root, aux_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Load any cached dbGaP metadata that exists. If `refresh_dbgap_metadata` is set to False,
# we'll use this cached metadata for any studies covered. New data will be loaded for
# previously-unseen study IDs regardless of the value of `refresh_dbgap_metadata`.

if path.exists( dbgap_study_cache ):
    
    cached_dbgap_data = load_tsv_as_dict( dbgap_study_cache )

# Load data for programs. Create separate CDA project records for all dbGaP studies listed as cross-references.

program = load_tsv_as_dict( program_input_tsv )

for program_id in program:
    
    program_name = program[program_id]['name']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.program.{program_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'program'

    cda_project_record['name'] = program_name

    cda_project_record['short_name'] = program_name

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['program.program_id'] = program_id

    upstream_identifiers[cda_id]['program.name'] = program_name

    hit_dbgap = False

    if program[program_id]['dbgap_accession_number'] != '':
        
        dbgap_study_accession = re.sub( r'^.*(phs[0-9]+)([^0-9].*)?$', r'\1', program[program_id]['dbgap_accession_number'] )

        upstream_identifiers[cda_id]['program.dbgap_accession_number'] = dbgap_study_accession

        cda_dbgap_id = f"dbGaP.study_accession.{dbgap_study_accession}"

        # Model programs as subprojects of the dbGaP studies which which they have been directly associated:

        if cda_id not in cda_project_in_project:
            
            cda_project_in_project[cda_id] = set()

        cda_project_in_project[cda_id].add( cda_dbgap_id )

        if cda_dbgap_id not in cda_project_records:
            
            cda_dbgap_project_record = dict()

            cda_dbgap_project_record['id'] = cda_dbgap_id

            # Until/unless CRDC mints its own IDs for dbGaP studies, this field should remain blank when documenting them.

            cda_dbgap_project_record['crdc_id'] = ''

            cda_dbgap_project_record['type'] = 'dbgap_study'

            if dbgap_study_accession in cached_dbgap_data and not refresh_dbgap_metadata:
                
                if debug:
                    
                    print( f"[{upstream_data_source} program '{program_name}']: Loading cached dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

                cda_dbgap_project_record['name'] = cached_dbgap_data[dbgap_study_accession]['name']

            else:
                
                if debug:
                    
                    print( f"[{upstream_data_source} program '{program_name}']: Importing dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

                dbgap_study_metadata = get_dbgap_study_metadata( dbgap_study_accession )

                hit_dbgap = True

                cda_dbgap_project_record['name'] = dbgap_study_metadata['study_name']

                # Save anything new to add to the cache file when we're finished.

                if dbgap_study_accession not in cached_dbgap_data:
                    
                    cached_dbgap_data[dbgap_study_accession] = dict()

                    cached_dbgap_data[dbgap_study_accession]['study_id'] = dbgap_study_accession

                    if 'parent_study_id' in dbgap_study_metadata:
                        
                        cached_dbgap_data[dbgap_study_accession]['parent_study_id'] = dbgap_study_metadata['parent_study_id']

                    if 'substudy_ids' in dbgap_study_metadata:
                        
                        cached_dbgap_data[dbgap_study_accession]['substudy_ids'] = repr( sorted( dbgap_study_metadata['substudy_ids'] ) )

                    cached_dbgap_data[dbgap_study_accession]['name'] = dbgap_study_metadata['study_name']

            if cda_dbgap_id not in upstream_identifiers:
                
                upstream_identifiers[cda_dbgap_id] = dict()

            upstream_identifiers[cda_dbgap_id]['study_accession'] = dbgap_study_accession

            upstream_identifiers[cda_dbgap_id]['study_name'] = cda_dbgap_project_record['name']

            if debug:
                
                print( 'done.', file=sys.stderr )

            cda_dbgap_project_record['short_name'] = dbgap_study_accession

            cda_project_records[cda_dbgap_id] = cda_dbgap_project_record

    # REMOVE ME IF POSSIBLE

    if hit_dbgap:
        
        sleep( 3 )

# Load data for projects. Create separate CDA project records for all dbGaP studies listed as cross-references.

project = load_tsv_as_dict( project_input_tsv )

for project_id in project:
    
    project_name = project[project_id]['name']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.project.{project_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'project'

    cda_project_record['name'] = project_name

    cda_project_record['short_name'] = project_id

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['project.project_id'] = project_id

    upstream_identifiers[cda_id]['project.name'] = project_name

    hit_dbgap = False

    if project[project_id]['dbgap_accession_number'] != '':
        
        dbgap_study_accession = re.sub( r'^.*(phs[0-9]+)([^0-9].*)?$', r'\1', project[project_id]['dbgap_accession_number'] )

        upstream_identifiers[cda_id]['project.dbgap_accession_number'] = dbgap_study_accession

        cda_dbgap_id = f"dbGaP.study_accession.{dbgap_study_accession}"

        # Model projects as subprojects of the dbGaP studies which which they have been directly associated:

        if cda_id not in cda_project_in_project:
            
            cda_project_in_project[cda_id] = set()

        cda_project_in_project[cda_id].add( cda_dbgap_id )

        if cda_dbgap_id not in cda_project_records:
            
            cda_dbgap_project_record = dict()

            cda_dbgap_project_record['id'] = cda_dbgap_id

            # Until/unless CRDC mints its own IDs for dbGaP studies, this field should remain blank when documenting them.

            cda_dbgap_project_record['crdc_id'] = ''

            cda_dbgap_project_record['type'] = 'dbgap_study'

            if dbgap_study_accession in cached_dbgap_data and not refresh_dbgap_metadata:
                
                if debug:
                    
                    print( f"[{upstream_data_source} project '{project_id}']: Loading cached dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

                cda_dbgap_project_record['name'] = cached_dbgap_data[dbgap_study_accession]['name']

            else:
                
                if debug:
                    
                    print( f"[{upstream_data_source} project '{project_id}']: Importing dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

                dbgap_study_metadata = get_dbgap_study_metadata( dbgap_study_accession )

                hit_dbgap = True

                cda_dbgap_project_record['name'] = dbgap_study_metadata['study_name']

                # Save anything new to add to the cache file when we're finished.

                if dbgap_study_accession not in cached_dbgap_data:
                    
                    cached_dbgap_data[dbgap_study_accession] = dict()

                    cached_dbgap_data[dbgap_study_accession]['study_id'] = dbgap_study_accession

                    if 'parent_study_id' in dbgap_study_metadata:
                        
                        cached_dbgap_data[dbgap_study_accession]['parent_study_id'] = dbgap_study_metadata['parent_study_id']

                    if 'substudy_ids' in dbgap_study_metadata:
                        
                        cached_dbgap_data[dbgap_study_accession]['substudy_ids'] = repr( sorted( dbgap_study_metadata['substudy_ids'] ) )

                    cached_dbgap_data[dbgap_study_accession]['name'] = dbgap_study_metadata['study_name']

            if cda_dbgap_id not in upstream_identifiers:
                
                upstream_identifiers[cda_dbgap_id] = dict()

            upstream_identifiers[cda_dbgap_id]['study_accession'] = dbgap_study_accession

            upstream_identifiers[cda_dbgap_id]['study_name'] = cda_dbgap_project_record['name']

            if debug:
                
                print( 'done.', file=sys.stderr )

            cda_dbgap_project_record['short_name'] = dbgap_study_accession

            cda_project_records[cda_dbgap_id] = cda_dbgap_project_record

    # REMOVE ME IF POSSIBLE

    if hit_dbgap:
        
        sleep( 3 )

# Load study-substudy containment metadata as scraped from dbGaP for all study IDs modeled as CDA projects.

# dbgap_study_cache_fields = [
#     
#     'study_id',
#     'parent_study_id',
#     'substudy_ids',
#     'name'
# ]

for dbgap_study_accession in sorted( cached_dbgap_data ):
    
    cda_dbgap_id = f"dbGaP.study_accession.{dbgap_study_accession}"

    if 'parent_study_id' in cached_dbgap_data[dbgap_study_accession]:
        
        parent_study_accession = cached_dbgap_data[dbgap_study_accession]['parent_study_id']

        cda_parent_dbgap_id = f"dbGaP.study_accession.{parent_study_accession}"

        if cda_parent_dbgap_id in cda_project_records:
            
            if cda_dbgap_id not in cda_project_in_project:
                
                cda_project_in_project[cda_dbgap_id] = set()

            cda_project_in_project[cda_dbgap_id].add( cda_parent_dbgap_id )

    if 'substudy_ids' in cached_dbgap_data[dbgap_study_accession]:
        
        substudy_id_list_string = cached_dbgap_data[dbgap_study_accession]['substudy_ids']

        substudy_ids = substudy_id_list_string.strip( r'[]' ).strip( "'" ).split( "', '" )

        for substudy_id in substudy_ids:
            
            cda_substudy_dbgap_id = f"dbGaP.study_accession.{substudy_id}"

            if cda_substudy_dbgap_id in cda_project_records:
                
                if cda_substudy_dbgap_id not in cda_project_in_project:
                    
                    cda_project_in_project[cda_substudy_dbgap_id] = set()

                cda_project_in_project[cda_substudy_dbgap_id].add( cda_dbgap_id )

# Load project-in-program containment metadata as published.

project_in_program = map_columns_one_to_one( project_in_program_input_tsv, 'project_id', 'program_id' )

for project_id in project_in_program:
    
    cda_project_id = f"{upstream_data_source}.project.{project_id}"

    program_id = project_in_program[project_id]

    cda_program_id = f"{upstream_data_source}.program.{program_id}"

    if cda_project_id not in cda_project_in_project:
        
        cda_project_in_project[cda_project_id] = set()

    cda_project_in_project[cda_project_id].add( cda_program_id )

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
        
        print_upstream_data_source = upstream_data_source

        if re.search( r'^dbGaP\.study_accession\.', cda_project_id ) is not None:
            
            print_upstream_data_source = 'dbGaP'

        for field_name in sorted( upstream_identifiers[cda_project_id] ):
            
            value = upstream_identifiers[cda_project_id][field_name]

            print( *[ 'project', cda_project_id, print_upstream_data_source, field_name, value ], sep='\t', file=OUT )

# Save updated dbGaP metadata.

with open( dbgap_study_cache, 'w' ) as OUT:
    
    print( *dbgap_study_cache_fields, sep='\t', file=OUT )

    for study_id in sorted( cached_dbgap_data ):
        
        output_row = list()

        for field_name in dbgap_study_cache_fields:
            
            if field_name in cached_dbgap_data[study_id]:
                
                output_row.append( cached_dbgap_data[study_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', file=OUT )



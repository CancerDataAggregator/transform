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

upstream_data_source = 'PDC'

debug = True

# Unmodified extracted data, converted directly from JSON to TSV.

extracted_tsv_input_root = path.join( 'extracted_data', 'pdc' )

reference_input_tsv = path.join( extracted_tsv_input_root, 'Reference', 'Reference.tsv' )

study_version_input_tsv = path.join( extracted_tsv_input_root, 'StudyCatalog', 'StudyCatalog.versions.tsv' )

# Collated version of the previous dataset: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated; cases without containing studies removed;
# groups of multiple files sharing the same FileMetadata.file_location removed.

collated_tsv_input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

program_input_tsv = path.join( collated_tsv_input_root, 'Program', 'Program.tsv' )

project_input_tsv = path.join( collated_tsv_input_root, 'Project', 'Project.tsv' )

program_has_project_input_tsv = path.join( collated_tsv_input_root, 'Program', 'Program.project_id.tsv' )

study_input_tsv = path.join( collated_tsv_input_root, 'Study', 'Study.tsv' )

project_has_study_input_tsv = path.join( collated_tsv_input_root, 'Project', 'Project.study_id.tsv' )

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

# Load data for programs.

program = load_tsv_as_dict( program_input_tsv )

for program_id in program:
    
    program_name = program[program_id]['name']

    program_submitter_id = program[program_id]['program_submitter_id']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.Program.{program_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'program'

    cda_project_record['name'] = program_name

    cda_project_record['short_name'] = program_submitter_id

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['Program.program_id'] = program_id

    upstream_identifiers[cda_id]['Program.name'] = program_name

    upstream_identifiers[cda_id]['Program.program_submitter_id'] = program_submitter_id

# Load data for projects.

project = load_tsv_as_dict( project_input_tsv )

for project_id in project:
    
    project_name = project[project_id]['name']

    project_submitter_id = project[project_id]['project_submitter_id']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.Project.{project_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'project'

    cda_project_record['name'] = project_name

    cda_project_record['short_name'] = project_submitter_id

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['Project.project_id'] = project_id

    upstream_identifiers[cda_id]['Project.project_submitter_id'] = project_submitter_id

# Load data for studies.

study = load_tsv_as_dict( study_input_tsv )

pdc_study_id_to_study_id = dict()

for study_id in study:
    
    study_submitter_id = study[study_id]['study_submitter_id']

    study_pdc_study_id = study[study_id]['pdc_study_id']

    pdc_study_id_to_study_id[study_pdc_study_id] = study_id

    study_submitter_id_name = study[study_id]['submitter_id_name']

    study_name = study[study_id]['study_name']

    study_shortname = study[study_id]['study_shortname']

    cda_project_record = dict()

    cda_id = f"{upstream_data_source}.Study.{study_id}"

    cda_project_record['id'] = cda_id

    # This is a placeholder field: these IDs have not yet been minted by CRDC as of August 2024.

    cda_project_record['crdc_id'] = ''

    cda_project_record['type'] = 'study'

    cda_project_record['name'] = study_name

    cda_project_record['short_name'] = study_pdc_study_id

    cda_project_records[cda_id] = cda_project_record

    if cda_id not in upstream_identifiers:
        
        upstream_identifiers[cda_id] = dict()

    upstream_identifiers[cda_id]['Study.study_id'] = study_id

    upstream_identifiers[cda_id]['Study.study_submitter_id'] = study_submitter_id

    upstream_identifiers[cda_id]['Study.pdc_study_id'] = study_pdc_study_id

    upstream_identifiers[cda_id]['Study.submitter_id_name'] = study_submitter_id_name

    upstream_identifiers[cda_id]['Study.study_name'] = study_name

    upstream_identifiers[cda_id]['Study.study_shortname'] = study_shortname

# Load IDs for obsolete Study records and point them to IDs for their latest versions.
# The information we consume after this section, from Reference.tsv, sometimes points
# to stale versions.

study_version = load_tsv_as_dict( study_version_input_tsv, id_column_count=2 )

latest_version = dict()

for pdc_study_id in study_version:
    
    for version_number in study_version[pdc_study_id]:
        
        if study_version[pdc_study_id][version_number]['is_latest_version'] == 'no':
            
            if pdc_study_id not in pdc_study_id_to_study_id:
                
                sys.exit( f"FATAL: Can't load current-version study_id for pdc_study_id '{pdc_study_id}' referenced in StudyCatalog.versions.tsv; aborting." )

            else:
                
                latest_version[study_version[pdc_study_id][version_number]['study_id']] = pdc_study_id_to_study_id[pdc_study_id]

# Create separate CDA project records for all dbGaP studies listed as cross-references.

reference = load_tsv_as_dict( reference_input_tsv )

for reference_id in reference:
    
    if reference[reference_id]['reference_resource_shortname'].lower() == 'dbgap':
        
        if reference[reference_id]['reference_entity_type'] != 'study':
            
            print( f"WARNING: Found dbgap annotation for unexpected reference_entity_type '{reference[reference_id]['reference_entity_type']}' in Reference.tsv, record {reference_id} -- please investigate.", file=sys.stderr )

        else:
            
            study_id = reference[reference_id]['entity_id']

            skip = False

            if study_id not in study:
                
                if study_id in latest_version:
                    
                    new_study_id = latest_version[study_id]

                    if new_study_id in study:
                        
                        study_id = new_study_id

                    else:
                        
                        sys.exit( f"FATAL: Apparently nonexistent study_id '{new_study_id}' should have been populated when pdc_study_id_to_study_id was first populated -- this should never happen. Cannot continue, aborting." )

                else:
                    
                    print( f"Warning: Reference.tsv record {reference_id} links dbgap annotation to nonexistent/unresolvable study_id '{study_id}' -- skipping annotation.", file=sys.stderr )

                    skip = True

            if not skip:
                
                dbgap_study_accession = re.sub( r'^.*(phs[0-9]+)([^0-9].*)?$', r'\1', reference[reference_id]['reference_entity_alias'] )

                hit_dbgap = False

                cda_dbgap_id = f"dbGaP.study_accession.{dbgap_study_accession}"

                # Model studies as subprojects of the dbGaP studies which which they have been directly associated:

                cda_id = f"{upstream_data_source}.Study.{study_id}"

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
                            
                            print( f"[{upstream_data_source} study '{study_id}']: Loading cached dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

                        cda_dbgap_project_record['name'] = cached_dbgap_data[dbgap_study_accession]['name']

                    else:
                        
                        if debug:
                            
                            print( f"[{upstream_data_source} study '{study_id}']: Importing dbGaP metadata for {dbgap_study_accession}...", end='', file=sys.stderr )

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

# Load project-in-program containment metadata as published from
# program_has_project_input_tsv.

project_in_program = map_columns_one_to_one( program_has_project_input_tsv, 'project_id', 'program_id' )

for project_id in project_in_program:
    
    cda_project_id = f"{upstream_data_source}.Project.{project_id}"

    program_id = project_in_program[project_id]

    cda_program_id = f"{upstream_data_source}.Program.{program_id}"

    if cda_project_id not in cda_project_in_project:
        
        cda_project_in_project[cda_project_id] = set()

    cda_project_in_project[cda_project_id].add( cda_program_id )

# Load study-in-project containment metadata as published from
# project_has_study_input_tsv.

study_in_project = map_columns_one_to_one( project_has_study_input_tsv, 'study_id', 'project_id' )

for study_id in study_in_project:
    
    cda_study_id = f"{upstream_data_source}.Study.{study_id}"

    project_id = study_in_project[study_id]

    cda_project_id = f"{upstream_data_source}.Project.{project_id}"

    if cda_study_id not in cda_project_in_project:
        
        cda_project_in_project[cda_study_id] = set()

    cda_project_in_project[cda_study_id].add( cda_project_id )

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



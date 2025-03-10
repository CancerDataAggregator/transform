#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'PDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated; cases without containing studies removed;
# groups of multiple files sharing the same FileMetadata.file_location removed.

tsv_input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

file_input_tsv = path.join( tsv_input_root, 'File', 'File.tsv' )

file_study_input_tsv = path.join( tsv_input_root, 'File', 'File.study_id.tsv' )

file_aliquot_input_tsv = path.join( tsv_input_root, 'File', 'File.aliquot_id.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'Sample', 'Sample.tsv' )

sample_aliquot_input_tsv = path.join( tsv_input_root, 'Sample', 'Sample.aliquot_id.tsv' )

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

upstream_identifiers_fields = [
    
    'cda_table',
    'id',
    'data_source',
    'data_source_id_field_name',
    'data_source_id_value'
]

debug = False

# EXECUTION

print( f"[{get_current_timestamp()}] Loading file metadata...", end='', file=sys.stderr )

# Load metadata for files.

cda_file_records = dict()

file = load_tsv_as_dict( file_input_tsv )

for file_id in file:
    
    cda_file_records[file_id] = dict()

    cda_file_records[file_id]['id'] = file_id
    cda_file_records[file_id]['crdc_id'] = ''
    cda_file_records[file_id]['name'] = file[file_id]['file_name']
    cda_file_records[file_id]['description'] = ''
    cda_file_records[file_id]['drs_uri'] = f"drs://dg.4dfc:{file_id}"
    cda_file_records[file_id]['access'] = file[file_id]['access']
    cda_file_records[file_id]['size'] = file[file_id]['file_size']
    cda_file_records[file_id]['format'] = file[file_id]['file_format']
    cda_file_records[file_id]['type'] = file[file_id]['file_type']
    cda_file_records[file_id]['category'] = file[file_id]['data_category']

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file<->study_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Load CDA IDs for studies, projects and programs. Don't use any of the canned loader
# functions to load this data structure; it's got multiplicities that are
# generally mishandled if certain keys are assumed to be unique (e.g. a CDA
# subject can have multiple case_id values from the same data source, rendering any attempt
# to key this information on the first X columns incorrect or so cumbersome as to
# be pointless).

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
                    
                    sys.exit( f"FATAL: {upstream_data_source} study_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'Project.project_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Project.project_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} project_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'Program.program_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Program.program_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Compute CDA file<->project relationships.

cda_file_in_project = dict()

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Associating files with projects...", end='', file=sys.stderr )

file_study = map_columns_one_to_many( file_study_input_tsv, 'file_id', 'study_id' )

for file_id in file_study:
    
    for study_id in file_study[file_id]:
        
        if study_id not in cda_project_id:
            
            sys.exit( f"FATAL: File {file_id} associated with study_id {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

        if file_id not in cda_file_in_project:
            
            cda_file_in_project[file_id] = set()

        cda_file_in_project[file_id].add( cda_project_id[study_id] )

        containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

        for ancestor_project_id in containing_projects:
            
            cda_file_in_project[file_id].add( ancestor_project_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading sample hierarchy and sample metadata...", end='', file=sys.stderr )

aliquot_of_sample = map_columns_one_to_one( sample_aliquot_input_tsv, 'aliquot_id', 'sample_id' )

file_describes_aliquot = map_columns_one_to_many( file_aliquot_input_tsv, 'file_id', 'aliquot_id' )

# Load sample metadata.

sample = load_tsv_as_dict( sample_input_tsv )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file-aliquot associations and importing relevant metadata from associated samples...", end='', file=sys.stderr )

# Traverse file<->sample associations and attach relevant metadata to corresponding CDA file records.

file_anatomic_site = dict()

file_tumor_vs_normal = dict()

warning_files = set()

for file_id in file_describes_aliquot:
    
    ancestor_samples = set()

    for aliquot_id in file_describes_aliquot[file_id]:
        
        sample_id = aliquot_of_sample[aliquot_id]

        ancestor_samples.add( sample_id )

    if len( ancestor_samples ) > 1:
        
        warning_files.add( file_id )

    for ancestor_sample_id in ancestor_samples:
        
        if sample[ancestor_sample_id]['biospecimen_anatomic_site'] != '':
            
            if file_id not in file_anatomic_site:
                
                file_anatomic_site[file_id] = set()

            file_anatomic_site[file_id].add( sample[ancestor_sample_id]['biospecimen_anatomic_site'] )

        if sample[ancestor_sample_id]['tissue_type'] != '':
            
            if file_id not in file_tumor_vs_normal:
                
                file_tumor_vs_normal[file_id] = set()

            file_tumor_vs_normal[file_id].add( sample[ancestor_sample_id]['tissue_type'] )

if debug and len( warning_files ) > 0:
    
    print( f"\n\n   NOTE: processed {len( warning_files )} files associated with multiple distinct sample records; at most this many files may have multiple values for sample-derived fields like anatomic_site and tumor_vs_normal.\n\n...done.", file=sys.stderr )

else:
    
    print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA file records.

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



#!/usr/bin/env python3 -u

import json
import re
import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CDS'

# Unmodified extracted data, converted directly from a Neo4j JSONL dump.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

file_input_tsv = path.join( tsv_input_root, 'file.tsv' )

study_input_tsv = path.join( tsv_input_root, 'study.tsv' )

file_study_input_tsv = path.join( tsv_input_root, 'file_from_study.tsv' )

genomic_info_input_tsv = path.join( tsv_input_root, 'genomic_info.tsv' )

genomic_info_file_input_tsv = path.join( tsv_input_root, 'genomic_info_of_file.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample.tsv' )

file_sample_input_tsv = path.join( tsv_input_root, 'file_from_sample.tsv' )

image_input_tsv = path.join( tsv_input_root, 'image.tsv' )

image_file_input_tsv = path.join( tsv_input_root, 'image_of_file.tsv' )

# Load the DICOM controlled-term code map to decode image.image_modality values.

dicom_code_map_dir = path.join( 'auxiliary_metadata', '__DICOM_code_maps' )

dicom_cv_map = path.join( dicom_code_map_dir, 'DICOM_controlled_terms.tsv' )

image_modality_code_map = map_columns_one_to_one( dicom_cv_map, 'Code Value', 'Code Meaning' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

file_output_tsv = path.join( tsv_output_root, 'file.tsv' )

file_anatomic_site_output_tsv = path.join( tsv_output_root, 'file_anatomic_site.tsv' )

file_tumor_vs_normal_output_tsv = path.join( tsv_output_root, 'file_tumor_vs_normal.tsv' )

file_in_project_output_tsv = path.join( tsv_output_root, 'file_in_project.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_value_output_dir = path.join( aux_output_root, 'values' )

file_genomic_info_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_file_genmomic_info_clashes.library_strategy.tsv" )

# Table header sequences.

cda_file_fields = [
    
    'id',
    'crdc_id',
    'name',
    'description',
    'drs_uri',
    'access',
    'size',
    'checksum_type',
    'checksum_value',
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

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

for output_dir in [ aux_value_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Loading library_strategy metadata from genomic_info...", end='', file=sys.stderr )

file_uuid_genomic_info_uuid = map_columns_one_to_many( genomic_info_file_input_tsv, 'file_uuid', 'genomic_info_uuid' )

genomic_info_library_strategy = map_columns_one_to_one( genomic_info_input_tsv, 'uuid', 'library_strategy' )

# Pre-load file.data_category values from genomic_info.library_strategy in associated genomic_info records.

file_uuid_data_category = dict()

# Record conflicts.

genomic_info_library_strategy_data_clashes = dict()

for file_uuid in file_uuid_genomic_info_uuid:
    
    for genomic_info_uuid in file_uuid_genomic_info_uuid[file_uuid]:
        
        current_library_strategy = genomic_info_library_strategy[genomic_info_uuid]

        if file_uuid not in file_uuid_data_category or file_uuid_data_category[file_uuid] == '':
            
            file_uuid_data_category[file_uuid] = current_library_strategy

        elif file_uuid_data_category[file_uuid] != current_library_strategy and current_library_strategy != '':
            
            # Set up comparison and clash-tracking data structures.

            original_value = file_uuid_data_category[file_uuid]

            new_value = current_library_strategy

            if file_uuid not in genomic_info_library_strategy_data_clashes:
                
                genomic_info_library_strategy_data_clashes[file_uuid] = dict()

            if original_value not in genomic_info_library_strategy_data_clashes[file_uuid]:
                
                genomic_info_library_strategy_data_clashes[file_uuid][original_value] = dict()

            # Does the existing value match a pattern we know will be deleted later?

            if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                
                # Is the new value any better?

                if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                    
                    # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                    genomic_info_library_strategy_data_clashes[file_uuid][original_value][new_value] = False

                else:
                    
                    # Replace the old value with the new one.

                    file_uuid_data_category[file_uuid] = new_value

                    genomic_info_library_strategy_data_clashes[file_uuid][original_value][new_value] = True

            else:
                
                # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                genomic_info_library_strategy_data_clashes[file_uuid][original_value][new_value] = False

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file metadata...", end='', file=sys.stderr )

# Load metadata for files.

cda_file_records = dict()

file = load_tsv_as_dict( file_input_tsv )

for file_uuid in file:
    
    file_id = file[file_uuid]['file_id']

    cda_file_records[file_id] = dict()

    cda_file_records[file_id]['id'] = file_id
    cda_file_records[file_id]['crdc_id'] = ''
    cda_file_records[file_id]['name'] = file[file_uuid]['file_name']
    cda_file_records[file_id]['description'] = ''
    cda_file_records[file_id]['drs_uri'] = f"drs://{file_id.lower()}"
    # Will be filled in later from study.study_access:
    cda_file_records[file_id]['access'] = ''
    cda_file_records[file_id]['size'] = file[file_uuid]['file_size']
    cda_file_records[file_id]['checksum_type'] = 'md5'
    cda_file_records[file_id]['checksum_value'] = file[file_uuid]['md5sum']
    cda_file_records[file_id]['format'] = file[file_uuid]['file_type']
    # Will be filled in later from image.image_modality:
    cda_file_records[file_id]['type'] = ''
    cda_file_records[file_id]['category'] = '' if ( file_uuid not in file_uuid_data_category or file_uuid_data_category[file_uuid] is None ) else file_uuid_data_category[file_uuid]

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file<->study_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Load CDA IDs for studies and programs. Don't use any of the canned loader
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
        
        if 'study.uuid' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['study.uuid']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} study.uuid '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'program.uuid' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['program.uuid']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program.uuid '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Compute CDA file<->project relationships.

cda_file_in_project = dict()

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Associating files with projects...", end='', file=sys.stderr )

file_study = map_columns_one_to_many( file_study_input_tsv, 'file_uuid', 'study_uuid' )

study = load_tsv_as_dict( study_input_tsv )

for file_uuid in file_study:
    
    file_id = file[file_uuid]['file_id']

    for study_uuid in file_study[file_uuid]:
        
        if study_uuid not in cda_project_id:
            
            sys.exit( f"FATAL: File {file_uuid} associated with study.uuid {study_uuid}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

        # Import access metadata and propagate to file records.

        if study[study_uuid]['study_access'] != '':
            
           cda_file_records[file_id]['access'] = study[study_uuid]['study_access'] 

        if file_id not in cda_file_in_project:
            
            cda_file_in_project[file_id] = set()

        cda_file_in_project[file_id].add( cda_project_id[study_uuid] )

        containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_uuid] )

        for ancestor_project_id in containing_projects:
            
            cda_file_in_project[file_id].add( ancestor_project_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading sample metadata...", end='', file=sys.stderr )

# Load sample metadata.

sample = load_tsv_as_dict( sample_input_tsv )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file-sample associations and importing relevant metadata from associated samples...", end='', file=sys.stderr )

file_describes_sample = map_columns_one_to_many( file_sample_input_tsv, 'file_uuid', 'sample_uuid' )

# Traverse file<->sample associations and attach relevant metadata to corresponding CDA file records.

file_anatomic_site = dict()

file_tumor_vs_normal = dict()

for file_uuid in file_describes_sample:
    
    file_id = file[file_uuid]['file_id']

    for sample_uuid in file_describes_sample[file_uuid]:
        
        if sample[sample_uuid]['sample_anatomic_site'] != '':
            
            if file_id not in file_anatomic_site:
                
                file_anatomic_site[file_id] = set()

            file_anatomic_site[file_id].add( sample[sample_uuid]['sample_anatomic_site'] )

        if sample[sample_uuid]['sample_tumor_status'] != '':
            
            if file_id not in file_tumor_vs_normal:
                
                file_tumor_vs_normal[file_id] = set()

            file_tumor_vs_normal[file_id].add( sample[sample_uuid]['sample_tumor_status'] )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading image metadata and updating file.type, anatomy and tumor-status metadata...", end='', file=sys.stderr )

image = load_tsv_as_dict( image_input_tsv )

image_of_file = map_columns_one_to_many( image_file_input_tsv, 'image_uuid', 'file_uuid' )

for image_uuid in image_of_file:
    
    organ_or_tissue = image[image_uuid]['organ_or_tissue']

    # CDS removed this field from their `image` entity between October and December 2024.
    # 
    # tumor_tissue_type = image[image_uuid]['tumor_tissue_type']
    tumor_tissue_type = ''

    image_modality = image[image_uuid]['image_modality']

    if image_modality in image_modality_code_map:
        
        image_modality = image_modality_code_map[image_modality]

    else:
        
        image_modality = ''

    for file_uuid in image_of_file[image_uuid]:
        
        file_id = file[file_uuid]['file_id']

        if image_modality != '':
            
            cda_file_records[file_id]['type'] = image_modality

        if organ_or_tissue != '':
            
            if file_id not in file_anatomic_site:
                
                file_anatomic_site[file_id] = set()

            file_anatomic_site[file_id].add( organ_or_tissue )

        if tumor_tissue_type != '':
            
            if file_id not in file_tumor_vs_normal:
                
                file_tumor_vs_normal[file_id] = set()

            file_tumor_vs_normal[file_id].add( tumor_tissue_type )

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

# Log data clashes within the same file across multiple associated genomic_info records with respect to the genomic_info.library_strategy field.

# genomic_info_library_strategy_data_clashes[file_uuid][original_value][new_value] = True

with open( file_genomic_info_data_clash_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}_file_uuid", f"{upstream_data_source}_genomic_info_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for file_uuid in sorted( genomic_info_library_strategy_data_clashes ):
        
        field_name = 'library_strategy'

        for original_value in sorted( genomic_info_library_strategy_data_clashes[file_uuid] ):
            
            for clashing_value in sorted( genomic_info_library_strategy_data_clashes[file_uuid][original_value] ):
                
                if genomic_info_library_strategy_data_clashes[file_uuid][original_value][clashing_value] == False:
                    
                    print( *[ file_uuid, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                else:
                    
                    print( *[ file_uuid, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )



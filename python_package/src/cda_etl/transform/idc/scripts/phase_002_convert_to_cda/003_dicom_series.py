#!/usr/bin/env python3 -u

import gzip
import json
import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'IDC'

# Extracted data, converted from JSONL to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

series_input_tsv = path.join( tsv_input_root, 'dicom_series.tsv' )

series_type_input_tsv = path.join( tsv_input_root, 'dicom_series.dicom_all.SOPClassUID.tsv' )

series_anatomy_input_tsv = path.join( tsv_input_root, 'dicom_series.dicom_all.anatomy_values_and_instance_counts.tsv' )

series_tumor_vs_normal_input_tsv = path.join( tsv_input_root, 'dicom_series.dicom_all.tumor_vs_normal_values_and_instance_counts.tsv' )

series_specimen_xref_input_tsv = path.join( tsv_input_root, 'dicom_series.dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.tsv' )

extramural_specimen_annotation_input_tsv = path.join( 'auxiliary_metadata', '__IDC_supplemental_metadata', 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.crossrefs_resolved.anatomic_and_tumor_vs_normal_values_by_source.tsv' )

# Load the DICOM code maps to decode Modality and SOPClassUID values.

dicom_code_map_dir = path.join( 'auxiliary_metadata', '__DICOM_code_maps' )

dicom_cv_map_tsv = path.join( dicom_code_map_dir, 'DICOM_controlled_terms.tsv' )

modality_code_map = map_columns_one_to_one( dicom_cv_map_tsv, 'Code Value', 'Code Meaning' )

dicom_class_map_tsv = path.join( dicom_code_map_dir, 'SOPClassUID.tsv' )

class_code_map = map_columns_one_to_one( dicom_class_map_tsv, 'SOP Class UID', 'SOP Class Name' )

retired_dicom_class_map_tsv = path.join( dicom_code_map_dir, 'retired_SOPClassUID.tsv' )

retired_class_code_map = map_columns_one_to_one( retired_dicom_class_map_tsv, 'SOP Class UID', 'SOP Class Name' )

for class_uid in retired_class_code_map:
    
    if class_uid not in class_code_map:
        
        class_code_map[class_uid] = retired_class_code_map[class_uid]

    else:
        
        sys.exit( f"FATAL: SOPClassUID code '{class_uid}' mapped in both active and retired DICOM metadata references. Please check; aborting." )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

dicom_series_output_tsv = path.join( tsv_output_root, 'dicom_series.tsv' )

dicom_series_instance_output_tsv = path.join( tsv_output_root, 'dicom_series_instance.tsv.gz' )

dicom_series_anatomic_site_output_tsv = path.join( tsv_output_root, 'dicom_series_anatomic_site.tsv' )

dicom_series_tumor_vs_normal_output_tsv = path.join( tsv_output_root, 'dicom_series_tumor_vs_normal.tsv' )

dicom_series_in_project_output_tsv = path.join( tsv_output_root, 'dicom_series_in_project.tsv' )

dicom_series_type_output_tsv = path.join( tsv_output_root, 'dicom_series_type.tsv' )

# Table header sequences.

cda_dicom_series_fields = [
    
    'id',
    'description',
    'access',
    'size',
    'checksum_type',
    'format',
    'category',
    'instance_count'
]

cda_dicom_series_instance_fields = [
    
    'id',
    'crdc_id',
    'dicom_series_id',
    'name',
    'drs_uri',
    'size',
    'checksum_value',
    'type'
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

if not path.exists( tsv_output_root ):
    
    makedirs( tsv_output_root )

print( f"[{get_current_timestamp()}] Loading dicom_series<->collection_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Load CDA IDs for collections and programs. Don't use any of the canned loader
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
    
    # Some of these may be from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.

    if upstream_data_source in upstream_identifiers['project'][project_id]:
        
        if 'original_collections_metadata.collection_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['original_collections_metadata.collection_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} original_collections_metadata.collection_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'original_collections_metadata.Program' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['original_collections_metadata.Program']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} original_collections_metadata.Program '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

print( f"...done.", file=sys.stderr )

print( f"[{get_current_timestamp()}] Collecting and encoding all DICOM-series-level metadata to CDA dicom_series records and caching project affiliations...", end='', file=sys.stderr )

cda_dicom_series_records = dict()

cda_dicom_series_in_project = dict()

dicom_series = load_tsv_as_dict( series_input_tsv )

warned_modality_codes = set()

for series_id in dicom_series:
    
    cda_dicom_series_records[series_id] = dict()

    cda_dicom_series_records[series_id]['id'] = series_id
    cda_dicom_series_records[series_id]['description'] = dicom_series[series_id]['SeriesDescription']
    cda_dicom_series_records[series_id]['access'] = 'Open'
    cda_dicom_series_records[series_id]['size'] = dicom_series[series_id]['total_byte_size']
    cda_dicom_series_records[series_id]['checksum_type'] = 'md5'
    cda_dicom_series_records[series_id]['format'] = 'DICOM'

    modality_code = dicom_series[series_id]['Modality']

    modality_value = ''

    if modality_code not in modality_code_map and modality_code not in warned_modality_codes:
        
        newline_prefix = ''

        if len( warned_modality_codes ) == 0:
            
            newline_prefix = '\n'

        print( f"{newline_prefix}\n   WARNING: Unknown dicom_all.Modality code '{modality_code}' encountered for crdc_series_uuid '{series_id}'; saving as null and suppressing future warnings for the same value.", file=sys.stderr )

        warned_modality_codes.add( modality_code )

    elif modality_code not in warned_modality_codes:
        
        modality_value = modality_code_map[modality_code]

    cda_dicom_series_records[series_id]['category'] = modality_value
    cda_dicom_series_records[series_id]['instance_count'] = dicom_series[series_id]['instance_count']

    collection_id = dicom_series[series_id]['collection_id']

    if collection_id not in cda_project_id:
        
        sys.exit( f"FATAL: crdc_series_uuid '{series_id}' associated with collection_id '{collection_id}', which is not represented in the CDA project table. Cannot continue; please investigate. Aborting." )

    else:
        
        if series_id not in cda_dicom_series_in_project:
            
            cda_dicom_series_in_project[series_id] = set()

        cda_dicom_series_in_project[series_id].add( cda_project_id[collection_id] )

        containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[collection_id] )

        for ancestor_project_id in containing_projects:
            
            cda_dicom_series_in_project[series_id].add( ancestor_project_id )

newline_prefix = ''

if len( warned_modality_codes ) > 0:
    
    newline_prefix = '\n...'

print( f"{newline_prefix}done. [{get_current_timestamp()}]", file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading dicom_series_type metadata...", end='', file=sys.stderr )

dicom_series_type = load_tsv_as_dict( series_type_input_tsv, id_column_count=2 )

cda_dicom_series_type = dict()

warned_class_codes = set()

for series_id in dicom_series_type:
    
    for class_code in dicom_series_type[series_id]:
        
        if class_code not in class_code_map and class_code not in warned_class_codes:
            
            newline_prefix = ''

            if len( warned_class_codes ) == 0:
                
                newline_prefix = '\n'

            print( f"{newline_prefix}\n   WARNING: Unknown SOPClassUID code '{class_code}' encountered for crdc_series_uuid '{series_id}'; saving no association record and suppressing future warnings for the same value.", file=sys.stderr )

            warned_class_codes.add( class_code )

        elif class_code not in warned_class_codes:
            
            class_value = class_code_map[class_code]

            if series_id not in cda_dicom_series_type:
                
                cda_dicom_series_type[series_id] = dict()

            cda_dicom_series_type[series_id][class_value] = dicom_series_type[series_id][class_code]['number_of_matching_SOPInstanceUIDs']

newline_prefix = ''

if len( warned_class_codes ) > 0:
    
    newline_prefix = '\n...'

print( f"{newline_prefix}done. [{get_current_timestamp()}]", file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading anatomic_site metadata for DICOM series from dicom_all and from cross-referenced sample metadata from other DCs...", end='', file=sys.stderr )

# Load relationship between DICOM series and extramural sample-ID cross-references (submitter IDs for specimens).

sample_to_series = map_columns_one_to_many( series_specimen_xref_input_tsv, 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier', 'crdc_series_uuid' )

dicom_series_anatomic_site = map_columns_one_to_many( series_anatomy_input_tsv, 'crdc_series_uuid', 'value' )

with open( extramural_specimen_annotation_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        if record['annotation_type'] == 'anatomy':
            
            sample_id = record['SpecimenDescriptionSequence.SpecimenIdentifier']

            if sample_id in sample_to_series:
                
                anatomy_value = record['remote_sample_field_value']

                for series_id in sample_to_series[sample_id]:
                    
                    if series_id not in dicom_series_anatomic_site:
                        
                        dicom_series_anatomic_site[series_id] = set()

                    dicom_series_anatomic_site[series_id].add( anatomy_value )

print( f"done. [{get_current_timestamp()}]", file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading tumor_vs_normal metadata for DICOM series from dicom_all and from cross-referenced sample metadata from other DCs...", end='', file=sys.stderr )

dicom_series_tumor_vs_normal = map_columns_one_to_many( series_tumor_vs_normal_input_tsv, 'crdc_series_uuid', 'value' )

with open( extramural_specimen_annotation_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        if record['annotation_type'] == 'tumor_vs_normal':
            
            sample_id = record['SpecimenDescriptionSequence.SpecimenIdentifier']

            if sample_id in sample_to_series:
                
                tumor_vs_normal_value = record['remote_sample_field_value']

                for series_id in sample_to_series[sample_id]:
                    
                    if series_id not in dicom_series_tumor_vs_normal:
                        
                        dicom_series_tumor_vs_normal[series_id] = set()

                    dicom_series_tumor_vs_normal[series_id].add( tumor_vs_normal_value )

print( f"done. [{get_current_timestamp()}]", file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA dicom_series records.

with open( dicom_series_output_tsv, 'w' ) as OUT:
    
    print( *cda_dicom_series_fields, sep='\t', file=OUT )

    for dicom_series_id in sorted( cda_dicom_series_records ):
        
        output_row = list()

        dicom_series_record = cda_dicom_series_records[dicom_series_id]

        for cda_dicom_series_field in cda_dicom_series_fields:
            
            output_row.append( dicom_series_record[cda_dicom_series_field] )

        print( *output_row, sep='\t', file=OUT )

# Write dicom_series->type.

with open( dicom_series_type_output_tsv, 'w' ) as OUT:
    
    print( *[ 'dicom_series_id', 'type', 'instance_count' ], sep='\t', file=OUT )

    for dicom_series_id in sorted( cda_dicom_series_type ):
        
        for type_value in sorted( cda_dicom_series_type[dicom_series_id] ):
            
            instance_count = cda_dicom_series_type[dicom_series_id][type_value]

            print( *[ dicom_series_id, type_value, instance_count ], sep='\t', file=OUT )

# Write the dicom_series<->project association.

with open( dicom_series_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'dicom_series_id', 'project_id' ], sep='\t', file=OUT )

    for dicom_series_id in sorted( cda_dicom_series_in_project ):
        
        for project_id in sorted( cda_dicom_series_in_project[dicom_series_id] ):
            
            print( *[ dicom_series_id, project_id ], sep='\t', file=OUT )

# Write dicom_series->anatomic_site.

with open( dicom_series_anatomic_site_output_tsv, 'w' ) as OUT:
    
    print( *[ 'dicom_series_id', 'anatomic_site' ], sep='\t', file=OUT )

    for dicom_series_id in sorted( dicom_series_anatomic_site ):
        
        for anatomic_site in sorted( dicom_series_anatomic_site[dicom_series_id] ):
            
            print( *[ dicom_series_id, anatomic_site ], sep='\t', file=OUT )

# Write dicom_series->tumor_vs_normal.

with open( dicom_series_tumor_vs_normal_output_tsv, 'w' ) as OUT:
    
    print( *[ 'dicom_series_id', 'tumor_vs_normal' ], sep='\t', file=OUT )

    for dicom_series_id in sorted( dicom_series_tumor_vs_normal ):
        
        for tumor_vs_normal in sorted( dicom_series_tumor_vs_normal[dicom_series_id] ):
            
            print( *[ dicom_series_id, tumor_vs_normal ], sep='\t', file=OUT )

print( f"done. [{get_current_timestamp()}]", file=sys.stderr )



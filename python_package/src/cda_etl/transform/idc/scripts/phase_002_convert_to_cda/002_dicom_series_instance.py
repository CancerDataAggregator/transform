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

instance_input_tsv = path.join( tsv_input_root, 'dicom_instance.tsv.gz' )

# Load the DICOM code map to decode SOPClassUID values.

dicom_code_map_dir = path.join( 'auxiliary_metadata', '__DICOM_code_maps' )

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

dicom_series_instance_output_tsv = path.join( tsv_output_root, 'dicom_series_instance.tsv.gz' )

# Table header sequences.

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

debug = False

# EXECUTION

if not path.exists( tsv_output_root ):
    
    makedirs( tsv_output_root )

print( f"[{get_current_timestamp()}] Transcoding dicom_all instance-level metadata directly to CDA dicom_series_instance records...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

warned_class_uids = set()

with gzip.open( instance_input_tsv, 'rt' ) as IN, gzip.open( dicom_series_instance_output_tsv, 'wt' ) as OUT:
    
    print( *cda_dicom_series_instance_fields, sep='\t', file=OUT )

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in IN:
        
        record = dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) )

        output_row = list()

        output_row.append( record['SOPInstanceUID'] )

        output_row.append( '' )

        output_row.append( record['crdc_series_uuid'] )

        output_row.append( record['gcs_url'] )

        output_row.append( f"drs://dg.4dfc:{record['crdc_instance_uuid']}" )

        output_row.append( record['instance_size'] )

        output_row.append( record['instance_hash'] )

        type_val = ''

        if record['SOPClassUID'] in class_code_map:
            
            type_val = class_code_map[record['SOPClassUID']]

        elif record['SOPClassUID'] in retired_class_code_map:
            
            type_val = retired_class_code_map[record['SOPClassUID']]

        elif record['SOPClassUID'] not in warned_class_uids:
            
            warned_class_uids.add( record['SOPClassUID'] )

            print( f"WARNING: Encountered unknown SOPClassUID '{record['SOPClassUID']}'; nulling for CDA and suppressing future warnings for the same value.", file=sys.stderr )

        output_row.append( type_val )

        print( *output_row, sep='\t', file=OUT )

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...processed {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )



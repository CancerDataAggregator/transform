#!/usr/bin/env python3 -u

import gzip
import jsonlines
import sys

from os import path, makedirs

from cda_etl.lib import get_current_timestamp

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

jsonl_input_dir = path.join( extraction_root, '__raw_BigQuery_JSONL' )

table_name = 'dicom_all'

input_file = path.join( jsonl_input_dir, f"{table_name}.jsonl.gz" )

output_dir = extraction_root

output_file = path.join( output_dir, 'dicom_series.tsv' )

log_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'values' )

series_metadata_fields = [
    
    'crdc_series_uuid',
    'SeriesDescription',
    'Modality',
    'PatientID',
    'idc_case_id',
    'PatientSpeciesDescription',
    'PatientBirthDate',
    'PatientSex',
    'EthnicGroup',
    'collection_id',
    'collection_tumorLocation',
    'tcia_tumorLocation'
]

clash_log_file = path.join( log_dir, f"IDC_same_DICOM_series_clashes.all_fields.tsv" )

# EXECUTION

for target_dir in [ output_dir, log_dir ]:
    
    if not path.isdir( output_dir ):
        
        makedirs( output_dir )

# Load DICOM series metadata from dicom_all and document inter-record clashes observed for values that seem like they should be invariant.

series = dict()

instance_count = dict()

total_byte_size = dict()

data_clashes = dict()

print( f"[{get_current_timestamp()}] Scanning {table_name} for series metadata...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

with gzip.open( input_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        if 'crdc_series_uuid' not in record or record['crdc_series_uuid'] is None or record['crdc_series_uuid'] == '':
            
            sys.exit( f"FATAL: SOPInstanceUID {record['SOPInstanceUID']} has no crdc_series_uuid; assumptions violated, cannot continue." )

        series_id = record['crdc_series_uuid']

        if series_id not in series:
            
            series[series_id] = dict()

            instance_count[series_id] = 1

            total_byte_size[series_id] = int( record['instance_size'] )

            for field_name in series_metadata_fields:
                
                if field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    series[series_id][field_name] = record[field_name]

                else:
                    
                    series[series_id][field_name] = ''

        else:
            
            instance_count[series_id] = instance_count[series_id] + 1

            total_byte_size[series_id] = total_byte_size[series_id] + int( record['instance_size'] )

            for field_name in series_metadata_fields:
                
                if field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    # This should break with a key error if the access fails: we initialized these dictionaries with complete field lists.

                    old_value = series[series_id][field_name]

                    new_value = record[field_name]

                    # If there's no conflict, do nothing. Otherwise:

                    if old_value != new_value:
                        
                        # Will we be updating the previous value? Let's find out.

                        updated = False

                        if old_value == '':
                            
                            # Replace nulls with non-null values.

                            series[series_id][field_name] = record[field_name]

                            updated = True

                        else:
                            
                            # Don't overwrite non-null values.

                            updated = False

                        # Log the observed clash.

                        if series_id not in data_clashes:
                            
                            data_clashes[series_id] = dict()

                        if field_name not in data_clashes[series_id]:
                            
                            data_clashes[series_id][field_name] = dict()

                        if old_value not in data_clashes[series_id][field_name]:
                            
                            data_clashes[series_id][field_name][old_value] = dict()

                        if updated:
                            
                            data_clashes[series_id][field_name][old_value][new_value] = new_value

                        else:
                            
                            data_clashes[series_id][field_name][old_value][new_value] = old_value

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] Writing output files and logging observed data clashes...", end='', file=sys.stderr )

# Write all DICOM series metadata to TSV.

with open( output_file, 'w' ) as OUT:
    
    output_fields = list()

    for field_name in series_metadata_fields:
        
        output_fields.append( field_name )

    for field_name in [ 'instance_count', 'total_byte_size' ]:
        
        output_fields.append( field_name )

    print( *output_fields, sep='\t', file=OUT )

    for series_id in sorted( series ):
        
        output_row = list()

        for field_name in series_metadata_fields:
            
            output_row.append( series[series_id][field_name] )

        output_row.append( instance_count[series_id] )

        output_row.append( total_byte_size[series_id] )

        print( *output_row, sep='\t', file=OUT )

# Log all observed clashes and what was done about them.

with open( clash_log_file, 'w' ) as OUT:
    
    print( *[ 'IDC_crdc_series_uuid', 'IDC_dicom_all_field_name', 'observed_value_from_IDC', 'clashing_value_at_IDC', 'CDA_kept_value' ], sep='\t', file=OUT )

    for series_id in sorted( data_clashes ):
        
        for field_name in series_metadata_fields:
            
            if field_name in data_clashes[series_id]:
                
                for old_value in sorted( data_clashes[series_id][field_name] ):
                    
                    for new_value in sorted( data_clashes[series_id][field_name][old_value] ):
                        
                        kept_value = data_clashes[series_id][field_name][old_value][new_value]

                        print( *[ series_id, field_name, old_value, new_value, kept_value ], sep='\t', file=OUT )

print( f"done.", end='\n\n', file=sys.stderr )



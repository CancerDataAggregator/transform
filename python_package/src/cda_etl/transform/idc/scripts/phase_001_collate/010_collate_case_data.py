#!/usr/bin/env python3 -u

import gzip
import jsonlines
import sys

from os import path, makedirs

from cda_etl.lib import get_current_timestamp

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

id_map_tsv = path.join( extraction_root, 'auxiliary_metadata.tsv' )

series_input_tsv = path.join( extraction_root, 'dicom_series.tsv' )

output_dir = extraction_root

output_file = path.join( output_dir, 'idc_case.tsv' )

patient_id_file = path.join( output_dir, 'idc_case.PatientID.tsv' )

log_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'values' )

case_metadata_fields = [
    
    'idc_case_id',
    'submitter_case_id',
    'PatientSpeciesDescription',
    'PatientBirthDate',
    'PatientSex',
    'EthnicGroup',
    'collection_id'
]

clash_log_file = path.join( log_dir, f"IDC_same_idc_case_id_clashes.all_fields.tsv" )

# EXECUTION

for target_dir in [ output_dir, log_dir ]:
    
    if not path.isdir( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Loading submitter_case_id for each idc_case_id...", end='', file=sys.stderr )

submitter_id = dict()

with open( id_map_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        submitter_id[record['idc_case_id']] = record['submitter_case_id']

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Scanning series metadata for IDC case metadata...", end='', file=sys.stderr )

# Load IDC case metadata from dicom_series.tsv and document inter-record clashes observed for values that seem like they should be invariant.

case = dict()

patient_id = dict()

line_count = 0

data_clashes = dict()

with open( series_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if 'idc_case_id' not in record or record['idc_case_id'] is None or record['idc_case_id'] == '':
            
            sys.exit( f"FATAL: crdc_series_uuid {record['crdc_series_uuid']} has no idc_case_id; assumptions violated, cannot continue." )

        case_id = record['idc_case_id']

        if case_id not in patient_id:
            
            patient_id[case_id] = set()

        # Never null at time of writing (August 2024). Should break loudly if that ever changes: these are already noisy, but null is another level entirely.

        patient_id[case_id].add( record['PatientID'] )

        if case_id not in case:
            
            case[case_id] = dict()

            for field_name in case_metadata_fields:
                
                if field_name == 'submitter_case_id':
                    
                    # Patch these in from auxiliary_metadata.tsv. The `PatientID` field in dicom_all, which should correspond
                    # precisely to auxiliary_metadata.submitter_case_id for identical idc_case_id values, is prone to a few
                    # mismatch bugs (see clash logs). auxiliary_metadata.tsv is not, at time of writing (August 2024).

                    case[case_id][field_name] = submitter_id[case_id]

                elif field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    case[case_id][field_name] = record[field_name]

                else:
                    
                    case[case_id][field_name] = ''

        else:
            
            for field_name in case_metadata_fields:
                
                if field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    # This should break with a key error if the access fails: we initialized these dictionaries with complete field lists.

                    old_value = case[case_id][field_name]

                    new_value = record[field_name]

                    # If there's no conflict, do nothing. Otherwise:

                    if old_value != new_value:
                        
                        # Will we be updating the previous value? Let's find out.

                        updated = False

                        if old_value == '':
                            
                            # Replace nulls with non-null values.

                            case[case_id][field_name] = record[field_name]

                            updated = True

                        else:
                            
                            # Don't overwrite non-null values.

                            updated = False

                        # Log the observed clash.

                        if case_id not in data_clashes:
                            
                            data_clashes[case_id] = dict()

                        if field_name not in data_clashes[case_id]:
                            
                            data_clashes[case_id][field_name] = dict()

                        if old_value not in data_clashes[case_id][field_name]:
                            
                            data_clashes[case_id][field_name][old_value] = dict()

                        if updated:
                            
                            data_clashes[case_id][field_name][old_value][new_value] = new_value

                        else:
                            
                            data_clashes[case_id][field_name][old_value][new_value] = old_value

        line_count = line_count + 1

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output files and logging observed data clashes...", end='', file=sys.stderr )

# Write all IDC case metadata to TSV.

with open( output_file, 'w' ) as OUT:
    
    print( *case_metadata_fields, sep='\t', file=OUT )

    for case_id in sorted( case ):
        
        print( *[ case[case_id][field_name] for field_name in case_metadata_fields ], sep='\t', file=OUT )

# Write a separate file for idc_case_id->PatientID relationships. These are, albeit probably due to a curation bug, not one-to-one at time of writing. We don't rely on PatientID correctness, but we do document ID assigments.

with open( patient_id_file, 'w' ) as OUT:
    
    print( *[ 'idc_case_id', 'PatientID' ], sep='\t', file=OUT )

    for case_id in sorted( patient_id ):
        
        for p_id in sorted( patient_id[case_id] ):
            
            print( *[ case_id, p_id ], sep='\t', file=OUT )

# Log all observed clashes and what was done about them.

with open( clash_log_file, 'w' ) as OUT:
    
    print( *[ 'IDC_idc_case_id', 'IDC_dicom_all_field_name', 'observed_value_from_IDC', 'clashing_value_at_IDC', 'CDA_kept_value' ], sep='\t', file=OUT )

    for case_id in sorted( data_clashes ):
        
        for field_name in case_metadata_fields:
            
            if field_name in data_clashes[case_id]:
                
                for old_value in sorted( data_clashes[case_id][field_name] ):
                    
                    for new_value in sorted( data_clashes[case_id][field_name][old_value] ):
                        
                        kept_value = data_clashes[case_id][field_name][old_value][new_value]

                        print( *[ case_id, field_name, old_value, new_value, kept_value ], sep='\t', file=OUT )

print( f"done.", end='\n\n', file=sys.stderr )



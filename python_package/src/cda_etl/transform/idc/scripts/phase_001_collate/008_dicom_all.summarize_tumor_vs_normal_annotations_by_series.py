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

output_file = path.join( output_dir, 'dicom_series.dicom_all.tumor_vs_normal_values_and_instance_counts.tsv' )

series_specimen_output_file = path.join( output_dir, 'dicom_series.dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.tsv' )

# EXECUTION

instance_count = dict()

series_id_to_specimen_submitter_id = dict()

series_tumor_vs_normal_instance_counts_by_value = {
    
    'SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning': dict()
}

print( f"[{get_current_timestamp()}] Scanning {table_name} for tumor/normal metadata and aggregating by series...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

with gzip.open( input_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        if 'crdc_series_uuid' not in record or record['crdc_series_uuid'] is None or record['crdc_series_uuid'] == '':
            
            sys.exit( f"FATAL: SOPInstanceUID {record['SOPInstanceUID']} has no crdc_series_uuid; assumptions violated, cannot continue." )

        series_id = record['crdc_series_uuid']

        if series_id not in instance_count:
            
            instance_count[series_id] = 1

        else:
            
            instance_count[series_id] = instance_count[series_id] + 1

        # Length of this array is always either 0 or 1 at time of writing (September 2024). We cover the fully general array structure anyway. Why not?

        if 'SpecimenDescriptionSequence' in record and record['SpecimenDescriptionSequence'] is not None and len( record['SpecimenDescriptionSequence'] ) > 0:
            
            for desc_dict in record['SpecimenDescriptionSequence']:
                
                # Cache specimen submitter IDs.

                if 'SpecimenIdentifier' in desc_dict and desc_dict['SpecimenIdentifier'] is not None and desc_dict['SpecimenIdentifier'] != '':
                    
                    specimen_submitter_id = desc_dict['SpecimenIdentifier']

                    if series_id not in series_id_to_specimen_submitter_id:
                        
                        series_id_to_specimen_submitter_id[series_id] = set()

                    series_id_to_specimen_submitter_id[series_id].add( specimen_submitter_id )

                if 'PrimaryAnatomicStructureSequence' in desc_dict and desc_dict['PrimaryAnatomicStructureSequence'] is not None and len( desc_dict['PrimaryAnatomicStructureSequence'] ) > 0:
                    
                    for structure_dict in desc_dict['PrimaryAnatomicStructureSequence']:
                        
                        if 'PrimaryAnatomicStructureModifierSequence' in structure_dict and structure_dict['PrimaryAnatomicStructureModifierSequence'] is not None and len( structure_dict['PrimaryAnatomicStructureModifierSequence'] ) > 0:
                            
                            for mod_dict in structure_dict['PrimaryAnatomicStructureModifierSequence']:
                                
                                if 'CodeMeaning' in mod_dict and mod_dict['CodeMeaning'] is not None and mod_dict['CodeMeaning'] != '':
                                    
                                    sds_value = mod_dict['CodeMeaning']

                                    if series_id not in series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning']:
                                        
                                        series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning'][series_id] = dict()

                                    if sds_value not in series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning'][series_id]:
                                        
                                        series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning'][series_id][sds_value] = 1

                                    else:
                                        
                                        series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning'][series_id][sds_value] = series_tumor_vs_normal_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning'][series_id][sds_value] + 1

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] Writing output files...", end='', file=sys.stderr )

# Summarize dicom_all series-level tumor/normal annotation values from and instance counts to TSV.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'total_instances', 'instances_matching_value', 'value', 'source' ], sep='\t', file=OUT )

    for series_id in sorted( instance_count ):
        
        for value_type in sorted( series_tumor_vs_normal_instance_counts_by_value ):
            
            if series_id in series_tumor_vs_normal_instance_counts_by_value[value_type]:
                
                for value in sorted( series_tumor_vs_normal_instance_counts_by_value[value_type][series_id] ):
                    
                    value_count = series_tumor_vs_normal_instance_counts_by_value[value_type][series_id][value]

                    print( *[ series_id, instance_count[series_id], value_count, value, value_type ], sep='\t', file=OUT )

# Save cross-references (as asserted in dicom_all) to TCGA specimen submitter IDs.

with open( series_specimen_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier' ], sep='\t', file=OUT )

    for series_id in sorted( series_id_to_specimen_submitter_id ):
        
        for specimen_submitter_id in sorted( series_id_to_specimen_submitter_id[series_id] ):
            
            print( *[ series_id, specimen_submitter_id ], sep='\t', file=OUT )

print( f"done.", end='\n\n', file=sys.stderr )



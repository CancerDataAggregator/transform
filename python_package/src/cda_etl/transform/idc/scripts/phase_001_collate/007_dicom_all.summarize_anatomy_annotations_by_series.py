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

output_file = path.join( output_dir, 'dicom_series.dicom_all.anatomy_values_and_instance_counts.tsv' )

# EXECUTION

instance_count = dict()

series_anatomy_instance_counts_by_value = {
    
    'AnatomicRegionSequence.CodeMeaning': dict(),
    'SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning': dict(),
    'RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning': dict(),
    'SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning': dict()
}

print( f"[{get_current_timestamp()}] Scanning {table_name} for anatomy metadata and aggregating by series...", end='\n\n', file=sys.stderr )

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

        if 'AnatomicRegionSequence' in record and record['AnatomicRegionSequence'] is not None and len( record['AnatomicRegionSequence'] ) > 0:
            
            for region_dict in record['AnatomicRegionSequence']:
                
                if 'CodeMeaning' in region_dict and region_dict['CodeMeaning'] is not None and region_dict['CodeMeaning'] != '':
                    
                    ars_value = region_dict['CodeMeaning']

                    if series_id not in series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning']:
                        
                        series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning'][series_id] = dict()

                    if ars_value not in series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning'][series_id]:
                        
                        series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning'][series_id][ars_value] = 1

                    else:
                        
                        series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning'][series_id][ars_value] = series_anatomy_instance_counts_by_value['AnatomicRegionSequence.CodeMeaning'][series_id][ars_value] + 1

        # Length of this array is always either 0 or 1 at time of writing (September 2024). We cover the fully general array structure anyway. Why not?

        if 'SpecimenDescriptionSequence' in record and record['SpecimenDescriptionSequence'] is not None and len( record['SpecimenDescriptionSequence'] ) > 0:
            
            for desc_dict in record['SpecimenDescriptionSequence']:
                
                if 'PrimaryAnatomicStructureSequence' in desc_dict and desc_dict['PrimaryAnatomicStructureSequence'] is not None and len( desc_dict['PrimaryAnatomicStructureSequence'] ) > 0:
                    
                    for structure_dict in desc_dict['PrimaryAnatomicStructureSequence']:
                        
                        if 'CodeMeaning' in structure_dict and structure_dict['CodeMeaning'] is not None and structure_dict['CodeMeaning'] != '':
                            
                            sds_value = structure_dict['CodeMeaning']

                            if series_id not in series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning']:
                                
                                series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning'][series_id] = dict()

                            if sds_value not in series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning'][series_id]:
                                
                                series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning'][series_id][sds_value] = 1

                            else:
                                
                                series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning'][series_id][sds_value] = series_anatomy_instance_counts_by_value['SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning'][series_id][sds_value] + 1

        # At time of writing (September 2024), this array is nonzero for 4,627 (of about 45M) instance records; its length distribution looks like { 1: 1906, 2: 467, 3: 295, 4: 619, 5: 634, >5: 706, >10: 413, >20: 362, >50: 0 }.

        if 'RTROIObservationsSequence' in record and record['RTROIObservationsSequence'] is not None and len( record['RTROIObservationsSequence'] ) > 0:
            
            for obs_dict in record['RTROIObservationsSequence']:
                
                if 'AnatomicRegionSequence' in obs_dict and obs_dict['AnatomicRegionSequence'] is not None and len( obs_dict['AnatomicRegionSequence'] ) > 0:
                    
                    for region_dict in obs_dict['AnatomicRegionSequence']:
                        
                        if 'CodeMeaning' in region_dict and region_dict['CodeMeaning'] is not None and region_dict['CodeMeaning'] != '':
                            
                            rtroi_value = region_dict['CodeMeaning']

                            if series_id not in series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning']:
                                
                                series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning'][series_id] = dict()

                            if rtroi_value not in series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning'][series_id]:
                                
                                series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning'][series_id][rtroi_value] = 1

                            else:
                                
                                series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning'][series_id][rtroi_value] = series_anatomy_instance_counts_by_value['RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning'][series_id][rtroi_value] + 1

        if 'SharedFunctionalGroupsSequence' in record and record['SharedFunctionalGroupsSequence'] is not None and len( record['SharedFunctionalGroupsSequence'] ) > 0:
            
            for group_dict in record['SharedFunctionalGroupsSequence']:
                
                if 'FrameAnatomySequence' in group_dict and group_dict['FrameAnatomySequence'] is not None and len( group_dict['FrameAnatomySequence'] ) > 0:
                    
                    for frame_dict in group_dict['FrameAnatomySequence']:
                        
                        if 'AnatomicRegionSequence' in frame_dict and frame_dict['AnatomicRegionSequence'] is not None and len( frame_dict['AnatomicRegionSequence'] ) > 0:
                            
                            for region_dict in frame_dict['AnatomicRegionSequence']:
                                
                                if 'CodeMeaning' in region_dict and region_dict['CodeMeaning'] is not None and region_dict['CodeMeaning'] != '':
                                    
                                    sfg_value = region_dict['CodeMeaning']

                                    if series_id not in series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning']:
                                        
                                        series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning'][series_id] = dict()

                                    if sfg_value not in series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning'][series_id]:
                                        
                                        series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning'][series_id][sfg_value] = 1

                                    else:
                                        
                                        series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning'][series_id][sfg_value] = series_anatomy_instance_counts_by_value['SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning'][series_id][sfg_value] + 1

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] Writing output file...", end='', file=sys.stderr )

# Summarize series-level anatomy annotation values and instance counts to TSV.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'total_instances', 'instances_matching_value', 'value', 'source' ], sep='\t', file=OUT )

    for series_id in sorted( instance_count ):
        
        for value_type in sorted( series_anatomy_instance_counts_by_value ):
            
            if series_id in series_anatomy_instance_counts_by_value[value_type]:
                
                for value in sorted( series_anatomy_instance_counts_by_value[value_type][series_id] ):
                    
                    value_count = series_anatomy_instance_counts_by_value[value_type][series_id][value]

                    print( *[ series_id, instance_count[series_id], value_count, value, value_type ], sep='\t', file=OUT )

print( f"done.", end='\n\n', file=sys.stderr )



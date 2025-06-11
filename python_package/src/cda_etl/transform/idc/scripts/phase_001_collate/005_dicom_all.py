#!/usr/bin/env python3 -u

import gzip
import json
import jsonlines
import sys

from os import path, makedirs

from cda_etl.lib import get_current_timestamp, get_idc_extraction_fields

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

jsonl_input_dir = path.join( extraction_root, '__raw_BigQuery_JSONL' )

table_name = 'dicom_all'

input_file = path.join( jsonl_input_dir, f"{table_name}.jsonl.gz" )

output_dir = extraction_root

series_output_file = path.join( output_dir, 'dicom_series.tsv' )

instance_output_file = path.join( output_dir, f"dicom_instance.tsv.gz" )

anatomy_output_file = path.join( output_dir, 'dicom_series.dicom_all.anatomy_values_and_instance_counts.tsv' )

tumor_vs_normal_output_file = path.join( output_dir, 'dicom_series.dicom_all.tumor_vs_normal_values_and_instance_counts.tsv' )

series_specimen_output_file = path.join( output_dir, 'dicom_series.dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.tsv' )

class_output_file = path.join( extraction_root, 'dicom_series.dicom_all.SOPClassUID.tsv' )

size_output_file = path.join( extraction_root, 'dicom_series.dicom_all.instance_size.tsv' )

# DRS URI for current_instance: f"drs://dg.4dfc:{current_instance['crdc_instance_uuid']}"

crdc_instance_uuid_output_file = path.join( extraction_root, 'dicom_series.dicom_all.crdc_instance_uuid.tsv' )

log_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'values' )

series_clash_log_file = path.join( log_dir, f"IDC_same_DICOM_series_clashes.all_fields.tsv" )

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
    'StudyDate',
    'BodyPartExamined',
    'collection_id',
    'collection_tumorLocation'
]

instance_metadata_fields = [
    
    'SOPInstanceUID',
    'crdc_series_uuid',
    'gcs_url',
    'crdc_instance_uuid',
    'instance_size',
    'instance_hash',
    'SOPClassUID'
]

sequence_fields = [
    
    'AnatomicRegionSequence',
    'SpecimenDescriptionSequence',
    'SegmentSequence',
    'RTROIObservationsSequence',
    'SharedFunctionalGroupsSequence'
]

# Make sure we use everything we extract.

toplevel_extraction_fields = get_idc_extraction_fields()[table_name]

for field_name in toplevel_extraction_fields:
    
    if field_name not in series_metadata_fields and field_name not in instance_metadata_fields and field_name not in sequence_fields:
        
        print( f"[{get_current_timestamp()}] WARNING: Scalar field '{field_name}' extracted from {table_name} but unused downstream.", file=sys.stderr )

# EXECUTION

for target_dir in [ output_dir, log_dir ]:
    
    if not path.isdir( target_dir ):
        
        makedirs( target_dir )

# Load DICOM series metadata from dicom_all and document inter-record clashes observed for values that seem like they should be invariant.

print( f"[{get_current_timestamp()}] Scanning {table_name} for series metadata...", file=sys.stderr )

series = dict()

instance_count = dict()

total_byte_size = dict()

data_clashes = dict()

# Load anatomical and basic pathology annotations from several source fields and aggregate them by series.

print( f"[{get_current_timestamp()}] Scanning {table_name} for anatomy and tumor_vs_normal metadata and aggregating by series...", file=sys.stderr )

series_anatomy_instance_counts_by_value = {
    
    'BodyPartExamined': dict(),
    'collection_tumorLocation': dict(),
    'AnatomicRegionSequence.CodeMeaning': dict(),
    'SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning': dict(),
    'RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning': dict(),
    'SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning': dict()
}

series_id_to_specimen_submitter_id = dict()

series_tumor_vs_normal_instance_counts_by_value = {
    
    'SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning': dict()
}

# Load SOPClassUID values and count each by series.

print( f"[{get_current_timestamp()}] Scanning {table_name} for SOPClassUID values and aggregating by series...", file=sys.stderr )

series_classes = dict()

series_sizes = dict()

series_crdc_instance_uuids = dict()

# Load DICOM instance metadata from dicom_all, process, and dump directly to our output TSV.

print( f"[{get_current_timestamp()}] Scanning {table_name} for instance metadata and transcoding directly to {instance_output_file}...", end='\n\n', file=sys.stderr )

line_count = 0

display_increment = 5000000

with gzip.open( input_file ) as IN, gzip.open( instance_output_file, 'wt' ) as OUT:
    
    print( *instance_metadata_fields, sep='\t', file=OUT )

    reader = jsonlines.Reader( IN )

    for record in reader:
        
        if 'SOPInstanceUID' not in record or record['SOPInstanceUID'] is None or record['SOPInstanceUID'] == '':
            
            sys.exit( f"FATAL: Record at line {line_count + 1} has no SOPInstanceUID; assumptions violated, cannot continue." )

        ########################################################################
        # Transcode instance metadata directly to the output TSV.
        ########################################################################

        output_row = list()

        for field_name in instance_metadata_fields:
            
            if field_name in record and record[field_name] is not None and record[field_name] != '':
                
                output_row.append( record[field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', file=OUT )

        if 'crdc_series_uuid' not in record or record['crdc_series_uuid'] is None or record['crdc_series_uuid'] == '':
            
            sys.exit( f"FATAL: SOPInstanceUID {record['SOPInstanceUID']} (line {line_count + 1} of {input_file}) has no crdc_series_uuid; assumptions violated, cannot continue." )

        series_id = record['crdc_series_uuid']

        ########################################################################
        # Aggregate series metadata and log clashes.
        ########################################################################

        if series_id not in series:
            
            series[series_id] = dict()

            instance_count[series_id] = 1

            total_byte_size[series_id] = int( record['instance_size'] )

            for field_name in series_metadata_fields:
                
                if field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    # There are all manner of format-breaking nonprintable characters embedded in SeriesDescription values (at least). Trust no data fields when it comes to text encoding.

                    series[series_id][field_name] = json.dumps( record[field_name] ).strip( '"' )

                else:
                    
                    series[series_id][field_name] = ''

        else:
            
            instance_count[series_id] = instance_count[series_id] + 1

            total_byte_size[series_id] = total_byte_size[series_id] + int( record['instance_size'] )

            for field_name in series_metadata_fields:
                
                if field_name in record and record[field_name] is not None and record[field_name] != '':
                    
                    # This should break with a key error if the access fails: we initialized these dictionaries with complete field lists.

                    old_value = series[series_id][field_name]

                    # There are all manner of format-breaking nonprintable characters embedded in SeriesDescription values (at least). Trust no data fields when it comes to text encoding.

                    new_value = json.dumps( record[field_name] ).strip( '"' )

                    # If there's no conflict, do nothing. Otherwise:

                    if old_value != new_value:
                        
                        # Will we be updating the previous value? Let's find out.

                        updated = False

                        if old_value == '':
                            
                            # Replace nulls with non-null values.

                            series[series_id][field_name] = new_value

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

        ########################################################################
        # Load anatomy annotations and aggregate by series.
        ########################################################################

        # dicom_all.BodyPartExamined

        bpe_value = series[series_id]['BodyPartExamined']

        if bpe_value != '':
            
            if series_id not in series_anatomy_instance_counts_by_value['BodyPartExamined']:
                
                series_anatomy_instance_counts_by_value['BodyPartExamined'][series_id] = dict()

            if bpe_value not in series_anatomy_instance_counts_by_value['BodyPartExamined'][series_id]:
                
                series_anatomy_instance_counts_by_value['BodyPartExamined'][series_id][bpe_value] = 1

            else:
                
                series_anatomy_instance_counts_by_value['BodyPartExamined'][series_id][bpe_value] = series_anatomy_instance_counts_by_value['BodyPartExamined'][series_id][bpe_value] + 1

        # dicom_all.collection_tumorLocation

        ctl_value = series[series_id]['collection_tumorLocation']

        if ctl_value != '':
            
            if series_id not in series_anatomy_instance_counts_by_value['collection_tumorLocation']:
                
                series_anatomy_instance_counts_by_value['collection_tumorLocation'][series_id] = dict()

            if ctl_value not in series_anatomy_instance_counts_by_value['collection_tumorLocation'][series_id]:
                
                series_anatomy_instance_counts_by_value['collection_tumorLocation'][series_id][ctl_value] = 1

            else:
                
                series_anatomy_instance_counts_by_value['collection_tumorLocation'][series_id][ctl_value] = series_anatomy_instance_counts_by_value['collection_tumorLocation'][series_id][ctl_value] + 1

        # dicom_all.AnatomicRegionSequence.CodeMeaning
        # 
        # The observed length of this array is always either 0 or 1 at time of writing (September 2024). We cover the fully general array structure anyway. Why not?

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

        # dicom_all.SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.CodeMeaning
        # 
        # The observed length of this array is always either 0 or 1 at time of writing (September 2024). We cover the fully general array structure anyway. Why not?

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

        # dicom_all.RTROIObservationsSequence.AnatomicRegionSequence.CodeMeaning
        # 
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

        # dicom_all.SharedFunctionalGroupsSequence.FrameAnatomySequence.AnatomicRegionSequence.CodeMeaning

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

        ########################################################################
        # Load tumor_vs_normal annotations and aggregate by series.
        ########################################################################

        # dicom_all.SpecimenDescriptionSequence.PrimaryAnatomicStructureSequence.PrimaryAnatomicStructureModifierSequence.CodeMeaning
        # 
        # The observed length of this array is always either 0 or 1 at time of writing (September 2024). We cover the fully general array structure anyway. Why not?

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

        ########################################################################
        # Count SOPClassUID values by series.
        ########################################################################

        class_uid = ''

        if 'SOPClassUID' in record and record['SOPClassUID'] is not None and record['SOPClassUID'] != '':
            
            class_uid = record['SOPClassUID']

        if series_id not in series_classes:
            
            series_classes[series_id] = dict()

        if class_uid not in series_classes[series_id]:
            
            series_classes[series_id][class_uid] = 1

        else:
            
            series_classes[series_id][class_uid] = series_classes[series_id][class_uid] + 1

        size_val = ''

        if 'instance_size' in record and record['instance_size'] is not None and record['instance_size'] != '':
            
            # Note both that this will result in size_val being a string (no int() wrapper) and that this is intended (we're not summing these values, just printing them).

            size_val = record['instance_size']

        if series_id not in series_sizes:
            
            series_sizes[series_id] = dict()

        if size_val not in series_sizes[series_id]:
            
            series_sizes[series_id][size_val] = 1

        else:
            
            series_sizes[series_id][size_val] = series_sizes[series_id][size_val] + 1

        crdc_instance_uuid = ''

        if 'crdc_instance_uuid' in record and record['crdc_instance_uuid'] is not None and record['crdc_instance_uuid'] != '':
            
            crdc_instance_uuid = record['crdc_instance_uuid']

        else:
            
            sys.exit( f"FATAL: SOPInstanceUID {record['SOPInstanceUID']} (line {line_count + 1} of {input_file}) has no crdc_instance_uuid; assumptions violated, cannot continue." )

        if series_id not in series_crdc_instance_uuids:
            
            series_crdc_instance_uuids[series_id] = set()

        series_crdc_instance_uuids[series_id].add( crdc_instance_uuid )

        line_count = line_count + 1

        if line_count % display_increment == 0:
            
            print( f"    [{get_current_timestamp()}] ...scanned {line_count} lines...", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

print( f"\n[{get_current_timestamp()}] Writing output files and logging observed data clashes...", end='\n\n', file=sys.stderr )

# Write all DICOM series metadata to TSV.

print( f"   [{get_current_timestamp()}] {series_output_file}...", file=sys.stderr )

with open( series_output_file, 'w' ) as OUT:
    
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

print( f"   [{get_current_timestamp()}] {series_clash_log_file}...", file=sys.stderr )

with open( series_clash_log_file, 'w' ) as OUT:
    
    print( *[ 'IDC_crdc_series_uuid', 'IDC_dicom_all_field_name', 'observed_value_from_IDC', 'clashing_value_at_IDC', 'CDA_kept_value' ], sep='\t', file=OUT )

    for series_id in sorted( data_clashes ):
        
        for field_name in series_metadata_fields:
            
            if field_name in data_clashes[series_id]:
                
                for old_value in sorted( data_clashes[series_id][field_name] ):
                    
                    for new_value in sorted( data_clashes[series_id][field_name][old_value] ):
                        
                        kept_value = data_clashes[series_id][field_name][old_value][new_value]

                        print( *[ series_id, field_name, old_value, new_value, kept_value ], sep='\t', file=OUT )

# Summarize series-level anatomy annotation values and instance counts to TSV.

print( f"   [{get_current_timestamp()}] {anatomy_output_file}...", file=sys.stderr )

with open( anatomy_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'total_instances', 'instances_matching_value', 'value', 'source' ], sep='\t', file=OUT )

    for series_id in sorted( instance_count ):
        
        for value_type in sorted( series_anatomy_instance_counts_by_value ):
            
            if series_id in series_anatomy_instance_counts_by_value[value_type]:
                
                for value in sorted( series_anatomy_instance_counts_by_value[value_type][series_id] ):
                    
                    value_count = series_anatomy_instance_counts_by_value[value_type][series_id][value]

                    print( *[ series_id, instance_count[series_id], value_count, value, value_type ], sep='\t', file=OUT )

# Summarize dicom_all series-level tumor/normal annotation values from and instance counts to TSV.

print( f"   [{get_current_timestamp()}] {tumor_vs_normal_output_file}...", file=sys.stderr )

with open( tumor_vs_normal_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'total_instances', 'instances_matching_value', 'value', 'source' ], sep='\t', file=OUT )

    for series_id in sorted( instance_count ):
        
        for value_type in sorted( series_tumor_vs_normal_instance_counts_by_value ):
            
            if series_id in series_tumor_vs_normal_instance_counts_by_value[value_type]:
                
                for value in sorted( series_tumor_vs_normal_instance_counts_by_value[value_type][series_id] ):
                    
                    value_count = series_tumor_vs_normal_instance_counts_by_value[value_type][series_id][value]

                    print( *[ series_id, instance_count[series_id], value_count, value, value_type ], sep='\t', file=OUT )

# Save cross-references (as asserted in dicom_all) to TCGA specimen submitter IDs.

print( f"   [{get_current_timestamp()}] {series_specimen_output_file}...", file=sys.stderr )

with open( series_specimen_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier' ], sep='\t', file=OUT )

    for series_id in sorted( series_id_to_specimen_submitter_id ):
        
        for specimen_submitter_id in sorted( series_id_to_specimen_submitter_id[series_id] ):
            
            print( *[ series_id, specimen_submitter_id ], sep='\t', file=OUT )

# Write SOPClassUID output TSV.

print( f"   [{get_current_timestamp()}] {class_output_file}...", file=sys.stderr )

with open( class_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'SOPClassUID', 'number_of_matching_SOPInstanceUIDs' ], sep='\t', file=OUT )

    for series_id in sorted( series_classes ):
        
        for class_uid in sorted( series_classes[series_id] ):
            
            print( *[ series_id, class_uid, series_classes[series_id][class_uid] ], sep='\t', file=OUT )

# Write instance_size output TSV.

print( f"   [{get_current_timestamp()}] {size_output_file}...", file=sys.stderr )

with open( size_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'instance_size', 'number_of_matching_SOPInstanceUIDs' ], sep='\t', file=OUT )

    for series_id in sorted( series_sizes ):
        
        for size_val in sorted( series_sizes[series_id] ):
            
            print( *[ series_id, size_val, series_sizes[series_id][size_val] ], sep='\t', file=OUT )

print( f"\n[{get_current_timestamp()}] ...done.", end='\n\n', file=sys.stderr )

# Write crdc_instance_uuid values for each series.

print( f"   [{get_current_timestamp()}] {crdc_instance_uuid_output_file}...", file=sys.stderr )

with open( crdc_instance_uuid_output_file, 'w' ) as OUT:
    
    print( *[ 'crdc_series_uuid', 'crdc_instance_uuid' ], sep='\t', file=OUT )

    for series_id in sorted( series_crdc_instance_uuids ):
        
        for instance_id in sorted( series_crdc_instance_uuids[series_id] ):
            
            print( *[ series_id, instance_id ], sep='\t', file=OUT )



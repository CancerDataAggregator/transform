#!/usr/bin/env python3 -u

from os import path, makedirs

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

# PARAMETERS

extraction_root = path.join( 'extracted_data' )

aux_root = path.join( 'auxiliary_metadata' )

# GDC source data.

gdc_root = path.join( extraction_root, 'gdc', 'all_TSV_output' )

gdc_aliquot_tsv = path.join( gdc_root, 'aliquot.tsv' )

gdc_aliquot_analyte_tsv = path.join( gdc_root, 'aliquot_of_analyte.tsv' )

gdc_analyte_tsv = path.join( gdc_root, 'analyte.tsv' )

gdc_analyte_portion_tsv = path.join( gdc_root, 'analyte_from_portion.tsv' )

gdc_portion_tsv = path.join( gdc_root, 'portion.tsv' )

gdc_portion_sample_tsv = path.join( gdc_root, 'portion_from_sample.tsv' )

gdc_sample_tsv = path.join( gdc_root, 'sample.tsv' )

gdc_slide_tsv = path.join( gdc_root, 'slide.tsv' )

gdc_slide_portion_tsv = path.join( gdc_root, 'slide_from_portion.tsv' )

gdc_entity_metadata_tsv = path.join( aux_root, '__GDC_supplemental_metadata', 'GDC_entities_by_program_and_project.tsv' )

# PDC source data.

pdc_root = path.join( extraction_root, 'pdc_postprocessed' )

pdc_aliquot_tsv = path.join( pdc_root, 'Aliquot', 'Aliquot.tsv' )

pdc_sample_aliquot_tsv = path.join( pdc_root, 'Sample', 'Sample.aliquot_id.tsv' )

pdc_sample_tsv = path.join( pdc_root, 'Sample', 'Sample.tsv' )

pdc_entity_metadata_tsv = path.join( aux_root, '__PDC_supplemental_metadata', 'PDC_entities_by_program_project_and_study.tsv' )

# ICDC source data.

icdc_root = path.join( extraction_root, 'icdc' )

icdc_sample_tsv = path.join( icdc_root, 'sample', 'sample.tsv' )

icdc_entity_metadata_tsv = path.join( aux_root, '__ICDC_supplemental_metadata', 'ICDC_entities_by_program_and_study.tsv' )

# IDC source data.

idc_root = path.join( extraction_root, 'idc' )

idc_series_specimen_xref_tsv = path.join( idc_root, 'dicom_series.dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.tsv' )

idc_tcga_biospecimen_tsv = path.join( idc_root, 'tcga_biospecimen_rel9.tsv' )

idc_aux_dir = path.join( aux_root, '__IDC_supplemental_metadata' )

idc_specimen_xref_result_tsv = path.join( idc_aux_dir, 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier.crossrefs_resolved.anatomic_and_tumor_vs_normal_values_by_source.tsv' )

# EXECUTION

for target_dir in [ idc_aux_dir ]:
    
    if not path.isdir( target_dir ):
        
        makedirs( target_dir )

gdc_aliquot_submitter_id_to_id = map_columns_one_to_many( gdc_aliquot_tsv, 'submitter_id', 'aliquot_id' )

gdc_aliquot_to_analyte = map_columns_one_to_many( gdc_aliquot_analyte_tsv, 'aliquot_id', 'analyte_id' )

gdc_analyte_submitter_id_to_id = map_columns_one_to_many( gdc_analyte_tsv, 'submitter_id', 'analyte_id' )

gdc_analyte_to_portion = map_columns_one_to_many( gdc_analyte_portion_tsv, 'analyte_id', 'portion_id' )

gdc_portion_submitter_id_to_id = map_columns_one_to_many( gdc_portion_tsv, 'submitter_id', 'portion_id' )

gdc_portion_to_sample = map_columns_one_to_many( gdc_portion_sample_tsv, 'portion_id', 'sample_id' )

gdc_slide_submitter_id_to_id = map_columns_one_to_many( gdc_slide_tsv, 'submitter_id', 'slide_id' )

gdc_slide_to_portion = map_columns_one_to_many( gdc_slide_portion_tsv, 'slide_id', 'portion_id' )

gdc_sample_submitter_id_to_id = map_columns_one_to_many( gdc_sample_tsv, 'submitter_id', 'sample_id' )

gdc_sample = load_tsv_as_dict( gdc_sample_tsv )

gdc_sample_to_project = map_columns_one_to_one( gdc_entity_metadata_tsv, 'entity_id', 'project.project_id', where_field='entity_type', where_value='sample' )

gdc_project_to_program = map_columns_one_to_one( gdc_entity_metadata_tsv, 'project.project_id', 'program.name' )

gdc_project_name = map_columns_one_to_one( gdc_entity_metadata_tsv, 'project.project_id', 'project.name' )

pdc_aliquot_submitter_id_to_id = map_columns_one_to_many( pdc_aliquot_tsv, 'aliquot_submitter_id', 'aliquot_id' )

pdc_aliquot_to_sample = map_columns_one_to_many( pdc_sample_aliquot_tsv, 'aliquot_id', 'sample_id' )

pdc_sample_submitter_id_to_id = map_columns_one_to_many( pdc_sample_tsv, 'sample_submitter_id', 'sample_id' )

pdc_sample = load_tsv_as_dict( pdc_sample_tsv )

pdc_sample_to_project = map_columns_one_to_one( pdc_entity_metadata_tsv, 'entity_id', 'project.project_submitter_id', where_field='entity_type', where_value='sample' )

pdc_project_to_program = map_columns_one_to_one( pdc_entity_metadata_tsv, 'project.project_submitter_id', 'program.name' )

pdc_project_name = map_columns_one_to_one( pdc_entity_metadata_tsv, 'project.project_submitter_id', 'project.name' )

icdc_sample = load_tsv_as_dict( icdc_sample_tsv )

icdc_sample_to_study = map_columns_one_to_one( icdc_entity_metadata_tsv, 'entity_id', 'study.clinical_study_designation', where_field='entity_type', where_value='sample' )

icdc_study_name = map_columns_one_to_one( icdc_entity_metadata_tsv, 'study.clinical_study_designation', 'study.clinical_study_name' )

icdc_study_to_program = map_columns_one_to_one( icdc_entity_metadata_tsv, 'study.clinical_study_designation', 'program.program_name' )

idc_tcga_biospecimen = load_tsv_as_dict( idc_tcga_biospecimen_tsv )

idc_series_to_sample_submitter_id = map_columns_one_to_many( idc_series_specimen_xref_tsv, 'crdc_series_uuid', 'dicom_all.SpecimenDescriptionSequence.SpecimenIdentifier' )

target_sample_submitter_ids = set()

for series_id in idc_series_to_sample_submitter_id:
    
    for submitter_id in idc_series_to_sample_submitter_id[series_id]:
        
        target_sample_submitter_ids.add( submitter_id )

# Note there is no anatomy metadata in the IDC TCGA biospecimen table.

found_anatomy = {
    
    'gdc': dict(),
    'pdc': dict(),
    'icdc': dict()
}

found_tumor_vs_normal = {
    
    'gdc': dict(),
    'pdc': dict(),
    'icdc': dict(),
    'idc_tcga': dict()
}

for target_submitter_id in target_sample_submitter_ids:
    
    # GDC.

    matched_sample_ids = set()

    if target_submitter_id in gdc_aliquot_submitter_id_to_id:
        
        for aliquot_id in gdc_aliquot_submitter_id_to_id[target_submitter_id]:
            
            for analyte_id in gdc_aliquot_to_analyte[aliquot_id]:
                
                for portion_id in gdc_analyte_to_portion[analyte_id]:
                    
                    for sample_id in gdc_portion_to_sample[portion_id]:
                        
                        matched_sample_ids.add( sample_id )

    elif target_submitter_id in gdc_analyte_submitter_id_to_id:
        
        for analyte_id in gdc_analyte_submitter_id_to_id[target_submitter_id]:
            
            for portion_id in gdc_analyte_to_portion[analyte_id]:
                
                for sample_id in gdc_portion_to_sample[portion_id]:
                    
                    matched_sample_ids.add( sample_id )

    elif target_submitter_id in gdc_portion_submitter_id_to_id:
        
        for portion_id in gdc_portion_submitter_id_to_id[target_submitter_id]:
            
            for sample_id in gdc_portion_to_sample[portion_id]:
                
                matched_sample_ids.add( sample_id )

    elif target_submitter_id in gdc_slide_submitter_id_to_id:
        
        for slide_id in gdc_slide_submitter_id_to_id[target_submitter_id]:
            
            for portion_id in gdc_slide_to_portion[slide_id]:
                
                for sample_id in gdc_portion_to_sample[portion_id]:
                    
                    matched_sample_ids.add( sample_id )

    elif target_submitter_id in gdc_sample_submitter_id_to_id:
        
        for sample_id in gdc_sample_submitter_id_to_id[target_submitter_id]:
            
            matched_sample_ids.add( sample_id )

    if len( matched_sample_ids ) > 0:
        
        for sample_id in matched_sample_ids:
            
            if 'biospecimen_anatomic_site' in gdc_sample[sample_id] and gdc_sample[sample_id]['biospecimen_anatomic_site'] is not None and gdc_sample[sample_id]['biospecimen_anatomic_site'] != '':
                
                if target_submitter_id not in found_anatomy['gdc']:
                    
                    found_anatomy['gdc'][target_submitter_id] = set()

                found_anatomy['gdc'][target_submitter_id].add( sample_id )

            if 'tissue_type' in gdc_sample[sample_id] and gdc_sample[sample_id]['tissue_type'] is not None and gdc_sample[sample_id]['tissue_type'] != '':
                
                if target_submitter_id not in found_tumor_vs_normal['gdc']:
                    
                    found_tumor_vs_normal['gdc'][target_submitter_id] = set()

                found_tumor_vs_normal['gdc'][target_submitter_id].add( sample_id )

    # PDC.

    matched_sample_ids = set()

    if target_submitter_id in pdc_aliquot_submitter_id_to_id:
        
        for aliquot_id in pdc_aliquot_submitter_id_to_id[target_submitter_id]:
            
            for sample_id in pdc_aliquot_to_sample[aliquot_id]:
                
                matched_sample_ids.add( sample_id )

    elif target_submitter_id in pdc_sample_submitter_id_to_id:
        
        for sample_id in pdc_sample_submitter_id_to_id[target_submitter_id]:
            
            matched_sample_ids.add( sample_id )

    if len( matched_sample_ids ) > 0:
        
        for sample_id in matched_sample_ids:
            
            if 'biospecimen_anatomic_site' in pdc_sample[sample_id] and pdc_sample[sample_id]['biospecimen_anatomic_site'] is not None and pdc_sample[sample_id]['biospecimen_anatomic_site'] != '':
                
                if target_submitter_id not in found_anatomy['pdc']:
                    
                    found_anatomy['pdc'][target_submitter_id] = set()

                found_anatomy['pdc'][target_submitter_id].add( sample_id )

            if 'tissue_type' in pdc_sample[sample_id] and pdc_sample[sample_id]['tissue_type'] is not None and pdc_sample[sample_id]['tissue_type'] != '':
                
                if target_submitter_id not in found_tumor_vs_normal['pdc']:
                    
                    found_tumor_vs_normal['pdc'][target_submitter_id] = set()

                found_tumor_vs_normal['pdc'][target_submitter_id].add( sample_id )

    # ICDC.

    if target_submitter_id in icdc_sample:
        
        if 'sample_site' in icdc_sample[target_submitter_id] and icdc_sample[target_submitter_id]['sample_site'] is not None and icdc_sample[target_submitter_id]['sample_site'] != '':
            
            if target_submitter_id not in found_anatomy['icdc']:
                
                found_anatomy['icdc'][target_submitter_id] = set()

            found_anatomy['icdc'][target_submitter_id].add( target_submitter_id )

        if 'general_sample_pathology' in icdc_sample[target_submitter_id] and icdc_sample[target_submitter_id]['general_sample_pathology'] is not None and icdc_sample[target_submitter_id]['general_sample_pathology'] != '':
            
            if target_submitter_id not in found_tumor_vs_normal['icdc']:
                
                found_tumor_vs_normal['icdc'][target_submitter_id] = set()

            found_tumor_vs_normal['icdc'][target_submitter_id].add( target_submitter_id )

    # IDC / TCGA.

    if target_submitter_id in idc_tcga_biospecimen:
        
        if 'sample_type_name' in idc_tcga_biospecimen[target_submitter_id] and idc_tcga_biospecimen[target_submitter_id]['sample_type_name'] is not None and idc_tcga_biospecimen[target_submitter_id]['sample_type_name'] != '':
            
            if target_submitter_id not in found_tumor_vs_normal['idc_tcga']:
                
                found_tumor_vs_normal['idc_tcga'][target_submitter_id] = set()

            found_tumor_vs_normal['idc_tcga'][target_submitter_id].add( target_submitter_id )

# Write output TSV.

with open( idc_specimen_xref_result_tsv, 'w' ) as OUT:
    
    print( *[ 'SpecimenDescriptionSequence.SpecimenIdentifier', 'remote_data_source', 'remote_program_name', 'remote_subprogram_type', 'remote_subprogram_name', 'remote_sample_id', 'remote_sample_submitter_id', 'annotation_type', 'remote_sample_field_name', 'remote_sample_field_value' ], sep='\t', file=OUT )

    for target_submitter_id in sorted( target_sample_submitter_ids ):
        
        if target_submitter_id in found_anatomy['gdc']:
            
            for sample_id in sorted( found_anatomy['gdc'][target_submitter_id] ):
                
                project_id = gdc_sample_to_project[sample_id]

                project_name = gdc_project_name[project_id]

                program_name = gdc_project_to_program[project_id]

                sample_submitter_id = gdc_sample[sample_id]['submitter_id']

                remote_field_name = 'biospecimen_anatomic_site'

                remote_value = gdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'GDC', program_name, 'project', project_name, sample_id, sample_submitter_id, 'anatomy', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_tumor_vs_normal['gdc']:
            
            for sample_id in sorted( found_tumor_vs_normal['gdc'][target_submitter_id] ):
                
                project_id = gdc_sample_to_project[sample_id]

                project_name = gdc_project_name[project_id]

                program_name = gdc_project_to_program[project_id]

                sample_submitter_id = gdc_sample[sample_id]['submitter_id']

                remote_field_name = 'tissue_type'

                remote_value = gdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'GDC', program_name, 'project', project_name, sample_id, sample_submitter_id, 'tumor_vs_normal', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_anatomy['pdc']:
            
            for sample_id in sorted( found_anatomy['pdc'][target_submitter_id] ):
                
                project_id = pdc_sample_to_project[sample_id]

                project_name = pdc_project_name[project_id]

                program_name = pdc_project_to_program[project_id]

                sample_submitter_id = pdc_sample[sample_id]['sample_submitter_id']

                remote_field_name = 'biospecimen_anatomic_site'

                remote_value = pdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'PDC', program_name, 'project', project_name, sample_id, sample_submitter_id, 'anatomy', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_tumor_vs_normal['pdc']:
            
            for sample_id in sorted( found_tumor_vs_normal['pdc'][target_submitter_id] ):
                
                project_id = pdc_sample_to_project[sample_id]

                project_name = pdc_project_name[project_id]

                program_name = pdc_project_to_program[project_id]

                sample_submitter_id = pdc_sample[sample_id]['sample_submitter_id']

                remote_field_name = 'tissue_type'

                remote_value = pdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'PDC', program_name, 'project', project_name, sample_id, sample_submitter_id, 'tumor_vs_normal', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_anatomy['icdc']:
            
            for sample_id in sorted( found_anatomy['icdc'][target_submitter_id] ):
                
                study_id = icdc_sample_to_study[sample_id]

                study_name = icdc_study_name[study_id]

                program_name = icdc_study_to_program[study_id]

                sample_submitter_id = sample_id

                remote_field_name = 'sample_site'

                remote_value = icdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'ICDC', program_name, 'study', study_name, sample_id, sample_submitter_id, 'anatomy', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_tumor_vs_normal['icdc']:
            
            for sample_id in sorted( found_tumor_vs_normal['icdc'][target_submitter_id] ):
                
                study_id = icdc_sample_to_study[sample_id]

                study_name = icdc_study_name[study_id]

                program_name = icdc_study_to_program[study_id]

                sample_submitter_id = sample_id

                remote_field_name = 'tissue_type'

                remote_value = icdc_sample[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'ICDC', program_name, 'study', study_name, sample_id, sample_submitter_id, 'tumor_vs_normal', remote_field_name, remote_value ], sep='\t', file=OUT )

        if target_submitter_id in found_tumor_vs_normal['idc_tcga']:
            
            for sample_id in sorted( found_tumor_vs_normal['idc_tcga'][target_submitter_id] ):
                
                project_name = idc_tcga_biospecimen[sample_id]['project_short_name']

                program_name = idc_tcga_biospecimen[sample_id]['program_name']

                sample_submitter_id = sample_id

                remote_field_name = 'sample_type_name'

                remote_value = idc_tcga_biospecimen[sample_id][remote_field_name]

                print( *[ target_submitter_id, 'IDC/TCGA', program_name, 'project', project_name, idc_tcga_biospecimen[sample_id]['sample_gdc_id'], sample_submitter_id, 'tumor_vs_normal', remote_field_name, remote_value ], sep='\t', file=OUT )



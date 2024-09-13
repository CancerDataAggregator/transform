#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'subjects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_subjects_merged_into_GDC_CDA_subjects.tsv' )

gdc_pdc_cds_merge_map = path.join( aux_dir, 'CDS_CDA_subjects_merged_into_GDC_PDC_CDA_subjects.tsv' )

gdc_pdc_cds_icdc_merge_map = path.join( aux_dir, 'ICDC_CDA_subjects_merged_into_GDC_PDC_CDS_CDA_subjects.tsv' )

idc_gdc_merge_map = path.join( aux_dir, 'IDC_CDA_subjects_linked_to_GDC_CDA_subjects.tsv' )

idc_pdc_merge_map = path.join( aux_dir, 'IDC_CDA_subjects_linked_to_PDC_CDA_subjects.tsv' )

idc_cds_merge_map = path.join( aux_dir, 'IDC_CDA_subjects_linked_to_CDS_CDA_subjects.tsv' )

idc_icdc_merge_map = path.join( aux_dir, 'IDC_CDA_subjects_linked_to_ICDC_CDA_subjects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_subject_tsv = path.join( gdc_cda_dir, 'subject.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_subject_tsv = path.join( pdc_cda_dir, 'subject.tsv' )

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_subject_tsv = path.join( cds_cda_dir, 'subject.tsv' )

icdc_cda_dir = path.join( cda_root, 'icdc_002_decorated_harmonized' )

icdc_subject_tsv = path.join( icdc_cda_dir, 'subject.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )

idc_subject_tsv = path.join( idc_cda_dir, 'subject.tsv' )

output_file = path.join( aux_dir, 'IDC_CDA_subjects_merged_into_GDC_PDC_CDS_ICDC_CDA_subjects.tsv' )

# EXECUTION

# If we ever alter GDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_CDS_alias (in gdc_pdc_cds_merge_map).

gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_subject_alias', 'new_subject_alias' )

# If we ever alter PDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_CDS_alias (in gdc_pdc_cds_merge_map).

pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_subject_alias', 'new_subject_alias' )

cds_to_merged = map_columns_one_to_one( gdc_pdc_cds_merge_map, 'CDS_subject_alias', 'new_subject_alias' )

icdc_to_merged = map_columns_one_to_one( gdc_pdc_cds_icdc_merge_map, 'ICDC_subject_alias', 'new_subject_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_cds_icdc_merge_map, 'new_subject_alias', 'new_subject_id' )

for merge_file in [ gdc_pdc_cds_merge_map, gdc_pdc_merge_map ]:
    
    new_merged_id = map_columns_one_to_one( merge_file, 'new_subject_alias', 'new_subject_id' )

    for id_alias in new_merged_id:
        
        if id_alias not in merged_id:
            
            merged_id[id_alias] = new_merged_id[id_alias]

idc_to_gdc = map_columns_one_to_one( idc_gdc_merge_map, 'IDC_subject_alias', 'GDC_subject_alias' )

idc_to_pdc = map_columns_one_to_one( idc_pdc_merge_map, 'IDC_subject_alias', 'PDC_subject_alias' )

idc_to_cds = map_columns_one_to_one( idc_cds_merge_map, 'IDC_subject_alias', 'CDS_subject_alias' )

idc_to_icdc = map_columns_one_to_one( idc_icdc_merge_map, 'IDC_subject_alias', 'ICDC_subject_alias' )

gdc_subject_id = map_columns_one_to_one( gdc_subject_tsv, 'id_alias', 'id' )

pdc_subject_id = map_columns_one_to_one( pdc_subject_tsv, 'id_alias', 'id' )

cds_subject_id = map_columns_one_to_one( cds_subject_tsv, 'id_alias', 'id' )

icdc_subject_id = map_columns_one_to_one( icdc_subject_tsv, 'id_alias', 'id' )

idc_subject_id = map_columns_one_to_one( idc_subject_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'IDC_subject_alias', 'IDC_subject_id', 'GDC_PDC_CDS_ICDC_subject_alias', 'GDC_PDC_CDS_ICDC_subject_id', 'new_subject_alias', 'new_subject_id' ], sep='\t', file=OUT )

    for idc_subject_alias in sorted( idc_subject_id ):
        
        if idc_subject_alias in idc_to_icdc:
            
            icdc_target_to_match = idc_to_icdc[idc_subject_alias]

            matching_alias = icdc_target_to_match

            matching_id = icdc_subject_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_subject_alias = icdc_target_to_match

            new_subject_id = icdc_subject_id[new_subject_alias]

            if icdc_target_to_match in icdc_to_merged:
                
                icdc_target_to_match = icdc_to_merged[icdc_target_to_match]

                matching_alias = icdc_target_to_match

                matching_id = merged_id[matching_alias]

                new_subject_alias = icdc_target_to_match

                new_subject_id = merged_id[new_subject_alias]

            if idc_subject_alias in idc_to_cds:
                
                cds_target_to_match = idc_to_cds[idc_subject_alias]

                matching_alias = cds_target_to_match

                matching_id = cds_subject_id[matching_alias]

                new_subject_alias = cds_target_to_match

                new_subject_id = cds_subject_id[new_subject_alias]

                if cds_target_to_match in cds_to_merged:
                    
                    cds_target_to_match = cds_to_merged[cds_target_to_match]

                    matching_alias = cds_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = cds_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if cds_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via CDS) to {cds_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if idc_subject_alias in idc_to_pdc:
                
                pdc_target_to_match = idc_to_pdc[idc_subject_alias]

                matching_alias = pdc_target_to_match

                matching_id = pdc_subject_id[matching_alias]

                new_subject_alias = pdc_target_to_match

                new_subject_id = pdc_subject_id[new_subject_alias]

                if pdc_target_to_match in pdc_to_merged:
                    
                    pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                    matching_alias = pdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = pdc_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if pdc_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if idc_subject_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_subject_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_subject_id[matching_alias]

                new_subject_alias = gdc_target_to_match

                new_subject_id = gdc_subject_id[new_subject_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = gdc_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if gdc_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via GDC) to {gdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            print( *[ idc_subject_alias, idc_subject_id[idc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif idc_subject_alias in idc_to_cds:
            
            cds_target_to_match = idc_to_cds[idc_subject_alias]

            matching_alias = cds_target_to_match

            matching_id = cds_subject_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_subject_alias = cds_target_to_match

            new_subject_id = cds_subject_id[new_subject_alias]

            if cds_target_to_match in cds_to_merged:
                
                cds_target_to_match = cds_to_merged[cds_target_to_match]

                matching_alias = cds_target_to_match

                matching_id = merged_id[matching_alias]

                new_subject_alias = cds_target_to_match

                new_subject_id = merged_id[new_subject_alias]

            if idc_subject_alias in idc_to_pdc:
                
                pdc_target_to_match = idc_to_pdc[idc_subject_alias]

                matching_alias = pdc_target_to_match

                matching_id = pdc_subject_id[matching_alias]

                new_subject_alias = pdc_target_to_match

                new_subject_id = pdc_subject_id[new_subject_alias]

                if pdc_target_to_match in pdc_to_merged:
                    
                    pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                    matching_alias = pdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = pdc_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if pdc_target_to_match != cds_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via CDS) to {cds_target_to_match}; cannot continue, aborting." )

            if idc_subject_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_subject_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_subject_id[matching_alias]

                new_subject_alias = gdc_target_to_match

                new_subject_id = gdc_subject_id[new_subject_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = gdc_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if gdc_target_to_match != cds_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via GDC) to {gdc_target_to_match} but also (via CDS) to {cds_target_to_match}; cannot continue, aborting." )

            print( *[ idc_subject_alias, idc_subject_id[idc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif idc_subject_alias in idc_to_pdc:
            
            pdc_target_to_match = idc_to_pdc[idc_subject_alias]

            matching_alias = pdc_target_to_match

            matching_id = pdc_subject_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_subject_alias = pdc_target_to_match

            new_subject_id = pdc_subject_id[new_subject_alias]

            if pdc_target_to_match in pdc_to_merged:
                
                pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                matching_alias = pdc_target_to_match

                matching_id = merged_id[matching_alias]

                new_subject_alias = pdc_target_to_match

                new_subject_id = merged_id[new_subject_alias]

            if idc_subject_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_subject_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_subject_id[matching_alias]

                new_subject_alias = gdc_target_to_match

                new_subject_id = gdc_subject_id[new_subject_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_subject_alias = gdc_target_to_match

                    new_subject_id = merged_id[new_subject_alias]

                if pdc_target_to_match != gdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC subject alias {idc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ idc_subject_alias, idc_subject_id[idc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif idc_subject_alias in idc_to_gdc:
            
            gdc_target_to_match = idc_to_gdc[idc_subject_alias]

            matching_alias = gdc_target_to_match

            matching_id = gdc_subject_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_subject_alias = gdc_target_to_match

            new_subject_id = gdc_subject_id[new_subject_alias]

            if gdc_target_to_match in gdc_to_merged:
                
                gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                matching_subject_alias = gdc_target_to_match

                matching_subject_id = merged_id[matching_subject_id]

                new_subject_alias = gdc_target_to_match

                new_subject_id = merged_id[new_subject_id]

            print( *[ idc_subject_alias, idc_subject_id[idc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )



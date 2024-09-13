#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'projects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_projects_merged_into_GDC_CDA_projects.tsv' )

gdc_pdc_cds_merge_map = path.join( aux_dir, 'CDS_CDA_projects_merged_into_GDC_PDC_CDA_projects.tsv' )

gdc_pdc_cds_icdc_merge_map = path.join( aux_dir, 'ICDC_CDA_projects_merged_into_GDC_PDC_CDS_CDA_projects.tsv' )

idc_gdc_merge_map = path.join( aux_dir, 'IDC_CDA_projects_linked_to_GDC_CDA_projects.tsv' )

idc_pdc_merge_map = path.join( aux_dir, 'IDC_CDA_projects_linked_to_PDC_CDA_projects.tsv' )

idc_cds_merge_map = path.join( aux_dir, 'IDC_CDA_projects_linked_to_CDS_CDA_projects.tsv' )

idc_icdc_merge_map = path.join( aux_dir, 'IDC_CDA_projects_linked_to_ICDC_CDA_projects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_project_tsv = path.join( gdc_cda_dir, 'project.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_project_tsv = path.join( pdc_cda_dir, 'project.tsv' )

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_project_tsv = path.join( cds_cda_dir, 'project.tsv' )

icdc_cda_dir = path.join( cda_root, 'icdc_002_decorated_harmonized' )

icdc_project_tsv = path.join( icdc_cda_dir, 'project.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )

idc_project_tsv = path.join( idc_cda_dir, 'project.tsv' )

output_file = path.join( aux_dir, 'IDC_CDA_projects_merged_into_GDC_PDC_CDS_ICDC_CDA_projects.tsv' )

# EXECUTION

# If we ever alter GDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_CDS_alias (in gdc_pdc_cds_merge_map).

gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_project_alias', 'new_project_alias' )

# If we ever alter PDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_CDS_alias (in gdc_pdc_cds_merge_map).

pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_project_alias', 'new_project_alias' )

cds_to_merged = map_columns_one_to_one( gdc_pdc_cds_merge_map, 'CDS_project_alias', 'new_project_alias' )

icdc_to_merged = map_columns_one_to_one( gdc_pdc_cds_icdc_merge_map, 'ICDC_project_alias', 'new_project_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_cds_icdc_merge_map, 'new_project_alias', 'new_project_id' )

for merge_file in [ gdc_pdc_cds_merge_map, gdc_pdc_merge_map ]:
    
    new_merged_id = map_columns_one_to_one( merge_file, 'new_project_alias', 'new_project_id' )

    for id_alias in new_merged_id:
        
        if id_alias not in merged_id:
            
            merged_id[id_alias] = new_merged_id[id_alias]

idc_to_gdc = map_columns_one_to_one( idc_gdc_merge_map, 'IDC_project_alias', 'GDC_project_alias' )

idc_to_pdc = map_columns_one_to_one( idc_pdc_merge_map, 'IDC_project_alias', 'PDC_project_alias' )

idc_to_cds = map_columns_one_to_one( idc_cds_merge_map, 'IDC_project_alias', 'CDS_project_alias' )

idc_to_icdc = map_columns_one_to_one( idc_icdc_merge_map, 'IDC_project_alias', 'ICDC_project_alias' )

gdc_project_id = map_columns_one_to_one( gdc_project_tsv, 'id_alias', 'id' )

pdc_project_id = map_columns_one_to_one( pdc_project_tsv, 'id_alias', 'id' )

cds_project_id = map_columns_one_to_one( cds_project_tsv, 'id_alias', 'id' )

icdc_project_id = map_columns_one_to_one( icdc_project_tsv, 'id_alias', 'id' )

idc_project_id = map_columns_one_to_one( idc_project_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'IDC_project_alias', 'IDC_project_id', 'GDC_PDC_CDS_ICDC_project_alias', 'GDC_PDC_CDS_ICDC_project_id', 'new_project_alias', 'new_project_id' ], sep='\t', file=OUT )

    for idc_project_alias in sorted( idc_project_id ):
        
        if idc_project_alias in idc_to_icdc:
            
            icdc_target_to_match = idc_to_icdc[idc_project_alias]

            matching_alias = icdc_target_to_match

            matching_id = icdc_project_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_project_alias = icdc_target_to_match

            new_project_id = icdc_project_id[new_project_alias]

            if icdc_target_to_match in icdc_to_merged:
                
                icdc_target_to_match = icdc_to_merged[icdc_target_to_match]

                matching_alias = icdc_target_to_match

                matching_id = merged_id[matching_alias]

                new_project_alias = icdc_target_to_match

                new_project_id = merged_id[new_project_alias]

            if idc_project_alias in idc_to_cds:
                
                cds_target_to_match = idc_to_cds[idc_project_alias]

                matching_alias = cds_target_to_match

                matching_id = cds_project_id[matching_alias]

                new_project_alias = cds_target_to_match

                new_project_id = cds_project_id[new_project_alias]

                if cds_target_to_match in cds_to_merged:
                    
                    cds_target_to_match = cds_to_merged[cds_target_to_match]

                    matching_alias = cds_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = cds_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if cds_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via CDS) to {cds_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if idc_project_alias in idc_to_pdc:
                
                pdc_target_to_match = idc_to_pdc[idc_project_alias]

                matching_alias = pdc_target_to_match

                matching_id = pdc_project_id[matching_alias]

                new_project_alias = pdc_target_to_match

                new_project_id = pdc_project_id[new_project_alias]

                if pdc_target_to_match in pdc_to_merged:
                    
                    pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                    matching_alias = pdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = pdc_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if pdc_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via PDC) to {pdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if idc_project_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_project_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_project_id[matching_alias]

                new_project_alias = gdc_target_to_match

                new_project_id = gdc_project_id[new_project_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = gdc_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if gdc_target_to_match != icdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via GDC) to {gdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            print( *[ idc_project_alias, idc_project_id[idc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )

        elif idc_project_alias in idc_to_cds:
            
            cds_target_to_match = idc_to_cds[idc_project_alias]

            matching_alias = cds_target_to_match

            matching_id = cds_project_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_project_alias = cds_target_to_match

            new_project_id = cds_project_id[new_project_alias]

            if cds_target_to_match in cds_to_merged:
                
                cds_target_to_match = cds_to_merged[cds_target_to_match]

                matching_alias = cds_target_to_match

                matching_id = merged_id[matching_alias]

                new_project_alias = cds_target_to_match

                new_project_id = merged_id[new_project_alias]

            if idc_project_alias in idc_to_pdc:
                
                pdc_target_to_match = idc_to_pdc[idc_project_alias]

                matching_alias = pdc_target_to_match

                matching_id = pdc_project_id[matching_alias]

                new_project_alias = pdc_target_to_match

                new_project_id = pdc_project_id[new_project_alias]

                if pdc_target_to_match in pdc_to_merged:
                    
                    pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                    matching_alias = pdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = pdc_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if pdc_target_to_match != cds_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via PDC) to {pdc_target_to_match} but also (via CDS) to {cds_target_to_match}; cannot continue, aborting." )

            if idc_project_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_project_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_project_id[matching_alias]

                new_project_alias = gdc_target_to_match

                new_project_id = gdc_project_id[new_project_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = gdc_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if gdc_target_to_match != cds_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via GDC) to {gdc_target_to_match} but also (via CDS) to {cds_target_to_match}; cannot continue, aborting." )

            print( *[ idc_project_alias, idc_project_id[idc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )

        elif idc_project_alias in idc_to_pdc:
            
            pdc_target_to_match = idc_to_pdc[idc_project_alias]

            matching_alias = pdc_target_to_match

            matching_id = pdc_project_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_project_alias = pdc_target_to_match

            new_project_id = pdc_project_id[new_project_alias]

            if pdc_target_to_match in pdc_to_merged:
                
                pdc_target_to_match = pdc_to_merged[pdc_target_to_match]

                matching_alias = pdc_target_to_match

                matching_id = merged_id[matching_alias]

                new_project_alias = pdc_target_to_match

                new_project_id = merged_id[new_project_alias]

            if idc_project_alias in idc_to_gdc:
                
                gdc_target_to_match = idc_to_gdc[idc_project_alias]

                matching_alias = gdc_target_to_match

                matching_id = gdc_project_id[matching_alias]

                new_project_alias = gdc_target_to_match

                new_project_id = gdc_project_id[new_project_alias]

                if gdc_target_to_match in gdc_to_merged:
                    
                    gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                    matching_alias = gdc_target_to_match

                    matching_id = merged_id[matching_alias]

                    new_project_alias = gdc_target_to_match

                    new_project_id = merged_id[new_project_alias]

                if pdc_target_to_match != gdc_target_to_match:
                    
                    # We hope never to get here.

                    sys.exit( f"FATAL: IDC project alias {idc_project_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ idc_project_alias, idc_project_id[idc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )

        elif idc_project_alias in idc_to_gdc:
            
            gdc_target_to_match = idc_to_gdc[idc_project_alias]

            matching_alias = gdc_target_to_match

            matching_id = gdc_project_id[matching_alias]

            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

            new_project_alias = gdc_target_to_match

            new_project_id = gdc_project_id[new_project_alias]

            if gdc_target_to_match in gdc_to_merged:
                
                gdc_target_to_match = gdc_to_merged[gdc_target_to_match]

                matching_project_alias = gdc_target_to_match

                matching_project_id = merged_id[matching_project_id]

                new_project_alias = gdc_target_to_match

                new_project_id = merged_id[new_project_id]

            print( *[ idc_project_alias, idc_project_id[idc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )



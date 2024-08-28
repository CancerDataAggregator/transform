#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'projects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_projects_merged_into_GDC_CDA_projects.tsv' )

cds_gdc_merge_map = path.join( aux_dir, 'CDS_CDA_projects_linked_to_GDC_CDA_projects.tsv' )

cds_pdc_merge_map = path.join( aux_dir, 'CDS_CDA_projects_linked_to_PDC_CDA_projects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_project_tsv = path.join( gdc_cda_dir, 'project.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_project_tsv = path.join( pdc_cda_dir, 'project.tsv' )

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_project_tsv = path.join( cds_cda_dir, 'project.tsv' )

output_file = path.join( aux_dir, 'CDS_CDA_projects_merged_into_GDC_PDC_CDA_projects.tsv' )

# EXECUTION

pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_project_alias', 'new_project_alias' )

gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_project_alias', 'new_project_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_merge_map, 'new_project_alias', 'new_project_id' )

cds_to_gdc = map_columns_one_to_one( cds_gdc_merge_map, 'CDS_project_alias', 'GDC_project_alias' )

cds_to_pdc = map_columns_one_to_one( cds_pdc_merge_map, 'CDS_project_alias', 'PDC_project_alias' )

gdc_project_id = map_columns_one_to_one( gdc_project_tsv, 'id_alias', 'id' )

pdc_project_id = map_columns_one_to_one( pdc_project_tsv, 'id_alias', 'id' )

cds_project_id = map_columns_one_to_one( cds_project_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'CDS_project_alias', 'CDS_project_id', 'GDC_PDC_project_alias', 'GDC_PDC_project_id', 'new_project_alias', 'new_project_id' ], sep='\t', file=OUT )

    for cds_project_alias in sorted( cds_project_id ):
        
        if cds_project_alias in cds_to_pdc:
            
            pdc_target_to_match = cds_to_pdc[cds_project_alias]

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

            if cds_project_alias in cds_to_gdc:
                
                gdc_target_to_match = cds_to_gdc[cds_project_alias]

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

                    sys.exit( f"FATAL: CDS project alias {cds_project_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ cds_project_alias, cds_project_id[cds_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )

        elif cds_project_alias in cds_to_gdc:
            
            gdc_target_to_match = cds_to_gdc[cds_project_alias]

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

            print( *[ cds_project_alias, cds_project_id[cds_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )



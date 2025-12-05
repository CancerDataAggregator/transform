#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'projects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_projects_merged_into_GDC_CDA_projects.tsv' )

gc_gdc_merge_map = path.join( aux_dir, 'GC_CDA_projects_linked_to_GDC_CDA_projects.tsv' )

gc_pdc_merge_map = path.join( aux_dir, 'GC_CDA_projects_linked_to_PDC_CDA_projects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_project_tsv = path.join( gdc_cda_dir, 'project.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_project_tsv = path.join( pdc_cda_dir, 'project.tsv' )

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )

gc_project_tsv = path.join( gc_cda_dir, 'project.tsv' )

output_file = path.join( aux_dir, 'GC_CDA_projects_merged_into_GDC_PDC_CDA_projects.tsv' )

# EXECUTION

pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_project_alias', 'new_project_alias' )

gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_project_alias', 'new_project_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_merge_map, 'new_project_alias', 'new_project_id' )

gc_to_gdc = map_columns_one_to_one( gc_gdc_merge_map, 'GC_project_alias', 'GDC_project_alias' )

gc_to_pdc = map_columns_one_to_one( gc_pdc_merge_map, 'GC_project_alias', 'PDC_project_alias' )

gdc_project_id = map_columns_one_to_one( gdc_project_tsv, 'id_alias', 'id' )

pdc_project_id = map_columns_one_to_one( pdc_project_tsv, 'id_alias', 'id' )

gc_project_id = map_columns_one_to_one( gc_project_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'GC_project_alias', 'GC_project_id', 'GDC_PDC_project_alias', 'GDC_PDC_project_id', 'new_project_alias', 'new_project_id' ], sep='\t', file=OUT )

    for gc_project_alias in sorted( gc_project_id ):
        
        if gc_project_alias in gc_to_pdc:
            
            pdc_target_to_match = gc_to_pdc[gc_project_alias]

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

            if gc_project_alias in gc_to_gdc:
                
                gdc_target_to_match = gc_to_gdc[gc_project_alias]

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

                    sys.exit( f"FATAL: GC project alias {gc_project_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ gc_project_alias, gc_project_id[gc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )

        elif gc_project_alias in gc_to_gdc:
            
            gdc_target_to_match = gc_to_gdc[gc_project_alias]

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

            print( *[ gc_project_alias, gc_project_id[gc_project_alias], matching_alias, matching_id, new_project_alias, new_project_id ], sep='\t', file=OUT )



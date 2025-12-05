#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'subjects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_subjects_merged_into_GDC_CDA_subjects.tsv' )

gc_gdc_merge_map = path.join( aux_dir, 'GC_CDA_subjects_linked_to_GDC_CDA_subjects.tsv' )

gc_pdc_merge_map = path.join( aux_dir, 'GC_CDA_subjects_linked_to_PDC_CDA_subjects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_subject_tsv = path.join( gdc_cda_dir, 'subject.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_subject_tsv = path.join( pdc_cda_dir, 'subject.tsv' )

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )

gc_subject_tsv = path.join( gc_cda_dir, 'subject.tsv' )

output_file = path.join( aux_dir, 'GC_CDA_subjects_merged_into_GDC_PDC_CDA_subjects.tsv' )

# EXECUTION

pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_subject_alias', 'new_subject_alias' )

gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_subject_alias', 'new_subject_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_merge_map, 'new_subject_alias', 'new_subject_id' )

gc_to_gdc = map_columns_one_to_one( gc_gdc_merge_map, 'GC_subject_alias', 'GDC_subject_alias' )

gc_to_pdc = map_columns_one_to_one( gc_pdc_merge_map, 'GC_subject_alias', 'PDC_subject_alias' )

gdc_subject_id = map_columns_one_to_one( gdc_subject_tsv, 'id_alias', 'id' )

pdc_subject_id = map_columns_one_to_one( pdc_subject_tsv, 'id_alias', 'id' )

gc_subject_id = map_columns_one_to_one( gc_subject_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'GC_subject_alias', 'GC_subject_id', 'GDC_PDC_subject_alias', 'GDC_PDC_subject_id', 'new_subject_alias', 'new_subject_id' ], sep='\t', file=OUT )

    for gc_subject_alias in sorted( gc_subject_id ):
        
        if gc_subject_alias in gc_to_pdc:
            
            pdc_target_to_match = gc_to_pdc[gc_subject_alias]

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

            if gc_subject_alias in gc_to_gdc:
                
                gdc_target_to_match = gc_to_gdc[gc_subject_alias]

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

                    sys.exit( f"FATAL: GC subject alias {gc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ gc_subject_alias, gc_subject_id[gc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif gc_subject_alias in gc_to_gdc:
            
            gdc_target_to_match = gc_to_gdc[gc_subject_alias]

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

            print( *[ gc_subject_alias, gc_subject_id[gc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )



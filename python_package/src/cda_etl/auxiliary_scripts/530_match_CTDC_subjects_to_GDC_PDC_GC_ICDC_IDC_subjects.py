#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

# PARAMETERS

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'subjects' )

gdc_pdc_merge_map = path.join( aux_dir, 'PDC_CDA_subjects_merged_into_GDC_CDA_subjects.tsv' )
gdc_pdc_gc_merge_map = path.join( aux_dir, 'GC_CDA_subjects_merged_into_GDC_PDC_CDA_subjects.tsv' )
gdc_pdc_gc_icdc_merge_map = path.join( aux_dir, 'ICDC_CDA_subjects_merged_into_GDC_PDC_GC_CDA_subjects.tsv' )
gdc_pdc_gc_icdc_idc_merge_map = path.join( aux_dir, 'IDC_CDA_subjects_merged_into_GDC_PDC_GC_ICDC_CDA_subjects.tsv' )

ctdc_gdc_merge_map = path.join( aux_dir, 'CTDC_CDA_subjects_linked_to_GDC_CDA_subjects.tsv' )
ctdc_pdc_merge_map = path.join( aux_dir, 'CTDC_CDA_subjects_linked_to_PDC_CDA_subjects.tsv' )
ctdc_gc_merge_map = path.join( aux_dir, 'CTDC_CDA_subjects_linked_to_GC_CDA_subjects.tsv' )
ctdc_icdc_merge_map = path.join( aux_dir, 'CTDC_CDA_subjects_linked_to_ICDC_CDA_subjects.tsv' )
ctdc_idc_merge_map = path.join( aux_dir, 'CTDC_CDA_subjects_linked_to_IDC_CDA_subjects.tsv' )

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )
gdc_subject_tsv = path.join( gdc_cda_dir, 'subject.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )
pdc_subject_tsv = path.join( pdc_cda_dir, 'subject.tsv' )

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )
gc_subject_tsv = path.join( gc_cda_dir, 'subject.tsv' )

icdc_cda_dir = path.join( cda_root, 'icdc_002_decorated_harmonized' )
icdc_subject_tsv = path.join( icdc_cda_dir, 'subject.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )
idc_subject_tsv = path.join( idc_cda_dir, 'subject.tsv' )

ctdc_cda_dir = path.join( cda_root, 'ctdc_002_decorated_harmonized' )
ctdc_subject_tsv = path.join( ctdc_cda_dir, 'subject.tsv' )

output_file = path.join( aux_dir, 'CTDC_CDA_subjects_merged_into_GDC_PDC_GC_ICDC_IDC_CDA_subjects.tsv' )

# EXECUTION

# If we ever alter GDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_GC_alias (in gdc_pdc_gc_merge_map).
gdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'GDC_subject_alias', 'new_subject_alias' )
# If we ever alter PDC IDs when merging in new data, this will need to be forwarded to, and resolved by, another transitive hop from GDC_PDC_alias (the rvalue in this assignment) -> GDC_PDC_GC_alias (in gdc_pdc_gc_merge_map).
pdc_to_merged = map_columns_one_to_one( gdc_pdc_merge_map, 'PDC_subject_alias', 'new_subject_alias' )
gc_to_merged = map_columns_one_to_one( gdc_pdc_gc_merge_map, 'GC_subject_alias', 'new_subject_alias' )
icdc_to_merged = map_columns_one_to_one( gdc_pdc_gc_icdc_merge_map, 'ICDC_subject_alias', 'new_subject_alias' )
idc_to_merged = map_columns_one_to_one( gdc_pdc_gc_icdc_idc_merge_map, 'IDC_subject_alias', 'new_subject_alias' )

merged_id = map_columns_one_to_one( gdc_pdc_gc_icdc_idc_merge_map, 'new_subject_alias', 'new_subject_id' )

for merge_file in [ gdc_pdc_gc_icdc_merge_map, gdc_pdc_gc_merge_map, gdc_pdc_merge_map ]:
    new_merged_id = map_columns_one_to_one( merge_file, 'new_subject_alias', 'new_subject_id' )
    for id_alias in new_merged_id:
        if id_alias not in merged_id:
            merged_id[id_alias] = new_merged_id[id_alias]

ctdc_to_gdc = map_columns_one_to_one( ctdc_gdc_merge_map, 'CTDC_subject_alias', 'GDC_subject_alias' )
ctdc_to_pdc = map_columns_one_to_one( ctdc_pdc_merge_map, 'CTDC_subject_alias', 'PDC_subject_alias' )
ctdc_to_gc = map_columns_one_to_one( ctdc_gc_merge_map, 'CTDC_subject_alias', 'GC_subject_alias' )
ctdc_to_icdc = map_columns_one_to_one( ctdc_icdc_merge_map, 'CTDC_subject_alias', 'ICDC_subject_alias' )
ctdc_to_idc = map_columns_one_to_one( ctdc_idc_merge_map, 'CTDC_subject_alias', 'IDC_subject_alias' )

gdc_subject_id = map_columns_one_to_one( gdc_subject_tsv, 'id_alias', 'id' )
pdc_subject_id = map_columns_one_to_one( pdc_subject_tsv, 'id_alias', 'id' )
gc_subject_id = map_columns_one_to_one( gc_subject_tsv, 'id_alias', 'id' )
icdc_subject_id = map_columns_one_to_one( icdc_subject_tsv, 'id_alias', 'id' )
idc_subject_id = map_columns_one_to_one( idc_subject_tsv, 'id_alias', 'id' )
ctdc_subject_id = map_columns_one_to_one( ctdc_subject_tsv, 'id_alias', 'id' )

with open( output_file, 'w' ) as OUT:
    print( *[ 'CTDC_subject_alias', 'CTDC_subject_id', 'GDC_PDC_GC_ICDC_IDC_subject_alias', 'GDC_PDC_GC_ICDC_IDC_subject_id', 'new_subject_alias', 'new_subject_id' ], sep='\t', file=OUT )

    for ctdc_subject_alias in sorted( ctdc_subject_id ):
        
        if ctdc_subject_alias in ctdc_to_idc:
            idc_target_to_match = ctdc_to_idc[ctdc_subject_alias]
            matching_alias = idc_target_to_match
            matching_id = idc_subject_id[matching_alias]
            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.
            new_subject_alias = idc_target_to_match
            new_subject_id = idc_subject_id[new_subject_alias]

            if idc_target_to_match in idc_to_merged:
                idc_target_to_match = idc_to_merged[idc_target_to_match]
                matching_alias = idc_target_to_match
                matching_id = merged_id[matching_alias]
                new_subject_alias = idc_target_to_match
                new_subject_id = merged_id[new_subject_alias]

            if ctdc_subject_alias in ctdc_to_icdc:
                icdc_target_to_match = ctdc_to_icdc[ctdc_subject_alias]
                matching_alias = icdc_target_to_match
                matching_id = icdc_subject_id[matching_alias]
                new_subject_alias = icdc_target_to_match
                new_subject_id = icdc_subject_id[new_subject_alias]

                if icdc_target_to_match in icdc_to_merged:
                    icdc_target_to_match = icdc_to_merged[icdc_target_to_match]
                    matching_alias = icdc_target_to_match
                    matching_id = merged_id[matching_alias]
                    new_subject_alias = icdc_target_to_match
                    new_subject_id = merged_id[new_subject_alias]

                if icdc_target_to_match != idc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via ICDC) to {icdc_target_to_match} but also (via IDC) to {idc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_gc:
                gc_target_to_match = ctdc_to_gc[ctdc_subject_alias]
                matching_alias = gc_target_to_match
                matching_id = gc_subject_id[matching_alias]
                new_subject_alias = gc_target_to_match
                new_subject_id = gc_subject_id[new_subject_alias]

                if gc_target_to_match in gc_to_merged:
                    gc_target_to_match = gc_to_merged[gc_target_to_match]
                    matching_alias = gc_target_to_match
                    matching_id = merged_id[matching_alias]
                    new_subject_alias = gc_target_to_match
                    new_subject_id = merged_id[new_subject_alias]

                if gc_target_to_match != idc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via GC) to {gc_target_to_match} but also (via IDC) to {idc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_pdc:
                pdc_target_to_match = ctdc_to_pdc[ctdc_subject_alias]
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

                if pdc_target_to_match != idc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via IDC) to {idc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_gdc:
                gdc_target_to_match = ctdc_to_gdc[ctdc_subject_alias]
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

                if gdc_target_to_match != idc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via GDC) to {gdc_target_to_match} but also (via IDC) to {idc_target_to_match}; cannot continue, aborting." )

            print( *[ ctdc_subject_alias, ctdc_subject_id[ctdc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif ctdc_subject_alias in ctdc_to_icdc:
            icdc_target_to_match = ctdc_to_icdc[ctdc_subject_alias]
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

            if ctdc_subject_alias in ctdc_to_gc:
                gc_target_to_match = ctdc_to_gc[ctdc_subject_alias]
                matching_alias = gc_target_to_match
                matching_id = gc_subject_id[matching_alias]
                new_subject_alias = gc_target_to_match
                new_subject_id = gc_subject_id[new_subject_alias]

                if gc_target_to_match in gc_to_merged:
                    gc_target_to_match = gc_to_merged[gc_target_to_match]
                    matching_alias = gc_target_to_match
                    matching_id = merged_id[matching_alias]
                    new_subject_alias = gc_target_to_match
                    new_subject_id = merged_id[new_subject_alias]

                if gc_target_to_match != icdc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via GC) to {gc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_pdc:
                pdc_target_to_match = ctdc_to_pdc[ctdc_subject_alias]
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
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_gdc:
                gdc_target_to_match = ctdc_to_gdc[ctdc_subject_alias]
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
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via GDC) to {gdc_target_to_match} but also (via ICDC) to {icdc_target_to_match}; cannot continue, aborting." )

            print( *[ ctdc_subject_alias, ctdc_subject_id[ctdc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif ctdc_subject_alias in ctdc_to_gc:
            gc_target_to_match = ctdc_to_gc[ctdc_subject_alias]
            matching_alias = gc_target_to_match
            matching_id = gc_subject_id[matching_alias]
            # If we keep the old alias value, we don't have to rewrite existing table data. If a
            # reason to make new aliases and/or IDs appears in the future, this'll make it easy.
            new_subject_alias = gc_target_to_match
            new_subject_id = gc_subject_id[new_subject_alias]

            if gc_target_to_match in gc_to_merged:
                gc_target_to_match = gc_to_merged[gc_target_to_match]
                matching_alias = gc_target_to_match
                matching_id = merged_id[matching_alias]
                new_subject_alias = gc_target_to_match
                new_subject_id = merged_id[new_subject_alias]

            if ctdc_subject_alias in ctdc_to_pdc:
                pdc_target_to_match = ctdc_to_pdc[ctdc_subject_alias]
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

                if pdc_target_to_match != gc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via GC) to {gc_target_to_match}; cannot continue, aborting." )

            if ctdc_subject_alias in ctdc_to_gdc:
                gdc_target_to_match = ctdc_to_gdc[ctdc_subject_alias]
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

                if gdc_target_to_match != gc_target_to_match:
                    # We hope never to get here.
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via GDC) to {gdc_target_to_match} but also (via GC) to {gc_target_to_match}; cannot continue, aborting." )

            print( *[ ctdc_subject_alias, ctdc_subject_id[ctdc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif ctdc_subject_alias in ctdc_to_pdc:
            pdc_target_to_match = ctdc_to_pdc[ctdc_subject_alias]
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

            if ctdc_subject_alias in ctdc_to_gdc:
                gdc_target_to_match = ctdc_to_gdc[ctdc_subject_alias]
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
                    sys.exit( f"FATAL: CTDC subject alias {ctdc_subject_alias} matched (via PDC) to {pdc_target_to_match} but also (via GDC) to {gdc_target_to_match}; cannot continue, aborting." )

            print( *[ ctdc_subject_alias, ctdc_subject_id[ctdc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )

        elif ctdc_subject_alias in ctdc_to_gdc:
            gdc_target_to_match = ctdc_to_gdc[ctdc_subject_alias]
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

            print( *[ ctdc_subject_alias, ctdc_subject_id[ctdc_subject_alias], matching_alias, matching_id, new_subject_alias, new_subject_id ], sep='\t', file=OUT )



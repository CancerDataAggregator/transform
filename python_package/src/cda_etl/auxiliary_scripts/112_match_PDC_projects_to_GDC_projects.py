#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_project_tsv = path.join( gdc_cda_dir, 'project.tsv' )

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_project_tsv = path.join( pdc_cda_dir, 'project.tsv' )

aux_root = 'auxiliary_metadata'

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

output_file = path.join( agg_project_dir, 'PDC_CDA_projects_merged_into_GDC_CDA_projects.tsv' )

# EXECUTION

gdc_project = load_tsv_as_dict( gdc_project_tsv )

gdc_project_alias_to_project_id = map_columns_one_to_one( gdc_project_tsv, 'id_alias', 'id' )

pdc_project = load_tsv_as_dict( pdc_project_tsv )

pdc_project_alias_to_project_id = map_columns_one_to_one( pdc_project_tsv, 'id_alias', 'id' )

pdc_project_alias_to_gdc_project_alias = dict()

# Identify dbGaP studies: these are straightforward to equate.

for pdc_project_id in sorted( pdc_project ):
    
    pdc_project_alias = pdc_project[pdc_project_id]['id_alias']

    if pdc_project[pdc_project_id]['type'] == 'dbgap_study' and pdc_project_id in gdc_project:
        
        gdc_project_alias = gdc_project[pdc_project_id]['id_alias']

        if pdc_project_alias in pdc_project_alias_to_gdc_project_alias and pdc_project_alias_to_gdc_project_alias[pdc_project_alias] != gdc_project_alias:
            
            # We should never get here.

            sys.exit( f"FATAL: PDC project {pdc_project_id} associated with two distinct GDC projects: {gdc_project_alias_to_project_id[pdc_project_alias_to_gdc_project_alias[pdc_project_alias]]} ({pdc_project_alias_to_gdc_project_alias[pdc_project_alias]}) and {gdc_project_alias_to_project_id[gdc_project_alias]} ({gdc_project_alias}); aborting." )

        else:
            
            pdc_project_alias_to_gdc_project_alias[pdc_project_alias] = gdc_project_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'PDC_project_alias', 'PDC_project_id', 'GDC_project_alias', 'GDC_project_id', 'new_project_alias', 'new_project_id' ], sep='\t', file=OUT )

    for pdc_project_alias in sorted( pdc_project_alias_to_gdc_project_alias ):
        
        pdc_project_id = pdc_project_alias_to_project_id[pdc_project_alias]

        gdc_project_alias = pdc_project_alias_to_gdc_project_alias[pdc_project_alias]

        gdc_project_id = gdc_project_alias_to_project_id[gdc_project_alias]

        # If we keep the old alias value, we don't have to rewrite GDC table data. If a
        # reason to make new aliases and/or IDs appears in the future, this'll make it easy.

        new_project_alias = gdc_project_alias

        new_project_id = gdc_project_id

        print( *[ pdc_project_alias, pdc_project_id, gdc_project_alias, gdc_project_id, new_project_alias, new_project_id ], sep='\t', file=OUT )



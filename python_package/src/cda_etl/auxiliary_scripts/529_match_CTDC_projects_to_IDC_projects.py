#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )
idc_project_tsv = path.join( idc_cda_dir, 'project.tsv' )

ctdc_cda_dir = path.join( cda_root, 'ctdc_002_decorated_harmonized' )
ctdc_project_tsv = path.join( ctdc_cda_dir, 'project.tsv' )

aux_root = 'auxiliary_metadata'

agg_root = path.join( aux_root, '__aggregation_logs' )
agg_project_dir = path.join( agg_root, 'projects' )
output_file = path.join( agg_project_dir, 'CTDC_CDA_projects_linked_to_IDC_CDA_projects.tsv' )

# EXECUTION

idc_project = load_tsv_as_dict( idc_project_tsv )
idc_project_alias_to_project_id = map_columns_one_to_one( idc_project_tsv, 'id_alias', 'id' )

ctdc_project = load_tsv_as_dict( ctdc_project_tsv )
ctdc_project_alias_to_project_id = map_columns_one_to_one( ctdc_project_tsv, 'id_alias', 'id' )

ctdc_project_alias_to_idc_project_alias = dict()

# Identify dbGaP studies: these are straightforward to equate.
for ctdc_project_id in sorted( ctdc_project ):
    ctdc_project_alias = ctdc_project[ctdc_project_id]['id_alias']

    if ctdc_project[ctdc_project_id]['type'] == 'dbgap_study' and ctdc_project_id in idc_project:
        idc_project_alias = idc_project[ctdc_project_id]['id_alias']
        if ctdc_project_alias in ctdc_project_alias_to_idc_project_alias and ctdc_project_alias_to_idc_project_alias[ctdc_project_alias] != idc_project_alias:
            # We should never get here.
            sys.exit( f"FATAL: CTDC project {ctdc_project_id} associated with two distinct IDC projects: {idc_project_alias_to_project_id[ctdc_project_alias_to_idc_project_alias[ctdc_project_alias]]} ({ctdc_project_alias_to_idc_project_alias[ctdc_project_alias]}) and {idc_project_alias_to_project_id[idc_project_alias]} ({idc_project_alias}); aborting." )
        else:
            ctdc_project_alias_to_idc_project_alias[ctdc_project_alias] = idc_project_alias

# Write results.
with open( output_file, 'w' ) as OUT:
    print( *[ 'CTDC_project_alias', 'CTDC_project_id', 'IDC_project_alias', 'IDC_project_id' ], sep='\t', file=OUT )
    for ctdc_project_alias in sorted( ctdc_project_alias_to_idc_project_alias ):
        ctdc_project_id = ctdc_project_alias_to_project_id[ctdc_project_alias]
        idc_project_alias = ctdc_project_alias_to_idc_project_alias[ctdc_project_alias]
        idc_project_id = idc_project_alias_to_project_id[idc_project_alias]
        print( *[ ctdc_project_alias, ctdc_project_id, idc_project_alias, idc_project_id ], sep='\t', file=OUT )



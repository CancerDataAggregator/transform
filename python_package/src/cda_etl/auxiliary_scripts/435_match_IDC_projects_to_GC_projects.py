#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )

gc_project_tsv = path.join( gc_cda_dir, 'project.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )

idc_project_tsv = path.join( idc_cda_dir, 'project.tsv' )

aux_root = 'auxiliary_metadata'

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

output_file = path.join( agg_project_dir, 'IDC_CDA_projects_linked_to_GC_CDA_projects.tsv' )

# EXECUTION

gc_project = load_tsv_as_dict( gc_project_tsv )

gc_project_alias_to_project_id = map_columns_one_to_one( gc_project_tsv, 'id_alias', 'id' )

idc_project = load_tsv_as_dict( idc_project_tsv )

idc_project_alias_to_project_id = map_columns_one_to_one( idc_project_tsv, 'id_alias', 'id' )

idc_project_alias_to_gc_project_alias = dict()

# Identify dbGaP studies: these are straightforward to equate.

for idc_project_id in sorted( idc_project ):
    
    idc_project_alias = idc_project[idc_project_id]['id_alias']

    if idc_project[idc_project_id]['type'] == 'dbgap_study' and idc_project_id in gc_project:
        
        gc_project_alias = gc_project[idc_project_id]['id_alias']

        if idc_project_alias in idc_project_alias_to_gc_project_alias and idc_project_alias_to_gc_project_alias[idc_project_alias] != gc_project_alias:
            
            # We should never get here.

            sys.exit( f"FATAL: IDC project {idc_project_id} associated with two distinct GC projects: {gc_project_alias_to_project_id[idc_project_alias_to_gc_project_alias[idc_project_alias]]} ({idc_project_alias_to_gc_project_alias[idc_project_alias]}) and {gc_project_alias_to_project_id[gc_project_alias]} ({gc_project_alias}); aborting." )

        else:
            
            idc_project_alias_to_gc_project_alias[idc_project_alias] = gc_project_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'IDC_project_alias', 'IDC_project_id', 'GC_project_alias', 'GC_project_id' ], sep='\t', file=OUT )

    for idc_project_alias in sorted( idc_project_alias_to_gc_project_alias ):
        
        idc_project_id = idc_project_alias_to_project_id[idc_project_alias]

        gc_project_alias = idc_project_alias_to_gc_project_alias[idc_project_alias]

        gc_project_id = gc_project_alias_to_project_id[gc_project_alias]

        print( *[ idc_project_alias, idc_project_id, gc_project_alias, gc_project_id ], sep='\t', file=OUT )



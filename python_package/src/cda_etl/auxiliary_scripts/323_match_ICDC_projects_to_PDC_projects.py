#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_project_tsv = path.join( pdc_cda_dir, 'project.tsv' )

icdc_cda_dir = path.join( cda_root, 'icdc_002_decorated_harmonized' )

icdc_project_tsv = path.join( icdc_cda_dir, 'project.tsv' )

aux_root = 'auxiliary_metadata'

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

output_file = path.join( agg_project_dir, 'ICDC_CDA_projects_linked_to_PDC_CDA_projects.tsv' )

# EXECUTION

pdc_project = load_tsv_as_dict( pdc_project_tsv )

pdc_project_alias_to_project_id = map_columns_one_to_one( pdc_project_tsv, 'id_alias', 'id' )

icdc_project = load_tsv_as_dict( icdc_project_tsv )

icdc_project_alias_to_project_id = map_columns_one_to_one( icdc_project_tsv, 'id_alias', 'id' )

icdc_project_alias_to_pdc_project_alias = dict()

# Identify dbGaP studies: these are straightforward to equate.

for icdc_project_id in sorted( icdc_project ):
    
    icdc_project_alias = icdc_project[icdc_project_id]['id_alias']

    if icdc_project[icdc_project_id]['type'] == 'dbgap_study' and icdc_project_id in pdc_project:
        
        pdc_project_alias = pdc_project[icdc_project_id]['id_alias']

        if icdc_project_alias in icdc_project_alias_to_pdc_project_alias and icdc_project_alias_to_pdc_project_alias[icdc_project_alias] != pdc_project_alias:
            
            # We should never get here.

            sys.exit( f"FATAL: ICDC project {icdc_project_id} associated with two distinct PDC projects: {pdc_project_alias_to_project_id[icdc_project_alias_to_pdc_project_alias[icdc_project_alias]]} ({icdc_project_alias_to_pdc_project_alias[icdc_project_alias]}) and {pdc_project_alias_to_project_id[pdc_project_alias]} ({pdc_project_alias}); aborting." )

        else:
            
            icdc_project_alias_to_pdc_project_alias[icdc_project_alias] = pdc_project_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'ICDC_project_alias', 'ICDC_project_id', 'PDC_project_alias', 'PDC_project_id' ], sep='\t', file=OUT )

    for icdc_project_alias in sorted( icdc_project_alias_to_pdc_project_alias ):
        
        icdc_project_id = icdc_project_alias_to_project_id[icdc_project_alias]

        pdc_project_alias = icdc_project_alias_to_pdc_project_alias[icdc_project_alias]

        pdc_project_id = pdc_project_alias_to_project_id[pdc_project_alias]

        print( *[ icdc_project_alias, icdc_project_id, pdc_project_alias, pdc_project_id ], sep='\t', file=OUT )



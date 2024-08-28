#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_project_tsv = path.join( gdc_cda_dir, 'project.tsv' )

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_project_tsv = path.join( cds_cda_dir, 'project.tsv' )

aux_root = 'auxiliary_metadata'

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

output_file = path.join( agg_project_dir, 'CDS_CDA_projects_linked_to_GDC_CDA_projects.tsv' )

# EXECUTION

gdc_project = load_tsv_as_dict( gdc_project_tsv )

gdc_project_alias_to_project_id = map_columns_one_to_one( gdc_project_tsv, 'id_alias', 'id' )

cds_project = load_tsv_as_dict( cds_project_tsv )

cds_project_alias_to_project_id = map_columns_one_to_one( cds_project_tsv, 'id_alias', 'id' )

cds_project_alias_to_gdc_project_alias = dict()

# Identify dbGaP studies: these are straightforward to equate.

for cds_project_id in sorted( cds_project ):
    
    cds_project_alias = cds_project[cds_project_id]['id_alias']

    if cds_project[cds_project_id]['type'] == 'dbgap_study' and cds_project_id in gdc_project:
        
        gdc_project_alias = gdc_project[cds_project_id]['id_alias']

        if cds_project_alias in cds_project_alias_to_gdc_project_alias and cds_project_alias_to_gdc_project_alias[cds_project_alias] != gdc_project_alias:
            
            # We should never get here.

            sys.exit( f"FATAL: CDS project {cds_project_id} associated with two distinct GDC projects: {gdc_project_alias_to_project_id[cds_project_alias_to_gdc_project_alias[cds_project_alias]]} ({cds_project_alias_to_gdc_project_alias[cds_project_alias]}) and {gdc_project_alias_to_project_id[gdc_project_alias]} ({gdc_project_alias}); aborting." )

        else:
            
            cds_project_alias_to_gdc_project_alias[cds_project_alias] = gdc_project_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'CDS_project_alias', 'CDS_project_id', 'GDC_project_alias', 'GDC_project_id' ], sep='\t', file=OUT )

    for cds_project_alias in sorted( cds_project_alias_to_gdc_project_alias ):
        
        cds_project_id = cds_project_alias_to_project_id[cds_project_alias]

        gdc_project_alias = cds_project_alias_to_gdc_project_alias[cds_project_alias]

        gdc_project_id = gdc_project_alias_to_project_id[gdc_project_alias]

        print( *[ cds_project_alias, cds_project_id, gdc_project_alias, gdc_project_id ], sep='\t', file=OUT )



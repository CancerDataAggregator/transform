#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://dataservice.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'cds' )

treatment_out_dir = path.join( output_root, 'treatment' )

treatment_output_tsv = path.join( treatment_out_dir, 'treatment.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

treatment_output_json = path.join( json_out_dir, 'treatment_metadata.json' )

# type treatment {
#     treatment_id: String
#     treatment_type: String
#     treatment_outcome: String
#     days_to_treatment: Int
#     therapeutic_agents: String
# }

scalar_treatment_fields = [
    'treatment_id',
    'treatment_type',
    'treatment_outcome',
    'days_to_treatment',
    'therapeutic_agents'
]

treatment_api_query_json_template = f'''    {{
        
        treatment( first: {page_size}, offset: __OFFSET__, orderBy: [ treatment_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_treatment_fields ) + f'''
        }}
    }}'''

# EXECUTION

for output_dir in [ treatment_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( treatment_output_json, 'w' ) as JSON, open( treatment_output_tsv, 'w' ) as TREATMENT:
    
    print( *scalar_treatment_fields, sep='\t', file=TREATMENT )

    while not null_result:
    
        treatment_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), treatment_api_query_json_template )
        }

        treatment_response = requests.post( api_url, json=treatment_api_query_json )

        if not treatment_response.ok:
            
            print( treatment_api_query_json['query'], file=sys.stderr )

            treatment_response.raise_for_status()

        treatment_result = json.loads( treatment_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( treatment_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        treatment_count = len( treatment_result['data']['treatment'] )

        if treatment_count == 0:
            
            null_result = True

        else:
            
            for treatment in treatment_result['data']['treatment']:
                
                treatment_id = treatment['treatment_id']

                treatment_row = list()

                for field_name in scalar_treatment_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( treatment[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    treatment_row.append( field_value )

                print( *treatment_row, sep='\t', file=TREATMENT )

        offset = offset + page_size



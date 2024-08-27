#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://caninecommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'icdc' )

biospecimen_source_out_dir = path.join( output_root, 'biospecimen_source' )
biospecimen_source_output_tsv = path.join( biospecimen_source_out_dir, 'biospecimen_source.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
biospecimen_source_output_json = path.join( json_out_dir, 'biospecimen_source_metadata.json' )

############################################################################################################################################################

# type biospecimen_source {
#     biospecimen_repository_acronym: String
#     biospecimen_repository_full_name: String
# }

scalar_biospecimen_source_fields = [
    'biospecimen_repository_acronym',
    'biospecimen_repository_full_name'
]

# biospecimen_source(biospecimen_repository_acronym: String, biospecimen_repository_full_name: String, filter: _biospecimen_sourceFilter, first: Int, offset: Int, orderBy: [_biospecimen_sourceOrdering!]): [biospecimen_source!]!

biospecimen_source_api_query_json_template = f'''    {{
        
        biospecimen_source( first: {page_size}, offset: __OFFSET__, orderBy: [ biospecimen_repository_acronym_asc ] ) {{
            ''' + '''
            '''.join( scalar_biospecimen_source_fields ) + f'''
        }}
    }}'''

# EXECUTION

for output_dir in [ biospecimen_source_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( biospecimen_source_output_json, 'w' ) as JSON, \
    open( biospecimen_source_output_tsv, 'w' ) as BIOSPECIMEN_SOURCE:

    print( *scalar_biospecimen_source_fields, sep='\t', file=BIOSPECIMEN_SOURCE )

    while not null_result:
    
        biospecimen_source_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), biospecimen_source_api_query_json_template )
        }

        biospecimen_source_response = requests.post( api_url, json=biospecimen_source_api_query_json )

        if not biospecimen_source_response.ok:
            
            print( biospecimen_source_api_query_json['query'], file=sys.stderr )

            biospecimen_source_response.raise_for_status()

        biospecimen_source_result = json.loads( biospecimen_source_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( biospecimen_source_result, indent=4, sort_keys=False ), file=JSON )

        biospecimen_source_count = len( biospecimen_source_result['data']['biospecimen_source'] )

        if biospecimen_source_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for biospecimen_source in biospecimen_source_result['data']['biospecimen_source']:
                
                biospecimen_source_row = list()

                for field_name in scalar_biospecimen_source_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( biospecimen_source[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    biospecimen_source_row.append( field_value )

                print( *biospecimen_source_row, sep='\t', file=BIOSPECIMEN_SOURCE )

        offset = offset + page_size



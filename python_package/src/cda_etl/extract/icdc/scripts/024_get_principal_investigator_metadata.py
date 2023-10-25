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

principal_investigator_out_dir = path.join( output_root, 'principal_investigator' )
principal_investigator_output_tsv = path.join( principal_investigator_out_dir, 'principal_investigator.tsv' )
principal_investigator_study_output_tsv = path.join( principal_investigator_out_dir, 'principal_investigator.clinical_study_designation.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
principal_investigator_output_json = path.join( json_out_dir, 'principal_investigator_metadata.json' )

# type principal_investigator {
#     pi_first_name: String
#     pi_last_name: String
#     pi_middle_initial: String
#     studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {
#         clinical_study_designation
#     }
# }

scalar_principal_investigator_fields = [
    'pi_first_name',
    'pi_last_name',
    'pi_middle_initial'
]

# principal_investigator(pi_first_name: String, pi_last_name: String, pi_middle_initial: String, filter: _principal_investigatorFilter, first: Int, offset: Int, orderBy: [_principal_investigatorOrdering!]): [principal_investigator!]!

principal_investigator_api_query_json_template = f'''    {{
        
        principal_investigator( first: {page_size}, offset: __OFFSET__, orderBy: [ pi_last_name_asc, pi_first_name_asc ] ) {{
            ''' + '''
            '''.join( scalar_principal_investigator_fields ) + f'''
            studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {{
                clinical_study_designation
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ principal_investigator_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( principal_investigator_output_json, 'w' ) as JSON, \
    open( principal_investigator_output_tsv, 'w' ) as PI, \
    open( principal_investigator_study_output_tsv, 'w' ) as PI_STUDY:

    print( *scalar_principal_investigator_fields, sep='\t', file=PI )
    print( *( scalar_principal_investigator_fields + [ 'clinical_study_designation' ] ), sep='\t', file=PI_STUDY )

    while not null_result:
    
        principal_investigator_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), principal_investigator_api_query_json_template )
        }

        principal_investigator_response = requests.post( api_url, json=principal_investigator_api_query_json )

        if not principal_investigator_response.ok:
            
            print( principal_investigator_api_query_json['query'], file=sys.stderr )

            principal_investigator_response.raise_for_status()

        principal_investigator_result = json.loads( principal_investigator_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( principal_investigator_result, indent=4, sort_keys=False ), file=JSON )

        principal_investigator_count = len( principal_investigator_result['data']['principal_investigator'] )

        if principal_investigator_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for principal_investigator in principal_investigator_result['data']['principal_investigator']:
                
                principal_investigator_row = list()

                for field_name in scalar_principal_investigator_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( principal_investigator[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    principal_investigator_row.append( field_value )

                print( *principal_investigator_row, sep='\t', file=PI )

                if 'studies' in principal_investigator and principal_investigator['studies'] is not None and len( principal_investigator['studies'] ) > 0:
                    
                    for study in principal_investigator['studies']:
                        
                        pi_study_row = list()

                        for field_name in scalar_principal_investigator_fields:
                            
                            field_value = json.dumps( principal_investigator[field_name] ).strip( '"' )

                            if field_value == 'null':
                                
                                field_value = ''

                            pi_study_row.append( field_value )

                        print( *( pi_study_row + [ study['clinical_study_designation'] ] ), sep='\t', file=PI_STUDY )

                    study_count = len( principal_investigator['studies'] )

                    if study_count == page_size:
                        
                        if principal_investigator['pi_middle_initial'] is not None:
                            
                            print( f"WARNING: principal_investigator \"{principal_investigator['pi_first_name']} {principal_investigator['pi_middle_initial']} {principal_investigator['pi_last_name']}\" has at least {page_size} study records: implement paging here or risk data loss.", file=sys.stderr )

                        else:
                            
                            print( f"WARNING: principal_investigator \"{principal_investigator['pi_first_name']} {principal_investigator['pi_last_name']}\" has at least {page_size} study records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size



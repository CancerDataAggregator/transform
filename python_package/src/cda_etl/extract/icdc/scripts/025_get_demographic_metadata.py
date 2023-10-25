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

demographic_out_dir = path.join( output_root, 'demographic' )
demographic_output_tsv = path.join( demographic_out_dir, 'demographic.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )
demographic_case_output_tsv = path.join( relationship_validation_out_dir, 'demographic.case_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
demographic_output_json = path.join( json_out_dir, 'demographic_metadata.json' )

# type demographic {
#     demographic_id: String
#     breed: String
#     additional_breed_detail: String
#     patient_age_at_enrollment: Float
#     patient_age_at_enrollment_unit: String
#     patient_age_at_enrollment_original: Float
#     patient_age_at_enrollment_original_unit: String
#     date_of_birth: String
#     sex: String
#     weight: Float
#     weight_unit: String
#     weight_original: Float
#     weight_original_unit: String
#     neutered_indicator: String
#     case {
#         # Just for validation.
#         case_id
#     }
# }

scalar_demographic_fields = [
    'demographic_id',
    'breed',
    'additional_breed_detail',
    'patient_age_at_enrollment',
    'patient_age_at_enrollment_unit',
    'patient_age_at_enrollment_original',
    'patient_age_at_enrollment_original_unit',
    'date_of_birth',
    'sex',
    'weight',
    'weight_unit',
    'weight_original',
    'weight_original_unit',
    'neutered_indicator'
]

# demographic(demographic_id: String, breed: String, additional_breed_detail: String, patient_age_at_enrollment: Float, patient_age_at_enrollment_unit: String, patient_age_at_enrollment_original: Float, patient_age_at_enrollment_original_unit: String, date_of_birth: String, sex: String, weight: Float, weight_unit: String, weight_original: Float, weight_original_unit: String, neutered_indicator: String, filter: _demographicFilter, first: Int, offset: Int, orderBy: [_demographicOrdering!]): [demographic!]!

demographic_api_query_json_template = f'''    {{
        
        demographic( first: {page_size}, offset: __OFFSET__, orderBy: [ demographic_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_demographic_fields ) + f'''
            case {{
                case_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ demographic_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( demographic_output_json, 'w' ) as JSON, \
    open( demographic_output_tsv, 'w' ) as DEMOGRAPHIC, \
    open( demographic_case_output_tsv, 'w' ) as DEMOGRAPHIC_CASE:

    print( *scalar_demographic_fields, sep='\t', file=DEMOGRAPHIC )
    print( *[ 'demographic_id', 'case_id' ], sep='\t', file=DEMOGRAPHIC_CASE )

    while not null_result:
    
        demographic_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), demographic_api_query_json_template )
        }

        demographic_response = requests.post( api_url, json=demographic_api_query_json )

        if not demographic_response.ok:
            
            print( demographic_api_query_json['query'], file=sys.stderr )

            demographic_response.raise_for_status()

        demographic_result = json.loads( demographic_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( demographic_result, indent=4, sort_keys=False ), file=JSON )

        demographic_count = len( demographic_result['data']['demographic'] )

        if demographic_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for demographic in demographic_result['data']['demographic']:
                
                demographic_id = ''

                # This shouldn't ever be null, but it sometimes is.

                if 'demographic_id' in demographic and demographic['demographic_id'] is not None:
                    
                    demographic_id = demographic['demographic_id']

                demographic_row = list()

                for field_name in scalar_demographic_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( demographic[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    demographic_row.append( field_value )

                print( *demographic_row, sep='\t', file=DEMOGRAPHIC )

                if 'case' in demographic and demographic['case'] is not None and demographic['case']['case_id'] is not None:
                    
                    print( *[ demographic_id, demographic['case']['case_id'] ], sep='\t', file=DEMOGRAPHIC_CASE )

        offset = offset + page_size



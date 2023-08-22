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

diagnosis_out_dir = path.join( output_root, 'diagnosis' )

diagnosis_output_tsv = path.join( diagnosis_out_dir, 'diagnosis.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

diagnosis_output_json = path.join( json_out_dir, 'diagnosis_metadata.json' )

# type diagnosis {
#     diagnosis_id: String
#     disease_type: String
#     vital_status: String
#     primary_diagnosis: String
#     primary_site: String
#     age_at_diagnosis: Int
#     tumor_grade: String
#     tumor_stage_clinical_m: String
#     tumor_stage_clinical_n: String
#     tumor_stage_clinical_t: String
#     morphology: String
#     incidence_type: String
#     progression_or_recurrence: String
#     days_to_recurrence: Int
#     days_to_last_followup: Int
#     last_known_disease_status: String
#     days_to_last_known_status: Int
#     participant: participant
# }

scalar_diagnosis_fields = [
    'diagnosis_id',
    'disease_type',
    'vital_status',
    'primary_diagnosis',
    'primary_site',
    'age_at_diagnosis',
    'tumor_grade',
    'tumor_stage_clinical_m',
    'tumor_stage_clinical_n',
    'tumor_stage_clinical_t',
    'morphology',
    'incidence_type',
    'progression_or_recurrence',
    'days_to_recurrence',
    'days_to_last_followup',
    'last_known_disease_status',
    'days_to_last_known_status'
]

diagnosis_api_query_json_template = f'''    {{
        
        diagnosis( first: {page_size}, offset: __OFFSET__, orderBy: [ diagnosis_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_diagnosis_fields ) + f'''
            participant {{
                study {{
                    study_name
                }}
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ diagnosis_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( diagnosis_output_json, 'w' ) as JSON, open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS:
    
    diagnosis_fields = [ 'study_name' ] + scalar_diagnosis_fields

    print( *diagnosis_fields, sep='\t', file=DIAGNOSIS )

    while not null_result:
    
        diagnosis_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), diagnosis_api_query_json_template )
        }

        diagnosis_response = requests.post( api_url, json=diagnosis_api_query_json )

        if not diagnosis_response.ok:
            
            print( diagnosis_api_query_json['query'], file=sys.stderr )

            diagnosis_response.raise_for_status()

        diagnosis_result = json.loads( diagnosis_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( diagnosis_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        diagnosis_count = len( diagnosis_result['data']['diagnosis'] )

        if diagnosis_count == 0:
            
            null_result = True

        else:
            
            for diagnosis in diagnosis_result['data']['diagnosis']:
                
                diagnosis_id = diagnosis['diagnosis_id']

                # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.
                # 
                # Also, we want this to break with a key error if it doesn't exist.

                study_name = json.dumps( diagnosis['participant']['study']['study_name'] ).strip( '"' )

                diagnosis_row = [ study_name ]

                for field_name in scalar_diagnosis_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( diagnosis[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    diagnosis_row.append( field_value )

                print( *diagnosis_row, sep='\t', file=DIAGNOSIS )

        offset = offset + page_size



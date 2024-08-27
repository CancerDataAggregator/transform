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

diagnosis_out_dir = path.join( output_root, 'diagnosis' )
diagnosis_output_tsv = path.join( diagnosis_out_dir, 'diagnosis.tsv' )

diagnosis_case_output_tsv = path.join( diagnosis_out_dir, 'diagnosis.case_id.tsv' )
diagnosis_file_output_tsv = path.join( diagnosis_out_dir, 'diagnosis.file_uuid.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
diagnosis_output_json = path.join( json_out_dir, 'diagnosis_metadata.json' )

# type diagnosis {
#     diagnosis_id: String
#     disease_term: String
#     primary_disease_site: String
#     stage_of_disease: String
#     date_of_diagnosis: String
#     histology_cytopathology: String
#     date_of_histology_confirmation: String
#     histological_grade: String
#     best_response: String
#     pathology_report: String
#     treatment_data: String
#     follow_up_data: String
#     concurrent_disease: String
#     concurrent_disease_type: String
#     case {
#         # Just for validation
#         case_id
#     }
#     files( first: {page_size}, offset: 0, orderBy: [ uuid_asc ] ) {
#         # Just for validation
#         uuid
#     }
# }

scalar_diagnosis_fields = [
    'diagnosis_id',
    'disease_term',
    'primary_disease_site',
    'stage_of_disease',
    'date_of_diagnosis',
    'histology_cytopathology',
    'date_of_histology_confirmation',
    'histological_grade',
    'best_response',
    'pathology_report',
    'treatment_data',
    'follow_up_data',
    'concurrent_disease',
    'concurrent_disease_type'
]

# diagnosis(diagnosis_id: String, disease_term: String, primary_disease_site: String, stage_of_disease: String, date_of_diagnosis: String, histology_cytopathology: String, date_of_histology_confirmation: String, histological_grade: String, best_response: String, pathology_report: String, treatment_data: String, follow_up_data: String, concurrent_disease: String, concurrent_disease_type: String, filter: _diagnosisFilter, first: Int, offset: Int, orderBy: [_diagnosisOrdering!]): [diagnosis!]!

diagnosis_api_query_json_template = f'''    {{
        
        diagnosis( first: {page_size}, offset: __OFFSET__, orderBy: [ diagnosis_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_diagnosis_fields ) + f'''
            case {{
                case_id
            }}
            files( first: {page_size}, offset: 0, orderBy: [ uuid_asc ] ) {{
                uuid
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ diagnosis_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( diagnosis_output_json, 'w' ) as JSON, \
    open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS, \
    open( diagnosis_case_output_tsv, 'w' ) as DIAGNOSIS_CASE, \
    open( diagnosis_file_output_tsv, 'w' ) as DIAGNOSIS_FILE:

    print( *scalar_diagnosis_fields, sep='\t', file=DIAGNOSIS )
    print( *[ 'diagnosis_id', 'case_id' ], sep='\t', file=DIAGNOSIS_CASE )
    print( *[ 'diagnosis_id', 'file.uuid' ], sep='\t', file=DIAGNOSIS_FILE )

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

        diagnosis_count = len( diagnosis_result['data']['diagnosis'] )

        if diagnosis_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for diagnosis in diagnosis_result['data']['diagnosis']:
                
                diagnosis_id = diagnosis['diagnosis_id']

                diagnosis_row = list()

                for field_name in scalar_diagnosis_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( diagnosis[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    diagnosis_row.append( field_value )

                print( *diagnosis_row, sep='\t', file=DIAGNOSIS )

                if 'case' in diagnosis and diagnosis['case'] is not None and diagnosis['case']['case_id'] is not None:
                    
                    print( *[ diagnosis_id, diagnosis['case']['case_id'] ], sep='\t', file=DIAGNOSIS_CASE )

                for file in diagnosis['files']:
                    
                    print( *[ diagnosis_id, file['uuid'] ], sep='\t', file=DIAGNOSIS_FILE )

                file_count = len( diagnosis['files'] )

                if file_count == page_size:
                    
                    print( f"WARNING: Diagnosis {diagnosis_id} has at least {page_size} files: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size



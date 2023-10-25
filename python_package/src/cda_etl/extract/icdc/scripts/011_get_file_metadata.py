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

file_out_dir = path.join( output_root, 'file' )
file_output_tsv = path.join( file_out_dir, 'file.tsv' )
file_study_output_tsv = path.join( file_out_dir, 'file.clinical_study_designation.tsv' )
file_case_output_tsv = path.join( file_out_dir, 'file.case_id.tsv' )
file_diagnosis_output_tsv = path.join( file_out_dir, 'file.diagnosis_id.tsv' )
file_sample_output_tsv = path.join( file_out_dir, 'file.sample_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
file_output_json = path.join( json_out_dir, 'file_metadata.json' )

# type file {
#     file_name: String
#     file_type: String
#     file_description: String
#     file_format: String
#     file_size: Float
#     md5sum: String
#     file_status: String
#     uuid: String
#     file_location: String
#     case {
#         # Just for validation
#         case_id
#     }
#     study {
#         clinical_study_designation
#     }
#     sample {
#         # Just one?
#         sample_id
#     }
#     assay {
#         [nothing]
#     }
#     diagnosis {
#         # Just one?
#         diagnosis_id
#     }
# }

scalar_file_fields = [
    'uuid',
    'file_name',
    'file_type',
    'file_description',
    'file_format',
    'file_size',
    'md5sum',
    'file_status',
    'file_location'
]

# file(file_name: String, file_type: String, file_description: String, file_format: String, file_size: Float, md5sum: String, file_status: String, uuid: String, file_location: String, filter: _fileFilter, first: Int, offset: Int, orderBy: [_fileOrdering!]): [file!]!

file_api_query_json_template = f'''    {{
        
        file( first: {page_size}, offset: __OFFSET__, orderBy: [ uuid_asc ] ) {{
            ''' + '''
            '''.join( scalar_file_fields ) + f'''
            study {{
                clinical_study_designation
            }}
            case {{
                case_id
            }}
            diagnosis {{
                diagnosis_id
            }}
            sample {{
                sample_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ file_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( file_output_json, 'w' ) as JSON, \
    open( file_output_tsv, 'w' ) as FILE, \
    open( file_study_output_tsv, 'w' ) as FILE_STUDY, \
    open( file_case_output_tsv, 'w' ) as FILE_CASE, \
    open( file_diagnosis_output_tsv, 'w' ) as FILE_DIAGNOSIS, \
    open( file_sample_output_tsv, 'w' ) as FILE_SAMPLE:

    print( *scalar_file_fields, sep='\t', file=FILE )
    print( *[ 'file.uuid', 'clinical_study_designation' ], sep='\t', file=FILE_STUDY )
    print( *[ 'file.uuid', 'case_id' ], sep='\t', file=FILE_CASE )
    print( *[ 'file.uuid', 'diagnosis_id' ], sep='\t', file=FILE_DIAGNOSIS )
    print( *[ 'file.uuid', 'sample_id' ], sep='\t', file=FILE_SAMPLE )

    while not null_result:
    
        file_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), file_api_query_json_template )
        }

        file_response = requests.post( api_url, json=file_api_query_json )

        if not file_response.ok:
            
            print( file_api_query_json['query'], file=sys.stderr )

            file_response.raise_for_status()

        file_result = json.loads( file_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( file_result, indent=4, sort_keys=False ), file=JSON )

        file_count = len( file_result['data']['file'] )

        if file_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for file in file_result['data']['file']:
                
                file_uuid = file['uuid']

                file_row = list()

                for field_name in scalar_file_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( file[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    file_row.append( field_value )

                print( *file_row, sep='\t', file=FILE )

                if 'study' in file and file['study'] is not None and file['study']['clinical_study_designation'] is not None:
                    
                    print( *[ file_uuid, json.dumps( file['study']['clinical_study_designation'] ).strip( '"' ) ], sep='\t', file=FILE_STUDY )

                if 'case' in file and file['case'] is not None and file['case']['case_id'] is not None:
                    
                    print( *[ file_uuid, file['case']['case_id'] ], sep='\t', file=FILE_CASE )

                if 'diagnosis' in file and file['diagnosis'] is not None and file['diagnosis']['diagnosis_id'] is not None:
                    
                    print( *[ file_uuid, file['diagnosis']['diagnosis_id'] ], sep='\t', file=FILE_DIAGNOSIS )

                if 'sample' in file and file['sample'] is not None and file['sample']['sample_id'] is not None:
                    
                    print( *[ file_uuid, file['sample']['sample_id'] ], sep='\t', file=FILE_SAMPLE )

        offset = offset + page_size



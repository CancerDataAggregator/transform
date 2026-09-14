#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import map_columns_one_to_one, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_list_type_tsv = path.join( study_out_dir, 'Study.list_type.from_StudyDataFileByStudyShortName.tsv' )
study_study_data_files_data_file_uuid_tsv = path.join( study_out_dir, 'Study.data_file_uuid.from_StudyDataFileByStudyShortName.study_data_files.tsv' )
study_data_files_data_file_uuid_tsv = path.join( study_out_dir, 'Study.data_file_uuid.from_StudyDataFileByStudyShortName.data_files.tsv' )

study_short_name_to_study_id = map_columns_one_to_one( study_tsv, 'study_short_name', 'study_id' )

data_file_out_dir = path.join( output_root, 'DataFile' )
data_file_study_data_files_tsv = path.join( data_file_out_dir, 'DataFile.from_StudyDataFileByStudyShortName.study_data_files.tsv' )
data_file_data_files_tsv = path.join( data_file_out_dir, 'DataFile.from_StudyDataFileByStudyShortName.data_files.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
StudyDataFileByStudyShortName_json_output_file = path.join( json_out_dir, 'StudyDataFileByStudyShortName.json' )

# What we actually receive:
# 
# type StudyDataFile {
#   study_short_name: String
#   study_accession: String
#   list_type: [String]
#   study_data_files: [DataFile]
#   data_files: [DataFile]
# }

# Non-scalar DataFile fields:
#     <none>
scalar_data_file_fields = [
    'data_file_uuid',
    'data_file_name',
    'data_file_type',
    'data_file_description',
    'data_file_format',
    'data_file_size',
    'data_file_checksum_value',
    'data_file_checksum_type',
    'data_file_compression_status',
    'data_file_location'
]

StudyDataFileByStudyShortName_query = '''
query search {
    StudyDataFileByStudyShortName ( study_short_name: "$STUDY_SHORT_NAME$" ) {
        study_short_name
        study_accession
        list_type
        study_data_files {
            ''' + '\n            '.join( scalar_data_file_fields ) + '''
        }
        data_files {
            ''' + '\n            '.join( scalar_data_file_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, study_out_dir, data_file_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Open handles for output files to save TSVs describing returned data.
# 
# We can't always safely use the Python `with` keyword in cases like this because the Python interpreter
# enforces an arbitrary hard-coded limit on the total number of simultaneous indents, and each `with` keyword
# creates another indent, even if you use the
# 
#     with open(A) as A, open(B) as B, ...
# 
# (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
# have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
# indentation, especially when the syntax above should've avoided creating multiple nested indents in the first place.

output_tsv_keywords = [
    'DATA_FILE_FROM_STUDY_DATA_FILES',
    'DATA_FILE_FROM_DATA_FILES',
    'STUDY_LIST_TYPE',
    'STUDY_STUDY_DATA_FILE',
    'STUDY_DATA_FILE'
]

output_tsv_filenames = [
    data_file_study_data_files_tsv,
    data_file_data_files_tsv,
    study_list_type_tsv,
    study_study_data_files_data_file_uuid_tsv,
    study_data_files_data_file_uuid_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_data_file_fields, sep='\t', end='\n', file=output_tsvs['DATA_FILE_FROM_STUDY_DATA_FILES'] )
print( *scalar_data_file_fields, sep='\t', end='\n', file=output_tsvs['DATA_FILE_FROM_DATA_FILES'] )
print( *[ 'study_id', 'list_type' ], sep='\t', end='\n', file=output_tsvs['STUDY_LIST_TYPE'] )
print( *[ 'study_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['STUDY_STUDY_DATA_FILE'] )
print( *[ 'study_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['STUDY_DATA_FILE'] )

with open( StudyDataFileByStudyShortName_json_output_file, 'w' ) as JSON:
    
    for study_short_name in sorted( study_short_name_to_study_id ):
        # Send the StudyDataFileByStudyShortName() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, StudyDataFileByStudyShortName_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, StudyDataFileByStudyShortName_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for study_data_file in result['data']['StudyDataFileByStudyShortName']:
            study_id = study_short_name_to_study_id[study_data_file['study_short_name']]

            # StudyDataFile.list_type [array of strings]. Associate with the Study record referenced in this StudyDataFile object.
            if 'list_type' in study_data_file and len( study_data_file['list_type'] ) > 0:
                for list_type in study_data_file['list_type']:
                    print( *[ study_id, list_type ], sep='\t', end='\n', file=output_tsvs['STUDY_LIST_TYPE'] )
            for data_file in study_data_file['study_data_files']:
                # This can happen.
                if 'data_file_uuid' in data_file and data_file['data_file_uuid'] is not None and data_file['data_file_uuid'] != '':
                    data_file_row = list()
                    for field_name in scalar_data_file_fields:
                        if data_file[field_name] is not None:
                            data_file_row.append( data_file[field_name] )
                        else:
                            data_file_row.append( '' )
                    print( *data_file_row, sep='\t', end='\n', file=output_tsvs['DATA_FILE_FROM_STUDY_DATA_FILES'] )
                    # Associate this study with this DataFile record. ID access attempts should fail with a KeyError if the needed ID isn't present.
                    print( *[ study_id, data_file['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['STUDY_STUDY_DATA_FILE'] )

            for data_file in study_data_file['data_files']:
                # This can happen.
                if 'data_file_uuid' in data_file and data_file['data_file_uuid'] is not None and data_file['data_file_uuid'] != '':
                    data_file_row = list()
                    for field_name in scalar_data_file_fields:
                        if data_file[field_name] is not None:
                            data_file_row.append( data_file[field_name] )
                        else:
                            data_file_row.append( '' )
                    print( *data_file_row, sep='\t', end='\n', file=output_tsvs['DATA_FILE_FROM_DATA_FILES'] )
                    # Associate this study with this DataFile record. ID access attempts should fail with a KeyError if the needed ID isn't present.
                    print( *[ study_id, data_file['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['STUDY_DATA_FILE'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )



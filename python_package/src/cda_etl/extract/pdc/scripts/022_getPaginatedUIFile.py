#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

uifile_out_dir = f"{output_root}/UIFile"

uifile_tsv = f"{uifile_out_dir}/UIFile.tsv"

scalar_uifile_fields = (

    'file_id',
    'file_name',
    'file_type',
    'data_category',
    'file_size',
    'md5sum',
    'downloadable',
    'embargo_date',
    'data_source',
    'access',
    'study_id',
    'submitter_id_name',
    'pdc_study_id',
    'study_run_metadata_submitter_id',
    'project_name'
)

offset = 0

offset_increment = 25000

returned_nothing = False

# EXECUTION

for output_dir in ( json_out_dir, uifile_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Open handles for output files to save TSVs describing the returned objects and (as needed) association TSVs
# enumerating containment relationships between objects and sub-objects as well as association TSVs enumerating
# relationships between objects and keyword-style dictionaries.
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

### OLD COMMENT: (none at this level)

output_tsv_keywords = [
    'UIFILE'
]

output_tsv_filenames = [
    uifile_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table header.

print( *scalar_uifile_fields, sep='\t', end='\n', file=output_tsvs['UIFILE'] )

while not returned_nothing:
    
    api_query_json = {
        'query': '''            {
            getPaginatedUIFile( ''' + f"offset: {offset}, limit: {offset_increment}" + ''' ) {
                uiFiles {
                    ''' + '\n                        '.join(scalar_uifile_fields) + '''
                }
            }
        }'''
    }

    # Send the getPaginatedUIFile() query to the API server.

    response = requests.post(api_url, json=api_query_json)

    # If the HTTP response code is not OK (200), dump the query, print the http
    # error result and exit.

    if not response.ok:
        
        print( api_query_json['query'], file=sys.stderr )

        response.raise_for_status()

    # Retrieve the server's JSON response as a Python object.

    result = json.loads(response.content)

    if result['data']['getPaginatedUIFile']['uiFiles'] is None or len(result['data']['getPaginatedUIFile']['uiFiles']) == 0:
        
        returned_nothing = True

    else:
        
        # Save a version of the returned data as JSON.

        upper_limit = offset + (len(result['data']['getPaginatedUIFile']['uiFiles']) - 1)

        getPaginatedUIFile_json_output_file = f"{json_out_dir}/getPaginatedUIFile.{offset:06}-{upper_limit:06}.json"

        with open(getPaginatedUIFile_json_output_file, 'w') as JSON:
            
            print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for uifile in result['data']['getPaginatedUIFile']['uiFiles']:
            
            uifile_row = list()

            for field_name in scalar_uifile_fields:
                
                if uifile[field_name] is not None:
                    
                    uifile_row.append(uifile[field_name])

                else:
                    
                    uifile_row.append('')

            print( *uifile_row, sep='\t', end='\n', file=output_tsvs['UIFILE'] )

        # Increment the paging offset in advance of the next query iteration.

        offset = offset + offset_increment

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



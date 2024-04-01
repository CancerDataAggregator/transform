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

file_out_dir = f"{output_root}/File"

file_tsv = f"{file_out_dir}/File.tsv"
file_studies_tsv = f"{file_out_dir}/File.studies.tsv"
file_study_run_metadata_tsv = f"{file_out_dir}/File.study_run_metadata.tsv"

getPaginatedFiles_json_output_file = f"{json_out_dir}/getPaginatedFiles.json"

scalar_file_fields = (
    'file_id',
    'file_name',
    'file_type',
    'data_category',
    'file_size',
    'md5sum',
    'downloadable',
    'file_location',
    'files_count',
    'study_id',
    'study_submitter_id',
    'pdc_study_id'
)

api_query_json = {
    'query': '''    {
        getPaginatedFiles( offset: 0, limit: 150000, acceptDUA: true ) {
            files {
                ''' + '\n                '.join(scalar_file_fields) + '''
                studies {
                    study_id
                }
                study_run_metadata {
                    study_run_metadata_id
                }
            }
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, file_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the getPaginatedFiles() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(getPaginatedFiles_json_output_file, 'w') as JSON:
    
    print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

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

### OLD COMMENT: Also save a version of the returned File objects in TSV form, plus associations
### with Study and StudyRunMetadata objects.

output_tsv_keywords = [
    'FILE',
    'FILE_STUDIES',
    'FILE_STUDY_RUN_METADATA'
]

output_tsv_filenames = [
    file_tsv,
    file_studies_tsv,
    file_study_run_metadata_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_file_fields, sep='\t', end='\n', file=output_tsvs['FILE'] )

print( *('file_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['FILE_STUDIES'] )
print( *('file_id', 'study_run_metadata_id'), sep='\t', end='\n', file=output_tsvs['FILE_STUDY_RUN_METADATA'] )

# Parse the returned data and save to TSV.

for file in result['data']['getPaginatedFiles']['files']:
    
    file_row = list()

    for field_name in scalar_file_fields:
        
        if file[field_name] is not None:
            
            file_row.append(file[field_name])

        else:
            
            file_row.append('')

    print( *file_row, sep='\t', end='\n', file=output_tsvs['FILE'] )

    if file['studies'] is not None and len(file['studies']) > 0:
        
        for study in file['studies']:
            
            print( *(file['file_id'], study['study_id']), sep='\t', end='\n', file=output_tsvs['FILE_STUDIES'] )

    if file['study_run_metadata'] is not None and len(file['study_run_metadata']) > 0:
        
        for study_run_metadata in file['study_run_metadata']:
            
            print( *(file['file_id'], study_run_metadata['study_run_metadata_id']), sep='\t', end='\n', file=output_tsvs['FILE_STUDY_RUN_METADATA'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



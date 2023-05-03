#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

# SUBROUTINES

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

aliquot_out_dir = f"{output_root}/Aliquot"

aliquot_tsv = f"{aliquot_out_dir}/Aliquot.tsv"
aliquot_aliquot_run_metadata_tsv = f"{aliquot_out_dir}/Aliquot.aliquot_run_metadata.tsv"
aliquot_sample_tsv = f"{aliquot_out_dir}/Aliquot.sample.tsv"
aliquot_project_tsv = f"{aliquot_out_dir}/Aliquot.project.tsv"

# We pull the following from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store it in the Aliquot directory
# to indicate that's where this version came from. This is half-normalization; will
# merge later during aggregation across (in this case) StudyRunMetadata and AliquotRunMetadata.

aliquot_run_metadata_tsv = f"{aliquot_out_dir}/AliquotRunMetadata.tsv"

aliquot_json_output_file = f"{json_out_dir}/aliquot.json"

scalar_aliquot_fields = (
    'aliquot_id',
    'aliquot_submitter_id',
    'status',
    'aliquot_is_ref',
    'pool',
    'aliquot_quantity',
    'aliquot_volume',
    'amount',
    'analyte_type',
    'concentration',
    'sample_id',
    'sample_submitter_id',
    'case_id',
    'case_submitter_id'
)

scalar_aliquot_run_metadata_fields = (
    'aliquot_run_metadata_id',
    'aliquot_submitter_id',
    'label',
    'experiment_number',
    'fraction',
    'replicate_number',
    'date',
    'alias',
    'analyte',
    'aliquot_id'
)

api_query_json = {
    'query': '''    {
        aliquot( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_aliquot_fields) + '''
            aliquot_run_metadata {
                ''' + '\n                '.join(scalar_aliquot_run_metadata_fields) + '''
            }
            sample {
                sample_id
            }
            project {
                project_id
            }
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, aliquot_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the aliquot() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(aliquot_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save a version of the returned Aliquot objects in TSV form, along with AliquotRunMetadata
### sub-records and relevant associations.

output_tsv_keywords = [
    'ALIQUOT',
    'ALIQUOT_ARM',
    'ALIQUOT_SAMPLE',
    'ALIQUOT_PROJECT',
    'ARM'
]

output_tsv_filenames = [
    aliquot_tsv,
    aliquot_aliquot_run_metadata_tsv,
    aliquot_sample_tsv,
    aliquot_project_tsv,
    aliquot_run_metadata_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_aliquot_fields, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )
print( *scalar_aliquot_run_metadata_fields, sep='\t', end='\n', file=output_tsvs['ARM'] )

print( *['aliquot_id', 'aliquot_run_metadata_id'], sep='\t', end='\n', file=output_tsvs['ALIQUOT_ARM'] )
print( *['aliquot_id', 'sample_id'], sep='\t', end='\n', file=output_tsvs['ALIQUOT_SAMPLE'] )
print( *['aliquot_id', 'project_id'], sep='\t', end='\n', file=output_tsvs['ALIQUOT_PROJECT'] )

# Parse the returned data and save to TSV.

for aliquot in result['data']['aliquot']:
    
    aliquot_row = list()

    for field_name in scalar_aliquot_fields:
        
        if aliquot[field_name] is not None:
            
            aliquot_row.append(aliquot[field_name])

        else:
            
            aliquot_row.append('')

    print( *aliquot_row, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )

    if aliquot['sample'] is not None:
        
        print( *(aliquot['aliquot_id'], aliquot['sample']['sample_id']), sep='\t', end='\n', file=output_tsvs['ALIQUOT_SAMPLE'] )

    if aliquot['project'] is not None:
        
        print( *(aliquot['aliquot_id'], aliquot['project']['project_id']), sep='\t', end='\n', file=output_tsvs['ALIQUOT_PROJECT'] )

    if aliquot['aliquot_run_metadata'] is not None:
        
        for aliquot_run_metadata in aliquot['aliquot_run_metadata']:
            
            aliquot_run_metadata_row = list()

            for field_name in scalar_aliquot_run_metadata_fields:
                
                if aliquot_run_metadata[field_name] is not None:
                    
                    aliquot_run_metadata_row.append(aliquot_run_metadata[field_name])

                else:
                    
                    aliquot_run_metadata_row.append('')

            print( *aliquot_run_metadata_row, sep='\t', end='\n', file=output_tsvs['ARM'] )

            print( *(aliquot['aliquot_id'], aliquot_run_metadata['aliquot_run_metadata_id']), sep='\t', end='\n', file=output_tsvs['ALIQUOT_ARM'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



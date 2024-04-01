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

experiment_project_out_dir = f"{output_root}/ExperimentProject"

experiment_project_tsv = f"{experiment_project_out_dir}/ExperimentProject.tsv"

projectsPerExperimentType_json_output_file = f"{json_out_dir}/projectsPerExperimentType.json"

scalar_experiment_project_fields = (
    'project_submitter_id',
    'name',
    'experiment_type'
)

api_query_json = {
    'query': '''    {
        projectsPerExperimentType( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_experiment_project_fields) + '''
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, experiment_project_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the projectsPerExperimentType() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(projectsPerExperimentType_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save the returned ExperimentProject associations in TSV form.

output_tsv_keywords = [
    'EXPERIMENT_PROJECT'
]

output_tsv_filenames = [
    experiment_project_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_experiment_project_fields, sep='\t', end='\n', file=output_tsvs['EXPERIMENT_PROJECT'] )

# Parse the returned data and save to TSV.

for experiment_project in result['data']['projectsPerExperimentType']:
    
    experiment_project_row = list()

    for field_name in scalar_experiment_project_fields:
        
        if experiment_project[field_name] is not None:
            
            experiment_project_row.append(experiment_project[field_name])

        else:
            
            experiment_project_row.append('')

    print( *experiment_project_row, sep='\t', end='\n', file=output_tsvs['EXPERIMENT_PROJECT'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



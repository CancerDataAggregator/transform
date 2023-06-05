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

ui_heatmap_study_out_dir = f"{output_root}/UIHeatmapStudy"

ui_heatmap_study_heatmap_files_tsv = f"{ui_heatmap_study_out_dir}/UIHeatmapStudy.heatmapFiles.tsv"

uiHeatmapStudies_json_output_file = f"{json_out_dir}/uiHeatmapStudies.json"

scalar_ui_heatmap_study_fields = (
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'submitter_id_name',
    'study_description',
    'embargo_date',
    'disease_type',
    'primary_site',
    'analytical_fraction',
    'experiment_type',
    'program_name',
    'project_name'
)

api_query_json = {
    'query': '''    {
        uiHeatmapStudies {
            ''' + '\n            '.join(scalar_ui_heatmap_study_fields) + '''
            heatmapFiles
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, ui_heatmap_study_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the uiHeatmapStudies() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(uiHeatmapStudies_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save the returned UIHeatmapStudy.heatmapFiles association metadata in TSV form.

output_tsv_keywords = [
    'UI_HEATMAP_STUDY_HEATMAP_FILES'
]

output_tsv_filenames = [
    ui_heatmap_study_heatmap_files_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table header.

print( *('study_id', 'file_name'), sep='\t', end='\n', file=output_tsvs['UI_HEATMAP_STUDY_HEATMAP_FILES'] )

# Parse the returned data and save to TSV.

for ui_heatmap_study in result['data']['uiHeatmapStudies']:
    
    if ui_heatmap_study['heatmapFiles'] is not None and len(ui_heatmap_study['heatmapFiles']) > 0:
        
        for file_name in ui_heatmap_study['heatmapFiles']:
            
            print( *(ui_heatmap_study['study_id'], file_name), sep='\t', end='\n', file=output_tsvs['UI_HEATMAP_STUDY_HEATMAP_FILES'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



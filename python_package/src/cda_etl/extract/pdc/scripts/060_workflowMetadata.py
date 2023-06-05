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

workflow_metadata_out_dir = f"{output_root}/WorkflowMetadata"

workflow_metadata_tsv = f"{workflow_metadata_out_dir}/WorkflowMetadata.tsv"

workflowMetadata_json_output_file = f"{json_out_dir}/workflowMetadata.json"

scalar_workflow_metadata_fields = (
    'workflow_metadata_id',
    'workflow_metadata_submitter_id',
    'submitter_id_name',
    'analytical_fraction',
    'experiment_type',
    'instrument',
    'phosphosite_localization',
    'hgnc_version',
    'gene_to_prot',
    'raw_data_processing',
    'raw_data_conversion',
    'sequence_database_search',
    'search_database_parameters',
    'ms1_data_analysis',
    'psm_report_generation',
    'cdap_reports',
    'mzidentml_refseq',
    'refseq_database_version',
    'mzidentml_uniprot',
    'uniport_database_version',
    'cptac_dcc_mzidentml',
    'cptac_galaxy_workflows',
    'cptac_galaxy_tools',
    'cptac_dcc_tools',
    'protocol_id',
    'protocol_submitter_id',
    'study_id',
    'study_submitter_id',
    'study_submitter_name',
    'pdc_study_id',
    'cptac_study_id'
)

api_query_json = {
    'query': '''    {
        workflowMetadata( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_workflow_metadata_fields) + '''
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, workflow_metadata_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the workflowMetadata() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(workflowMetadata_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save a version of the returned WorkflowMetadata objects in TSV form.

output_tsv_keywords = [
    'WORKFLOW_METADATA'
]

output_tsv_filenames = [
    workflow_metadata_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table header.

print( *scalar_workflow_metadata_fields, sep='\t', end='\n', file=output_tsvs['WORKFLOW_METADATA'] )

# Parse the returned data and save to TSV.

for workflow_metadata in result['data']['workflowMetadata']:
    
    workflow_metadata_row = list()

    for field_name in scalar_workflow_metadata_fields:
        
        if workflow_metadata[field_name] is not None:
            
            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

            workflow_metadata_row.append(json.dumps(workflow_metadata[field_name]).strip('"'))

        else:
            
            workflow_metadata_row.append('')

    print( *workflow_metadata_row, sep='\t', end='\n', file=output_tsvs['WORKFLOW_METADATA'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



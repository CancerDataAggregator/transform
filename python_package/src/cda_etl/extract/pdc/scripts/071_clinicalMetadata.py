#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import get_unique_values_from_tsv_column, sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

clinical_metadata_out_dir = f"{output_root}/ClinicalMetadata"

clinical_metadata_tsv = f"{clinical_metadata_out_dir}/ClinicalMetadata.tsv"
clinical_metadata_study_tsv = f"{clinical_metadata_out_dir}/ClinicalMetadata.Study.tsv"

clinicalMetadata_json_output_file = f"{json_out_dir}/clinicalMetadata.json"

scalar_clinical_metadata_fields = (
    'aliquot_id',
    'aliquot_submitter_id',
    'morphology',
    'primary_diagnosis',
    'tumor_grade',
    'tumor_stage',
    'tumor_largest_dimension_diameter'
)

pdc_study_ids = get_unique_values_from_tsv_column( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, clinical_metadata_out_dir ):
    
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
    'CLINICAL_METADATA',
    'CLINICAL_METADATA_STUDY'
]

output_tsv_filenames = [
    clinical_metadata_tsv,
    clinical_metadata_study_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open(clinicalMetadata_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_clinical_metadata_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_METADATA'] )

    print( *('aliquot_id', 'pdc_study_id'), sep='\t', end='\n', file=output_tsvs['CLINICAL_METADATA_STUDY'] )

    # Some ClinicalMetadata records (Aliquot clinical metadata, really) are in multiple Studies. Don't duplicate rows in the ClinicalMetadata table.

    seen_aliquot_IDs = set()

    # It's actually worse than that: some ClinicalMetadata records are repeatedly returned within _the_same_ Study. (Filtering on study_id produces the same result as for pdc_study_id.)

    seen_aliquot_IDs_by_study = dict()

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                clinicalMetadata( ''' + f'pdc_study_id: "{pdc_study_id}", acceptDUA: true' + ''' ) {
                    ''' + '\n                    '.join(scalar_clinical_metadata_fields) + '''
                }
            }'''
        }

        # Send the clinicalMetadata() query to the API server.

        response = requests.post(api_url, json=api_query_json)

        # If the HTTP response code is not OK (200), dump the query, print the http
        # error result and exit.

        if not response.ok:
            
            print( api_query_json['query'], file=sys.stderr )

            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.

        result = json.loads(response.content)

        # Save a version of the returned data as JSON (caching the PDC Study ID ahead of each block).

        print( pdc_study_id, file=JSON ) 

        print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for clinical_metadata in result['data']['clinicalMetadata']:
            
            if clinical_metadata['aliquot_id'] not in seen_aliquot_IDs:
                
                # Only make a row for this (Aliquot) ClinicalMetadata record if we haven't seen it before.

                seen_aliquot_IDs.add(clinical_metadata['aliquot_id'])

                clinical_metadata_row = list()

                for field_name in scalar_clinical_metadata_fields:
                    
                    if clinical_metadata[field_name] is not None:
                        
                        clinical_metadata_row.append(clinical_metadata[field_name])

                    else:
                        
                        clinical_metadata_row.append('')

                print( *clinical_metadata_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_METADATA'] )

            # Associate each (Aliquot) ClinicalMetadata record with each of its containing Studies exactly once (despite the fact that
            # this query returns multiple identical ClinicalMetadata records within the same Study).

            if pdc_study_id not in seen_aliquot_IDs_by_study or clinical_metadata['aliquot_id'] not in seen_aliquot_IDs_by_study[pdc_study_id]:
                
                if pdc_study_id not in seen_aliquot_IDs_by_study:
                    
                    seen_aliquot_IDs_by_study[pdc_study_id] = set()

                seen_aliquot_IDs_by_study[pdc_study_id].add(clinical_metadata['aliquot_id'])

                print( *(clinical_metadata['aliquot_id'], pdc_study_id), sep='\t', end='\n', file=output_tsvs['CLINICAL_METADATA_STUDY'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



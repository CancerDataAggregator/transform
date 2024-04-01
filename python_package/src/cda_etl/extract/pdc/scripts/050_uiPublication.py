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

publication_out_dir = f"{output_root}/Publication"

publication_tsv = f"{publication_out_dir}/Publication.tsv"
publication_studies_tsv = f"{publication_out_dir}/Publication.studies.tsv"
publication_disease_types_tsv = f"{publication_out_dir}/Publication.disease_types.tsv"
publication_supplementary_data_tsv = f"{publication_out_dir}/Publication.supplementary_data.tsv"

uiPublication_json_output_file = f"{json_out_dir}/uiPublication.json"

scalar_publication_fields = (
    'publication_id',
    'pubmed_id',
    'doi',
    'author',
    'group_name',
    'title',
    'title_legacy',
    'journal',
    'journal_url',
    'year',
    'abstract',
    'citation',
    'program_name'
)

pdc_study_ids = get_unique_values_from_tsv_column( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, publication_out_dir ):
    
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

output_tsv_keywords = [
    'PUBLICATION',
    'PUBLICATION_STUDIES',
    'PUBLICATION_DISEASE_TYPES',
    'PUBLICATION_SUPP_DATA'
]

output_tsv_filenames = [
    publication_tsv,
    publication_studies_tsv,
    publication_disease_types_tsv,
    publication_supplementary_data_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# We need to query by study ID, but some publications are associated with multiple studies. Keep track of which ones we've printed so we don't duplicate rows.
# 
# ASSUMPTION: publication_id is unique to each Publication object.

seen_ids = set()

with open(uiPublication_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_publication_fields, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )
    print( *('publication_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['PUBLICATION_STUDIES'] )
    print( *('publication_id', 'disease_type'), sep='\t', end='\n', file=output_tsvs['PUBLICATION_DISEASE_TYPES'] )
    print( *('publication_id', 'file_name'), sep='\t', end='\n', file=output_tsvs['PUBLICATION_SUPP_DATA'] )

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                uiPublication( ''' + f'pdc_study_id: "{pdc_study_id}"' + ''' ) {
                    ''' + '\n                    '.join(scalar_publication_fields) + '''
                    studies {
                        study_id
                    }
                    disease_types
                    supplementary_data
                }
            }'''
        }

        # Send the uiPublication() query to the API server.

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

        for publication in result['data']['uiPublication']:
            
            if publication['publication_id'] not in seen_ids:
                
                seen_ids.add(publication['publication_id'])

                publication_row = list()

                for field_name in scalar_publication_fields:
                    
                    if publication[field_name] is not None:
                        
                        publication_row.append(publication[field_name])

                    else:
                        
                        publication_row.append('')

                print( *publication_row, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )

                if publication['studies'] is not None and len(publication['studies']) > 0:
                    
                    for study in publication['studies']:
                        
                        print( *(publication['publication_id'], study['study_id']), sep='\t', end='\n', file=output_tsvs['PUBLICATION_STUDIES'] )

                if publication['disease_types'] is not None and len(publication['disease_types']) > 0:
                    
                    for disease_type in publication['disease_types']:
                        
                        print( *( publication['publication_id'], disease_type ), sep='\t', end='\n', file=output_tsvs['PUBLICATION_DISEASE_TYPES'] )

                if publication['supplementary_data'] is not None and len(publication['supplementary_data']) > 0:
                    
                    for file_name in publication['supplementary_data']:
                        
                        print( *( publication['publication_id'], file_name ), sep='\t', end='\n', file=output_tsvs['PUBLICATION_SUPP_DATA'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



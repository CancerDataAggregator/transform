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

biospecimen_out_dir = f"{output_root}/Biospecimen"

biospecimen_tsv = f"{biospecimen_out_dir}/Biospecimen.tsv"
biospecimen_study_tsv = f"{biospecimen_out_dir}/Biospecimen.Study.tsv"
biospecimen_externalReferences_tsv = f"{biospecimen_out_dir}/Biospecimen.externalReferences.tsv"

# We pull these from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store these in the Biospecimen directory
# to indicate that's where this version came from. This is half-normalization; will
# merge later during aggregation across (in this case) Biospecimen and Case and Clinical and UIClinical.

entity_reference_tsv = f"{biospecimen_out_dir}/EntityReference.tsv"

biospecimenPerStudy_json_output_file = f"{json_out_dir}/biospecimenPerStudy.json"

scalar_biospecimen_fields = (
    'aliquot_id',
    'aliquot_submitter_id',
    'aliquot_is_ref',
    'aliquot_status',
    'sample_status',
    'sample_type',
    'case_is_ref',
    'case_status',
    'disease_type',
    'primary_site',
    'pool',
    'taxon',
    'sample_id',
    'sample_submitter_id',
    'case_id',
    'case_submitter_id',
    'project_name'
)

scalar_entity_reference_fields = (
    'external_reference_id',
    'entity_id',
    'entity_type',
    'reference_id',
    'reference_type',
    'reference_entity_type',
    'reference_entity_alias',
    'reference_entity_location',
    'reference_resource_name',
    'reference_resource_shortname',
    'submitter_id_name',
    'annotation'
)

pdc_study_ids = get_unique_values_from_tsv_column( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, biospecimen_out_dir ):
    
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
    'BIOSPECIMEN',
    'BIOSPECIMEN_STUDY',
    'BIOSPECIMEN_EXTERNAL_REFERENCES',
    'ENTITY_REFERENCE'
]

output_tsv_filenames = [
    biospecimen_tsv,
    biospecimen_study_tsv,
    biospecimen_externalReferences_tsv,
    entity_reference_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open(biospecimenPerStudy_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_biospecimen_fields, sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN'] )
    print( *scalar_entity_reference_fields, sep='\t', end='\n', file=output_tsvs['ENTITY_REFERENCE'] )

    print( *('aliquot_id', 'pdc_study_id'), sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_STUDY'] )
    print( *('aliquot_id', 'external_reference_id'), sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_EXTERNAL_REFERENCES'] )

    # Some Biospecimens (Aliquots, really) are in multiple Studies. Don't duplicate rows in the Biospecimen table.

    seen_aliquot_IDs = set()

    # Some externalReferences records refer to collections, and are thus repeated a lot. Don't record duplicate rows in the EntityReference table.

    seen_external_reference_IDs = set()

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                biospecimenPerStudy( ''' + f'pdc_study_id: "{pdc_study_id}", acceptDUA: true' + ''' ) {
                    ''' + '\n                    '.join(scalar_biospecimen_fields) + '''
                    externalReferences {
                        ''' + '\n                        '.join(scalar_entity_reference_fields) + '''
                    }
                }
            }'''
        }

        # Send the biospecimenPerStudy() query to the API server.

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

        for biospecimen in result['data']['biospecimenPerStudy']:
            
            if biospecimen['aliquot_id'] not in seen_aliquot_IDs:
                
                # Only make a row for this Biospecimen (Aliquot) if we haven't seen it before.

                seen_aliquot_IDs.add(biospecimen['aliquot_id'])

                biospecimen_row = list()

                for field_name in scalar_biospecimen_fields:
                    
                    if biospecimen[field_name] is not None:
                        
                        biospecimen_row.append(biospecimen[field_name])

                    else:
                        
                        biospecimen_row.append('')

                print( *biospecimen_row, sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN'] )

                # Scan this Biospecimen's (Aliquot's) externalReferences if we haven't seen the Biospecimen (Aliquot) before.

                if biospecimen['externalReferences'] is not None and len(biospecimen['externalReferences']) > 0:
                    
                    for entity_reference in biospecimen['externalReferences']:
                        
                        # Record only one EntityReference row per external reference object (some will repeat because they refer to collections and not to individual Biospecimens (Aliquots)).

                        if entity_reference['external_reference_id'] not in seen_external_reference_IDs:
                            
                            seen_external_reference_IDs.add(entity_reference['external_reference_id'])

                            entity_reference_row = list()

                            for field_name in scalar_entity_reference_fields:
                                
                                if entity_reference[field_name] is not None:
                                    
                                    entity_reference_row.append(entity_reference[field_name])

                                else:
                                    
                                    entity_reference_row.append('')

                            print( *entity_reference_row, sep='\t', end='\n', file=output_tsvs['ENTITY_REFERENCE'] )

                        # Even if we've seen the reference before, be sure to associate it with the current Biospecimen (Aliquot).

                        print( *(biospecimen['aliquot_id'], entity_reference['external_reference_id']), sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_EXTERNAL_REFERENCES'] )

            # Even if we've seen this Biospecimen (Aliquot) before, be sure to associate it with the current Study.

            print( *(biospecimen['aliquot_id'], pdc_study_id), sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_STUDY'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



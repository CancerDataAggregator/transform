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

legacy_study_out_dir = f"{output_root}/LegacyStudy"
legacy_publication_out_dir = f"{output_root}/LegacyPublication"

legacy_study_tsv = f"{legacy_study_out_dir}/LegacyStudy.tsv"
legacy_study_supplementary_files_count_tsv = f"{legacy_study_out_dir}/LegacyStudy.supplementaryFilesCount.tsv"
legacy_study_non_supplementary_files_count_tsv = f"{legacy_study_out_dir}/LegacyStudy.nonSupplementaryFilesCount.tsv"
legacy_study_publications_tsv = f"{legacy_study_out_dir}/LegacyStudy.publications.tsv"

legacy_publication_tsv = f"{legacy_publication_out_dir}/LegacyPublication.tsv"
legacy_publication_disease_types_tsv = f"{legacy_publication_out_dir}/LegacyPublication.disease_types.tsv"
legacy_publication_supplementary_data_tsv = f"{legacy_publication_out_dir}/LegacyPublication.supplementary_data.tsv"
legacy_publication_studies_tsv = f"{legacy_publication_out_dir}/LegacyPublication.studies.tsv"

uiLegacyStudies_json_output_file = f"{json_out_dir}/uiLegacyStudies.json"

scalar_legacy_study_fields = (
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'submitter_id_name',
    'study_description',
    'embargo_date',
    'analytical_fraction',
    'experiment_type',
    'acquisition_type',
    'cptac_phase',
    'sort_order',
    'project_submitter_id'
)

scalar_legacy_study_files_count_fields = (
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

scalar_legacy_publication_fields = (
    'publication_id',
    'pubmed_id',
    'doi',
    'author',
    'title',
    'title_legacy',
    'journal',
    'journal_url',
    'year',
    'abstract',
    'citation'
)

api_query_json = {
    'query': '''    {
        uiLegacyStudies {
            ''' + '\n            '.join(scalar_legacy_study_fields) + '''
            supplementaryFilesCount {
                ''' + '\n                '.join(scalar_legacy_study_files_count_fields) + '''
            }
            nonSupplementaryFilesCount {
                ''' + '\n                '.join(scalar_legacy_study_files_count_fields) + '''
            }
            publications {
                ''' + '\n                '.join(scalar_legacy_publication_fields) + '''
                disease_types
                supplementary_data
                studies {
                    study_id
                }
            }
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, legacy_study_out_dir, legacy_publication_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the uiLegacyStudies() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(uiLegacyStudies_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save the returned UIStudy objects in TSV form.

output_tsv_keywords = [
    'LEGACY_STUDY',
    'LEGACY_STUDY_SUPP_COUNT',
    'LEGACY_STUDY_NON_SUPP_COUNT',
    'LEGACY_STUDY_PUBLICATIONS',
    'LEGACY_PUBLICATION',
    'LEGACY_PUBLICATION_DISEASE_TYPES',
    'LEGACY_PUBLICATION_SUPP_DATA',
    'LEGACY_PUBLICATION_STUDIES'
]

output_tsv_filenames = [
    legacy_study_tsv,
    legacy_study_supplementary_files_count_tsv,
    legacy_study_non_supplementary_files_count_tsv,
    legacy_study_publications_tsv,
    legacy_publication_tsv,
    legacy_publication_disease_types_tsv,
    legacy_publication_supplementary_data_tsv,
    legacy_publication_studies_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_legacy_study_fields, sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY'] )
print( *scalar_legacy_publication_fields, sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION'] )

# We fully qualify "LegacyStudy.study_id" here instead of just giving "study_id" because the File objects enumerated in each map already have their own "study_id" field.

print( *(['LegacyStudy.study_id'] + list(scalar_legacy_study_files_count_fields)), sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_SUPP_COUNT'] )
print( *(['LegacyStudy.study_id'] + list(scalar_legacy_study_files_count_fields)), sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_NON_SUPP_COUNT'] )

print( *('study_id', 'publication_id'), sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_PUBLICATIONS'] )
print( *('publication_id', 'disease_type'), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_DISEASE_TYPES'] )
print( *('publication_id', 'supplementary_data'), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_SUPP_DATA'] )
print( *('publication_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_STUDIES'] )

# LegacyPublications are sometimes associated with more than one LegacyStudy. Only save one row per LegacyPublication.

seen_legacy_publication_IDs = set()

# Parse the returned data and save to TSV.

for legacy_study in result['data']['uiLegacyStudies']:
    
    legacy_study_row = list()

    for field_name in scalar_legacy_study_fields:
        
        if legacy_study[field_name] is not None:
            
            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

            legacy_study_row.append(json.dumps(legacy_study[field_name]).strip('"'))

        else:
            
            legacy_study_row.append('')

    print( *legacy_study_row, sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY'] )

    if legacy_study['supplementaryFilesCount'] is not None and len(legacy_study['supplementaryFilesCount']) > 0:
        
        for files_count in legacy_study['supplementaryFilesCount']:
            
            files_count_row = [legacy_study['study_id']]

            for field_name in scalar_legacy_study_files_count_fields:
                
                if files_count[field_name] is not None:
                    
                    files_count_row.append(files_count[field_name])

                else:
                    
                    files_count_row.append('')

            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_SUPP_COUNT'] )

    if legacy_study['nonSupplementaryFilesCount'] is not None and len(legacy_study['nonSupplementaryFilesCount']) > 0:
        
        for files_count in legacy_study['nonSupplementaryFilesCount']:
            
            files_count_row = [legacy_study['study_id']]

            for field_name in scalar_legacy_study_files_count_fields:
                
                if files_count[field_name] is not None:
                    
                    files_count_row.append(files_count[field_name])

                else:
                    
                    files_count_row.append('')

            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_NON_SUPP_COUNT'] )

    if legacy_study['publications'] is not None and len(legacy_study['publications']) > 0:
        
        for legacy_publication in legacy_study['publications']:
            
            print( *(legacy_study['study_id'], legacy_publication['publication_id']), sep='\t', end='\n', file=output_tsvs['LEGACY_STUDY_PUBLICATIONS'] )

            if legacy_publication['publication_id'] not in seen_legacy_publication_IDs:
                
                seen_legacy_publication_IDs.add(legacy_publication['publication_id'])

                legacy_publication_row = list()

                for field_name in scalar_legacy_publication_fields:
                    
                    if legacy_publication[field_name] is not None:
                        
                        # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                        legacy_publication_row.append(json.dumps(legacy_publication[field_name]).strip('"'))

                    else:
                        
                        legacy_publication_row.append('')

                print( *legacy_publication_row, sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION'] )

                if legacy_publication['disease_types'] is not None and len(legacy_publication['disease_types']) > 0:
                    
                    for disease_type in legacy_publication['disease_types']:
                        
                        print( *(legacy_publication['publication_id'], disease_type), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_DISEASE_TYPES'] )

                if legacy_publication['supplementary_data'] is not None and len(legacy_publication['supplementary_data']) > 0:
                    
                    for supplementary_data in legacy_publication['supplementary_data']:
                        
                        print( *(legacy_publication['publication_id'], supplementary_data), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_SUPP_DATA'] )

                if legacy_publication['studies'] is not None and len(legacy_publication['studies']) > 0:
                    
                    for study in legacy_publication['studies']:
                        
                        print( *(legacy_publication['publication_id'], study['study_id']), sep='\t', end='\n', file=output_tsvs['LEGACY_PUBLICATION_STUDIES'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



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

uistudy_out_dir = f"{output_root}/UIStudy"

uistudy_tsv = f"{uistudy_out_dir}/UIStudy.tsv"
uistudy_contacts_tsv = f"{uistudy_out_dir}/UIStudy.contacts.tsv"
uistudy_files_count_tsv = f"{uistudy_out_dir}/UIStudy.filesCount.tsv"
uistudy_supplementary_files_count_tsv = f"{uistudy_out_dir}/UIStudy.supplementaryFilesCount.tsv"
uistudy_non_supplementary_files_count_tsv = f"{uistudy_out_dir}/UIStudy.nonSupplementaryFilesCount.tsv"
uistudy_versions_tsv = f"{uistudy_out_dir}/UIStudy.versions.tsv"

# We pull Contact records from multiple sources and don't want to clobber a single
# master list over different pulls, so we store this in the Study directory
# to indicate that's where it came from. This is half-normalization; will
# merge later during aggregation across (in this case) Study and UIStudy.

contact_tsv = f"{uistudy_out_dir}/Contact.tsv"

getPaginatedUIStudy_json_output_file = f"{json_out_dir}/getPaginatedUIStudy.json"

scalar_uistudy_fields = (
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
    'cases_count',
    'aliquots_count',
    'program_name',
    'project_name'
)

scalar_contact_fields = (
    'contact_id',
    'name',
    'institution',
    'email',
    'url'
)

scalar_uistudy_files_count_fields = (
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

scalar_version_fields = (
    'name',
    'number'
)

api_query_json = {
    'query': '''    {
        getPaginatedUIStudy( getAll: true ) {
            uiStudies {
                ''' + '\n                '.join(scalar_uistudy_fields) + '''
                contacts {
                    ''' + '\n                    '.join(scalar_contact_fields) + '''
                }
                filesCount {
                    ''' + '\n                    '.join(scalar_uistudy_files_count_fields) + '''
                }
                supplementaryFilesCount {
                    ''' + '\n                    '.join(scalar_uistudy_files_count_fields) + '''
                }
                nonSupplementaryFilesCount {
                    ''' + '\n                    '.join(scalar_uistudy_files_count_fields) + '''
                }
                versions {
                    ''' + '\n                    '.join(scalar_version_fields) + '''
                }
            }
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, uistudy_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the getPaginatedUIStudy() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(getPaginatedUIStudy_json_output_file, 'w') as JSON:
    
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
    'UISTUDY',
    'UISTUDY_CONTACTS',
    'UISTUDY_FILES_COUNT',
    'UISTUDY_SUPP_COUNT',
    'UISTUDY_NON_SUPP_COUNT',
    'UISTUDY_VERSIONS',
    'CONTACT'
]

output_tsv_filenames = [
    uistudy_tsv,
    uistudy_contacts_tsv,
    uistudy_files_count_tsv,
    uistudy_supplementary_files_count_tsv,
    uistudy_non_supplementary_files_count_tsv,
    uistudy_versions_tsv,
    contact_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_uistudy_fields, sep='\t', end='\n', file=output_tsvs['UISTUDY'] )
print( *scalar_contact_fields, sep='\t', end='\n', file=output_tsvs['CONTACT'] )

print( *('study_id', 'contact_id'), sep='\t', end='\n', file=output_tsvs['UISTUDY_CONTACTS'] )
print( *(['study_id'] + list(scalar_version_fields)), sep='\t', end='\n', file=output_tsvs['UISTUDY_VERSIONS'] )

# We fully qualify "UIStudy.study_id" here instead of just giving "study_id" because the File objects enumerated in each map already have their own "study_id" field.

print( *(['UIStudy.study_id'] + list(scalar_uistudy_files_count_fields)), sep='\t', end='\n', file=output_tsvs['UISTUDY_FILES_COUNT'] )
print( *(['UIStudy.study_id'] + list(scalar_uistudy_files_count_fields)), sep='\t', end='\n', file=output_tsvs['UISTUDY_SUPP_COUNT'] )
print( *(['UIStudy.study_id'] + list(scalar_uistudy_files_count_fields)), sep='\t', end='\n', file=output_tsvs['UISTUDY_NON_SUPP_COUNT'] )

# Parse the returned data and save to TSV.

for uistudy in result['data']['getPaginatedUIStudy']['uiStudies']:
    
    uistudy_row = list()

    for field_name in scalar_uistudy_fields:
        
        if uistudy[field_name] is not None:
            
            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

            uistudy_row.append(json.dumps(uistudy[field_name]).strip('"'))

        else:
            
            uistudy_row.append('')

    print( *uistudy_row, sep='\t', end='\n', file=output_tsvs['UISTUDY'] )

    if uistudy['contacts'] is not None and len(uistudy['contacts']) > 0:
        
        for contact in uistudy['contacts']:
            
            contact_row = list()

            for field_name in scalar_contact_fields:
                
                if contact[field_name] is not None:
                    
                    contact_row.append(contact[field_name])

                else:
                    
                    contact_row.append('')

            print( *contact_row, sep='\t', end='\n', file=output_tsvs['CONTACT'] )

            print( *( uistudy['study_id'], contact['contact_id'] ), sep='\t', end='\n', file=output_tsvs['UISTUDY_CONTACTS'] )

    if uistudy['filesCount'] is not None and len(uistudy['filesCount']) > 0:
        
        for files_count in uistudy['filesCount']:
            
            files_count_row = [uistudy['study_id']]

            for field_name in scalar_uistudy_files_count_fields:
                
                if files_count[field_name] is not None:
                    
                    files_count_row.append(files_count[field_name])

                else:
                    
                    files_count_row.append('')

            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['UISTUDY_FILES_COUNT'] )

    if uistudy['supplementaryFilesCount'] is not None and len(uistudy['supplementaryFilesCount']) > 0:
        
        for files_count in uistudy['supplementaryFilesCount']:
            
            files_count_row = [uistudy['study_id']]

            for field_name in scalar_uistudy_files_count_fields:
                
                if files_count[field_name] is not None:
                    
                    files_count_row.append(files_count[field_name])

                else:
                    
                    files_count_row.append('')

            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['UISTUDY_SUPP_COUNT'] )

    if uistudy['nonSupplementaryFilesCount'] is not None and len(uistudy['nonSupplementaryFilesCount']) > 0:
        
        for files_count in uistudy['nonSupplementaryFilesCount']:
            
            files_count_row = [uistudy['study_id']]

            for field_name in scalar_uistudy_files_count_fields:
                
                if files_count[field_name] is not None:
                    
                    files_count_row.append(files_count[field_name])

                else:
                    
                    files_count_row.append('')

            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['UISTUDY_NON_SUPP_COUNT'] )

    if uistudy['versions'] is not None and len(uistudy['versions']) > 0:
        
        for version in uistudy['versions']:
            
            version_row = [uistudy['study_id']]

            for field_name in scalar_version_fields:
                
                if version[field_name] is not None:
                    
                    version_row.append(version[field_name])

                else:
                    
                    version_row.append('')

            print( *version_row, sep='\t', end='\n', file=output_tsvs['UISTUDY_VERSIONS'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



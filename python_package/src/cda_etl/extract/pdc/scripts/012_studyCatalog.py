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

study_catalog_out_dir = f"{output_root}/StudyCatalog"

study_catalog_versions_tsv = f"{study_catalog_out_dir}/StudyCatalog.versions.tsv"

# We pull Study records from multiple sources and don't want to clobber a single
# master list over different pulls, so we store this in the StudyCatalog
# directory to indicate that's where it came from. This is half-normalization; will
# merge later during aggregation across (in this case) Study and StudyCatalog.
# 
# Note: some Study fields (specifically study_version, is_latest_version and study_shortname)
# are ALWAYS NULL when you retrieve Study objects via, say, allPrograms(). These same fields
# are NOT ALWAYS NULL when you retrieve the SAME Study objects via studyCatalog().
# 
# I see no reason to bother re-parsing non-atomic fields Study.disease_types,
# Study.primary_sites, Study.contacts or Study.project here: we've
# done these once already while processing allPrograms() and (unlike the fields
# named above), they do not appear to come back with different values when requested
# via studyCatalog().

study_tsv = f"{study_catalog_out_dir}/Study.tsv"

studyCatalog_json_output_file = f"{json_out_dir}/studyCatalog.json"

scalar_study_fields = (
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'submitter_id_name',
    'study_name',
    'study_shortname',
    'study_version',
    'is_latest_version',
    'embargo_date',
    'disease_type',
    'primary_site',
    'analytical_fraction',
    'experiment_type',
    'acquisition_type',
    'cases_count',
    'aliquots_count',
    'program_id',
    'program_name',
    'project_id',
    'project_submitter_id',
    'project_name'
)

api_query_json = {
    'query': '''    {
        studyCatalog( acceptDUA: true ) {
            pdc_study_id
            versions {
                ''' + '\n                '.join(scalar_study_fields) + '''
            }
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, study_catalog_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the studyCatalog() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(studyCatalog_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save the returned Program, Project, Study, Contact, and Study.filesCount (File) objects in
### TSV form, as well as association TSVs enumerating inter-object containment relationships.

output_tsv_keywords = [
    'STUDY_CATALOG_VERSIONS',
    'STUDY'
]

output_tsv_filenames = [
    study_catalog_versions_tsv,
    study_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *('pdc_study_id', 'study_version', 'is_latest_version', 'study_id', 'study_shortname'), sep='\t', end='\n', file=output_tsvs['STUDY_CATALOG_VERSIONS'] )

print( *scalar_study_fields, sep='\t', end='\n', file=output_tsvs['STUDY'] )

# Parse the returned data and save to TSV.

for catalog_entry in result['data']['studyCatalog']:
    
    if catalog_entry['versions'] is not None and len(catalog_entry['versions']) > 0:
        
        for version in catalog_entry['versions']:
            
            # 1. Save a copy of the Study record in STUDY.

            study_row = list()

            for field_name in scalar_study_fields:
                
                if version[field_name] is not None:
                    
                    study_row.append(version[field_name])

                else:
                    
                    study_row.append('')

            print( *study_row, sep='\t', end='\n', file=output_tsvs['STUDY'] )

            # 2. Save version highlights for this Study record in STUDY_CATALOG_VERSIONS.

            version_row = [catalog_entry['pdc_study_id']]

            for field_name in 'study_version', 'is_latest_version', 'study_id', 'study_shortname':
                
                if version[field_name] is not None:
                    
                    version_row.append(version[field_name])

                else:
                    
                    version_row.append('')

            print( *version_row, sep='\t', end='\n', file=output_tsvs['STUDY_CATALOG_VERSIONS'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



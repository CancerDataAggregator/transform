#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import map_columns_one_to_one, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_ctep_disease_term_tsv = path.join( study_out_dir, 'Study.ctep_disease_term.tsv' )
study_diagnosis_record_id_tsv = path.join( study_out_dir, 'Study.diagnosis_record_id.tsv' )

study_short_name_to_study_id = map_columns_one_to_one( study_tsv, 'study_short_name', 'study_id' )

diagnosis_out_dir = path.join( output_root, 'Diagnosis' )
diagnosis_tsv = path.join( diagnosis_out_dir, 'Diagnosis.from_studyDiagnosisByStudyShortName.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
studyDiagnosisByStudyShortName_json_output_file = path.join( json_out_dir, 'studyDiagnosisByStudyShortName.json' )

# What we actually receive:
# 
# type StudyDiagnosis {
#   study_short_name: String
#   diagnosis: [Diagnosis]
#   ctep_disease_terms: [String]
# }

# Non-scalar Diagnosis fields:
#     <none>
scalar_diagnosis_fields = [
    'diagnosis_record_id',
    'primary_diagnosis_disease_group',
    'ctep_disease_term',
    'meddra_disease_code',
    'snomed_disease_term',
    'snomed_disease_code',
    'primary_disease_site',
    'histology',
    'histological_subtype',
    'stage_of_disease',
    'tumor_grade'
]

studyDiagnosisByStudyShortName_query = '''
query search {
    studyDiagnosisByStudyShortName ( study_short_name: "$STUDY_SHORT_NAME$" ) {
        study_short_name
        diagnosis {
            ''' + '\n            '.join( scalar_diagnosis_fields ) + '''
        }
        ctep_disease_terms
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, study_out_dir, diagnosis_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Open handles for output files to save TSVs describing returned data.
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
    'DIAGNOSIS',
    'STUDY_CTEP_DISEASE_TERM',
    'STUDY_DIAGNOSIS'
]

output_tsv_filenames = [
    diagnosis_tsv,
    study_ctep_disease_term_tsv,
    study_diagnosis_record_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_diagnosis_fields, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )
print( *[ 'study_id', 'ctep_disease_term' ], sep='\t', end='\n', file=output_tsvs['STUDY_CTEP_DISEASE_TERM'] )
print( *[ 'study_id', 'diagnosis_record_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_DIAGNOSIS'] )

with open( studyDiagnosisByStudyShortName_json_output_file, 'w' ) as JSON:
    
    for study_short_name in sorted( study_short_name_to_study_id ):
        # Send the studyDiagnosisByStudyShortName() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, studyDiagnosisByStudyShortName_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, studyDiagnosisByStudyShortName_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for study_diagnosis in result['data']['studyDiagnosisByStudyShortName']:
            study_id = study_short_name_to_study_id[study_diagnosis['study_short_name']]

            seen_ctep_disease_terms = set()

            for diagnosis in study_diagnosis['diagnosis']:
                # Note: there are stub records here whose fields are all null.
                diagnosis_row = list()
                all_null = True
                for field_name in scalar_diagnosis_fields:
                    if diagnosis[field_name] is not None:
                        diagnosis_row.append( diagnosis[field_name] )
                        all_null = False
                        if field_name == 'ctep_disease_term':
                            seen_ctep_disease_terms.add( diagnosis[field_name] )
                    else:
                        diagnosis_row.append( '' )
                # Don't process all-null Diagnosis records.
                if not all_null:
                    print( *diagnosis_row, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )
                    # Associate this study with this Diagnosis record. ID access attempts should fail with a KeyError if the needed ID isn't present.
                    print( *[ study_id, diagnosis['diagnosis_record_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_DIAGNOSIS'] )

            # StudyDiagnosis.ctep_disease_terms [array of strings]. Associate with the Study record referenced in this StudyDiagnosis object.
            if 'ctep_disease_terms' in study_diagnosis and len( study_diagnosis['ctep_disease_terms'] ) > 0:
                for ctep_disease_term in study_diagnosis['ctep_disease_terms']:
                    # Sanity check.
                    if ctep_disease_term not in seen_ctep_disease_terms:
                        print( f"WARNING: CTEP disease term {ctep_disease_term} explicitly associated with Study {study_id} but not listed in any associated Diagnosis record: please investigate.", file=sys.stderr )
                    print( *[ study_id, ctep_disease_term ], sep='\t', end='\n', file=output_tsvs['STUDY_CTEP_DISEASE_TERM'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )



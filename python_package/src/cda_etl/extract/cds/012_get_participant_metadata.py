#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://dataservice.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'cds' )

participant_out_dir = path.join( output_root, 'participant' )

participant_output_tsv = path.join( participant_out_dir, 'participant.tsv' )

participant_specimens_output_tsv = path.join( participant_out_dir, 'participant.specimen_id.tsv' )

participant_samples_output_tsv = path.join( participant_out_dir, 'participant.sample_id.tsv' )

participant_diagnoses_output_tsv = path.join( participant_out_dir, 'participant.diagnosis_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

participant_output_json = path.join( json_out_dir, 'participant_metadata.json' )

# type participant {
#     participant_id: String
#     race: String
#     gender: String
#     ethnicity: String
#     dbGaP_subject_id: String
#     study: study
#     diagnoses(first: Int, offset: Int, orderBy: [_diagnosisOrdering!], filter: _diagnosisFilter): [diagnosis]
#     specimens(first: Int, offset: Int, orderBy: [_specimenOrdering!], filter: _specimenFilter): [specimen]
#     samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
# }

scalar_participant_fields = [
    'participant_id',
    'race',
    'gender',
    'ethnicity',
    'dbGaP_subject_id'
]

participant_api_query_json_template = f'''    {{
        
        participant( first: {page_size}, offset: __OFFSET__, orderBy: [ participant_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_participant_fields ) + f'''
            study {{
                study_name
            }}
            diagnoses( first: {page_size}, offset: 0, orderBy: [ diagnosis_id_asc ] ) {{
                diagnosis_id
            }}
            specimens( first: {page_size}, offset: 0, orderBy: [ specimen_id_asc ] ) {{
                specimen_id
            }}
            samples( first: {page_size}, offset: 0, orderBy: [ sample_id_asc ] ) {{
                sample_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ participant_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( participant_output_json, 'w' ) as JSON, \
        open( participant_output_tsv, 'w' ) as PARTICIPANT, \
        open( participant_specimens_output_tsv, 'w' ) as PARTICIPANT_SPECIMENS, \
        open( participant_samples_output_tsv, 'w' ) as PARTICIPANT_SAMPLES, \
        open( participant_diagnoses_output_tsv, 'w' ) as PARTICIPANT_DIAGNOSES:
    
    participant_fields = [ 'study_name' ] + scalar_participant_fields

    print( *participant_fields, sep='\t', file=PARTICIPANT )
    print( *[ 'study_name', 'participant_id', 'specimen_id' ], sep='\t', file=PARTICIPANT_SPECIMENS )
    print( *[ 'study_name', 'participant_id', 'sample_id' ], sep='\t', file=PARTICIPANT_SAMPLES )
    print( *[ 'study_name', 'participant_id', 'diagnosis_id' ], sep='\t', file=PARTICIPANT_DIAGNOSES )

    while not null_result:
    
        participant_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), participant_api_query_json_template )
        }

        participant_response = requests.post( api_url, json=participant_api_query_json )

        if not participant_response.ok:
            
            print( participant_api_query_json['query'], file=sys.stderr )

            participant_response.raise_for_status()

        participant_result = json.loads( participant_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( participant_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        participant_count = len( participant_result['data']['participant'] )

        if participant_count == 0:
            
            null_result = True

        else:
            
            for participant in participant_result['data']['participant']:
                
                participant_id = participant['participant_id']

                # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.
                # 
                # Also, we want this to break with a key error if it doesn't exist.

                study_name = json.dumps( participant['study']['study_name'] ).strip( '"' )

                participant_row = [ study_name ]

                for field_name in scalar_participant_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( participant[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    participant_row.append( field_value )

                print( *participant_row, sep='\t', file=PARTICIPANT )

                for diagnosis in participant['diagnoses']:
                    
                    print( *[ study_name, participant_id, diagnosis['diagnosis_id'] ], sep='\t', file=PARTICIPANT_DIAGNOSES )

                diagnosis_count = len( participant['diagnoses'] )

                if diagnosis_count == page_size:
                    
                    print( f"WARNING: Participant {participant_id} in study '{study_name}' has at least {page_size} diagnoses: implement paging here or risk data loss.", file=sys.stderr )

                for specimen in participant['specimens']:
                    
                    print( *[ study_name, participant_id, specimen['specimen_id'] ], sep='\t', file=PARTICIPANT_SPECIMENS )

                specimen_count = len( participant['specimens'] )

                if specimen_count == page_size:
                    
                    print( f"WARNING: Participant {participant_id} in study '{study_name}' has at least {page_size} specimens: implement paging here or risk data loss.", file=sys.stderr )

                for sample in participant['samples']:
                    
                    print( *[ study_name, participant_id, sample['sample_id'] ], sep='\t', file=PARTICIPANT_SAMPLES )

                sample_count = len( participant['samples'] )

                if sample_count == page_size:
                    
                    print( f"WARNING: Participant {participant_id} in study '{study_name}' has at least {page_size} samples: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size



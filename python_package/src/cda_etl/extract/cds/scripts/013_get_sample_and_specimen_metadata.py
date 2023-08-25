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

sample_out_dir = path.join( output_root, 'sample' )

sample_output_tsv = path.join( sample_out_dir, 'sample.tsv' )

specimen_out_dir = path.join( output_root, 'specimen' )

specimen_output_tsv = path.join( specimen_out_dir, 'specimen.tsv' )

specimen_samples_output_tsv = path.join( specimen_out_dir, 'specimen.sample_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

sample_specimen_output_json = path.join( json_out_dir, 'sample_and_specimen_metadata.json' )

# type sample {
#     sample_id: String
#     sample_type: String
#     sample_tumor_status: String
#     sample_anatomic_site: String
#     sample_age_at_collection: Int
#     derived_from_specimen: String
#     biosample_accession: String
#     participant: participant
#     specimen: specimen
#     files(first: Int, offset: Int, orderBy: [_fileOrdering!], filter: _fileFilter): [file]
#     genomic_info(first: Int, offset: Int, orderBy: [_genomic_infoOrdering!], filter: _genomic_infoFilter): [genomic_info]
# }

scalar_sample_fields = [
    'sample_id',
    'sample_type',
    'sample_tumor_status',
    'sample_anatomic_site',
    'sample_age_at_collection',
    'derived_from_specimen',
    'biosample_accession'
]

# type specimen {
#     specimen_id: String
#     participant: participant
#     samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
# }

scalar_specimen_fields = [
    'specimen_id'
]

sample_api_query_json_template = f'''    {{
        
        sample( first: {page_size}, offset: __OFFSET__, orderBy: [ sample_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_sample_fields ) + f'''
            participant {{
                participant_id
                study {{
                    study_name
                }}
            }}
            specimen {{
                ''' + '''
                '''.join( scalar_specimen_fields ) + f'''
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ sample_out_dir, specimen_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( sample_specimen_output_json, 'w' ) as JSON, open( sample_output_tsv, 'w' ) as SAMPLE, open( specimen_output_tsv, 'w' ) as SPECIMEN, open( specimen_samples_output_tsv, 'w' ) as SPECIMEN_SAMPLES:
    
    sample_fields = [ 'study_name' ] + scalar_sample_fields
    specimen_fields = [ 'study_name' ] + scalar_specimen_fields

    print( *sample_fields, sep='\t', file=SAMPLE )
    print( *specimen_fields, sep='\t', file=SPECIMEN )
    print( *[ 'study_name', 'specimen_id', 'sample_id' ], sep='\t', file=SPECIMEN_SAMPLES )

    while not null_result:
    
        sample_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), sample_api_query_json_template )
        }

        sample_response = requests.post( api_url, json=sample_api_query_json )

        if not sample_response.ok:
            
            print( sample_api_query_json['query'], file=sys.stderr )

            sample_response.raise_for_status()

        sample_result = json.loads( sample_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( sample_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        sample_count = len( sample_result['data']['sample'] )

        if sample_count == 0:
            
            null_result = True

        else:
            
            for sample in sample_result['data']['sample']:
                
                sample_id = sample['sample_id']

                # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.
                # 
                # Also, we want this to break with a key error if it doesn't exist.

                study_name = json.dumps( sample['participant']['study']['study_name'] ).strip( '"' )

                sample_row = [ study_name ]

                for field_name in scalar_sample_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( sample[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    sample_row.append( field_value )

                print( *sample_row, sep='\t', file=SAMPLE )

                specimen = sample['specimen']

                if specimen is not None:
                    
                    specimen_row = [ study_name ]

                    for field_name in scalar_specimen_fields:
                        
                        # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                        field_value = json.dumps( specimen[field_name] ).strip( '"' )

                        if field_value == 'null':
                            
                            field_value = ''

                        specimen_row.append( field_value )

                    print( *specimen_row, sep='\t', file=SPECIMEN )

                    specimen_id = specimen['specimen_id']

                    print( *[ study_name, specimen_id, sample_id ], sep='\t', file=SPECIMEN_SAMPLES )

        offset = offset + page_size



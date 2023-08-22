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

genomic_info_out_dir = path.join( output_root, 'genomic_info' )

genomic_info_output_tsv = path.join( genomic_info_out_dir, 'genomic_info.tsv' )

genomic_info_samples_output_tsv = path.join( genomic_info_out_dir, 'genomic_info.sample_id.tsv' )

genomic_info_files_output_tsv = path.join( genomic_info_out_dir, 'genomic_info.files.file_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

genomic_info_output_json = path.join( json_out_dir, 'genomic_info_metadata.json' )

# type genomic_info {
#     library_id: String
#     bases: Int
#     number_of_reads: Int
#     avg_read_length: Float
#     coverage: Float
#     reference_genome_assembly: String
#     custom_assembly_fasta_file_for_alignment: String
#     design_description: String
#     library_strategy: String
#     library_layout: String
#     library_source: String
#     library_selection: String
#     platform: String
#     instrument_model: String
#     sequence_alignment_software: String
#     files(first: Int, offset: Int, orderBy: [_fileOrdering!], filter: _fileFilter): [file]
#     samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
#     file: file
# }

scalar_genomic_info_fields = [
    'library_id',
    'bases',
    'number_of_reads',
    'avg_read_length',
    'coverage',
    'reference_genome_assembly',
    'custom_assembly_fasta_file_for_alignment',
    'design_description',
    'library_strategy',
    'library_layout',
    'library_source',
    'library_selection',
    'platform',
    'instrument_model',
    'sequence_alignment_software'
]

genomic_info_api_query_json_template = f'''    {{
        
        genomic_info( first: {page_size}, offset: __OFFSET__, orderBy: [ library_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_genomic_info_fields ) + f'''
            files( first: {page_size}, offset: 0, orderBy: [ file_id_asc ] ) {{
                file_id
            }}
            samples( first: {page_size}, offset: 0, orderBy: [ sample_id_asc ] ) {{
                sample_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ genomic_info_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( genomic_info_output_json, 'w' ) as JSON, \
        open( genomic_info_output_tsv, 'w' ) as GENOMIC_INFO, \
        open( genomic_info_samples_output_tsv, 'w' ) as GENOMIC_INFO_SAMPLES, \
        open( genomic_info_files_output_tsv, 'w' ) as GENOMIC_INFO_FILES:
    
    print( *scalar_genomic_info_fields, sep='\t', file=GENOMIC_INFO )
    print( *[ 'library_id', 'sample_id' ], sep='\t', file=GENOMIC_INFO_SAMPLES )
    print( *[ 'library_id', 'file_id' ], sep='\t', file=GENOMIC_INFO_FILES )

    while not null_result:
    
        genomic_info_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), genomic_info_api_query_json_template )
        }

        genomic_info_response = requests.post( api_url, json=genomic_info_api_query_json )

        if not genomic_info_response.ok:
            
            print( genomic_info_api_query_json['query'], file=sys.stderr )

            genomic_info_response.raise_for_status()

        genomic_info_result = json.loads( genomic_info_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( genomic_info_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        genomic_info_count = len( genomic_info_result['data']['genomic_info'] )

        if genomic_info_count == 0:
            
            null_result = True

        else:
            
            for genomic_info in genomic_info_result['data']['genomic_info']:
                
                library_id = genomic_info['library_id']

                genomic_info_row = list()

                for field_name in scalar_genomic_info_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( genomic_info[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    genomic_info_row.append( field_value )

                print( *genomic_info_row, sep='\t', file=GENOMIC_INFO )

                for sample in genomic_info['samples']:
                    
                    print( *[ library_id, sample['sample_id'] ], sep='\t', file=GENOMIC_INFO_SAMPLES )

                sample_count = len( genomic_info['samples'] )

                if sample_count == page_size:
                    
                    print( f"WARNING: Genomic info record with library_id {library_id} has at least {page_size} samples: implement paging here or risk data loss.", file=sys.stderr )

                if genomic_info['files'] is not None:
                    
                    for file in genomic_info['files']:
                        
                        print( *[ library_id, file['file_id'] ], sep='\t', file=GENOMIC_INFO_FILES )

                file_count = len( genomic_info['files'] )

                if file_count == page_size:
                    
                    print( f"WARNING: Genomic info record with library_id {library_id} has at least {page_size} files: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size



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

def get_unique_values( tsv_path, column_name ):
    
    if not path.exists( tsv_path ):
        
        sys.exit(f"FATAL: Can't find specified TSV \"{tsv_path}\"; aborting.\n")

    with open(tsv_path) as IN:
        
        headers = IN.readline().rstrip('\n').split('\t')

        if column_name not in headers:
            
            sys.exit(f"FATAL: TSV \"{tsv_path}\" has no column named \"{column_name}\"; aborting.\n")

        scanned_values = set()

        for line in [ nextLine.rstrip() for nextLine in IN.readlines() ]:
            
            current_record = dict( zip( headers, line.split('\t') ) )

            scanned_values.add(current_record[column_name])

        return sorted(scanned_values)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

experimental_metadata_out_dir = f"{output_root}/ExperimentalMetadata"

experimental_metadata_tsv = f"{experimental_metadata_out_dir}/ExperimentalMetadata.tsv"
experimental_metadata_study_run_metadata_tsv = f"{experimental_metadata_out_dir}/ExperimentalMetadata.study_run_metadata.tsv"

# We pull all of the following from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store these in the ExperimentalMetadata directory
# to indicate that's where this version came from. This is half-normalization; will
# merge later during aggregation across (in this case) StudyRunMetadata and AliquotRunMetadata.

study_run_metadata_tsv = f"{experimental_metadata_out_dir}/StudyRunMetadata.tsv"
study_run_metadata_study_tsv = f"{experimental_metadata_out_dir}/StudyRunMetadata.study.tsv"
study_run_metadata_protocol_tsv = f"{experimental_metadata_out_dir}/StudyRunMetadata.protocol.tsv"
study_run_metadata_aliquot_run_metadata_tsv = f"{experimental_metadata_out_dir}/StudyRunMetadata.aliquot_run_metadata.tsv"
study_run_metadata_files_tsv = f"{experimental_metadata_out_dir}/StudyRunMetadata.files.tsv"

aliquot_run_metadata_tsv = f"{experimental_metadata_out_dir}/AliquotRunMetadata.tsv"
aliquot_run_metadata_aliquot_tsv = f"{experimental_metadata_out_dir}/AliquotRunMetadata.aliquot.tsv"
aliquot_run_metadata_protocol_tsv = f"{experimental_metadata_out_dir}/AliquotRunMetadata.protocol.tsv"
aliquot_run_metadata_study_tsv = f"{experimental_metadata_out_dir}/AliquotRunMetadata.study.tsv"

experimentalMetadata_json_output_file = f"{json_out_dir}/experimentalMetadata.json"

scalar_experimental_metadata_fields = (
    'experiment_type',
    'analytical_fraction',
    'instrument',
    'study_id',
    'study_submitter_id',
    'pdc_study_id'
)

scalar_study_run_metadata_fields = (
    'study_run_metadata_id',
    'study_run_metadata_submitter_id',
    'experiment_number',
    'experiment_type',
    'folder_name',
    'fraction',
    'analyte',
    'date',
    'alias',
    'replicate_number',
    'label_free',
    'itraq_113',
    'itraq_114',
    'itraq_115',
    'itraq_116',
    'itraq_117',
    'itraq_118',
    'itraq_119',
    'itraq_121',
    'tmt_126',
    'tmt_127n',
    'tmt_127c',
    'tmt_128n',
    'tmt_128c',
    'tmt_129n',
    'tmt_129c',
    'tmt_129cc',
    'tmt_130n',
    'tmt_131',
    'tmt_131c'
)

scalar_aliquot_run_metadata_fields = (
    'aliquot_run_metadata_id',
    'aliquot_submitter_id',
    'label',
    'experiment_number',
    'fraction',
    'replicate_number',
    'date',
    'alias',
    'analyte',
    'aliquot_id'
)

scalar_file_fields = (
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

pdc_study_ids = get_unique_values( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, experimental_metadata_out_dir ):
    
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
    'EXP_METADATA',
    'EXP_SRM',
    'SRM',
    'SRM_STUDY',
    'SRM_PROTOCOL',
    'SRM_ARM',
    'SRM_FILES',
    'ARM',
    'ARM_ALIQUOT',
    'ARM_PROTOCOL',
    'ARM_STUDY'
]

output_tsv_filenames = [
    experimental_metadata_tsv,
    experimental_metadata_study_run_metadata_tsv,
    study_run_metadata_tsv,
    study_run_metadata_study_tsv,
    study_run_metadata_protocol_tsv,
    study_run_metadata_aliquot_run_metadata_tsv,
    study_run_metadata_files_tsv,
    aliquot_run_metadata_tsv,
    aliquot_run_metadata_aliquot_tsv,
    aliquot_run_metadata_protocol_tsv,
    aliquot_run_metadata_study_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open(experimentalMetadata_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_experimental_metadata_fields, sep='\t', end='\n', file=output_tsvs['EXP_METADATA'] )
    print( *scalar_study_run_metadata_fields, sep='\t', end='\n', file=output_tsvs['SRM'] )
    print( *scalar_aliquot_run_metadata_fields, sep='\t', end='\n', file=output_tsvs['ARM'] )

    print( *('study_id', 'instrument', 'study_run_metadata_id'), sep='\t', end='\n', file=output_tsvs['EXP_SRM'] )
    print( *('study_run_metadata_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['SRM_STUDY'] )
    print( *('study_run_metadata_id', 'protocol_id'), sep='\t', end='\n', file=output_tsvs['SRM_PROTOCOL'] )
    print( *('study_run_metadata_id', 'aliquot_run_metadata_id'), sep='\t', end='\n', file=output_tsvs['SRM_ARM'] )
    print( *(['study_run_metadata_id'] + list(scalar_file_fields)), sep='\t', end='\n', file=output_tsvs['SRM_FILES'] )
    print( *('aliquot_run_metadata_id', 'aliquot_id'), sep='\t', end='\n', file=output_tsvs['ARM_ALIQUOT'] )
    print( *('aliquot_run_metadata_id', 'protocol_id'), sep='\t', end='\n', file=output_tsvs['ARM_PROTOCOL'] )
    print( *('aliquot_run_metadata_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['ARM_STUDY'] )

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                experimentalMetadata( ''' + f'pdc_study_id: "{pdc_study_id}", acceptDUA: true' + ''' ) {
                    ''' + '\n                    '.join(scalar_experimental_metadata_fields) + '''
                    study_run_metadata {
                        ''' + '\n                        '.join(scalar_study_run_metadata_fields) + '''
                        study {
                            study_id
                        }
                        protocol {
                            protocol_id
                        }
                        aliquot_run_metadata {
                            ''' + '\n                            '.join(scalar_aliquot_run_metadata_fields) + '''
                            aliquot {
                                aliquot_id
                            }
                            protocol {
                                protocol_id
                            }
                            study {
                                study_id
                            }
                        }
                        files {
                            ''' + '\n                            '.join(scalar_file_fields) + '''
                        }
                    }
                }
            }'''
        }

        # Multiple ExperimentalMetadata records for the same Study will repeat all
        # of that Study's StudyRunMetadata records. Don't record repeats.

        seen_SRM_IDs = set()

        # Send the experimentalMetadata() query to the API server.

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

        for experimental_metadata in result['data']['experimentalMetadata']:
            
            experimental_metadata_row = list()

            for field_name in scalar_experimental_metadata_fields:
                
                if experimental_metadata[field_name] is not None:
                    
                    experimental_metadata_row.append(experimental_metadata[field_name])

                else:
                    
                    experimental_metadata_row.append('')

            print( *experimental_metadata_row, sep='\t', end='\n', file=output_tsvs['EXP_METADATA'] )

            if experimental_metadata['study_run_metadata'] is not None and len(experimental_metadata['study_run_metadata']) > 0:
                
                for study_run_metadata in experimental_metadata['study_run_metadata']:
                    
                    # The experimentalMetadata() query associates ALL study_run_metadata records from a
                    # given Study with EACH returned ExperimentalMetadata record. This is strange, but
                    # we will nevertheless record the association output as received.

                    print( *[experimental_metadata['study_id'], experimental_metadata['instrument'], study_run_metadata['study_run_metadata_id']], sep='\t', end='\n', file=output_tsvs['EXP_SRM'] )

                    if study_run_metadata['study_run_metadata_id'] not in seen_SRM_IDs:
                        
                        seen_SRM_IDs.add(study_run_metadata['study_run_metadata_id'])

                        study_run_metadata_row = list()

                        for field_name in scalar_study_run_metadata_fields:
                            
                            if study_run_metadata[field_name] is not None:
                                
                                study_run_metadata_row.append(study_run_metadata[field_name])

                            else:
                                
                                study_run_metadata_row.append('')

                        print( *study_run_metadata_row, sep='\t', end='\n', file=output_tsvs['SRM'] )

                        if study_run_metadata['study'] is not None:
                            
                            print( *[study_run_metadata['study_run_metadata_id'], study_run_metadata['study']['study_id']], sep='\t', end='\n', file=output_tsvs['SRM_STUDY'] )

                        if study_run_metadata['protocol'] is not None:
                            
                            print( *[study_run_metadata['study_run_metadata_id'], study_run_metadata['protocol']['protocol_id']], sep='\t', end='\n', file=output_tsvs['SRM_PROTOCOL'] )

                        if study_run_metadata['aliquot_run_metadata'] is not None and len(study_run_metadata['aliquot_run_metadata']) > 0:
                            
                            for aliquot_run_metadata in study_run_metadata['aliquot_run_metadata']:
                                
                                aliquot_run_metadata_row = list()

                                for field_name in scalar_aliquot_run_metadata_fields:
                                    
                                    if aliquot_run_metadata[field_name] is not None:
                                        
                                        aliquot_run_metadata_row.append(aliquot_run_metadata[field_name])

                                    else:
                                        
                                        aliquot_run_metadata_row.append('')

                                print( *aliquot_run_metadata_row, sep='\t', end='\n', file=output_tsvs['ARM'] )

                                print( *[study_run_metadata['study_run_metadata_id'], aliquot_run_metadata['aliquot_run_metadata_id']], sep='\t', end='\n', file=output_tsvs['SRM_ARM'] )

                                if aliquot_run_metadata['aliquot'] is not None:
                                    
                                    print( *[aliquot_run_metadata['aliquot_run_metadata_id'], aliquot_run_metadata['aliquot']['aliquot_id']], sep='\t', end='\n', file=output_tsvs['ARM_ALIQUOT'] )

                                if aliquot_run_metadata['protocol'] is not None:
                                    
                                    print( *[aliquot_run_metadata['aliquot_run_metadata_id'], aliquot_run_metadata['protocol']['protocol_id']], sep='\t', end='\n', file=output_tsvs['ARM_PROTOCOL'] )

                                if aliquot_run_metadata['study'] is not None:
                                    
                                    print( *[aliquot_run_metadata['aliquot_run_metadata_id'], aliquot_run_metadata['study']['study_id']], sep='\t', end='\n', file=output_tsvs['ARM_STUDY'] )

                        if study_run_metadata['files'] is not None and len(study_run_metadata['files']) > 0:
                            
                            for file in study_run_metadata['files']:
                                
                                file_row = [study_run_metadata['study_run_metadata_id']]

                                for field_name in scalar_file_fields:
                                    
                                    if file[field_name] is not None:
                                        
                                        file_row.append(file[field_name])

                                    else:
                                        
                                        file_row.append('')

                                print( *file_row, sep='\t', end='\n', file=output_tsvs['SRM_FILES'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



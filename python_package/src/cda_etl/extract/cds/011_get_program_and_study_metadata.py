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

program_out_dir = path.join( output_root, 'program' )

program_output_tsv = path.join( program_out_dir, 'program.tsv' )

program_studies_output_tsv = path.join( program_out_dir, 'program.study_name.tsv' )

study_out_dir = path.join( output_root, 'study' )

study_output_tsv = path.join( study_out_dir, 'study.tsv' )

study_files_output_tsv = path.join( study_out_dir, 'study.file_id.tsv' )

study_participants_output_tsv = path.join( study_out_dir, 'study.participant_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

program_study_output_json = path.join( json_out_dir, 'program_and_study_metadata.json' )

# type study {
#   study_name: String
#   study_acronym: String
#   study_description: String
#   short_description: String
#   study_external_url: String
#   primary_investigator_name: String
#   primary_investigator_email: String
#   co_investigator_name: String
#   co_investigator_email: String
#   phs_accession: String
#   bioproject_accession: String
#   index_date: String
#   cds_requestor: String
#   funding_agency: String
#   funding_source_program_name: String
#   grant_id: String
#   clinical_trial_system: String
#   clinical_trial_identifier: String
#   clinical_trial_arm: String
#   organism_species: String
#   adult_or_childhood_study: String
#   data_types: String
#   file_types: String
#   data_access_level: String
#   cds_primary_bucket: String
#   cds_secondary_bucket: String
#   cds_tertiary_bucket: String
#   number_of_participants: Float
#   number_of_samples: Float
#   study_data_types: String
#   file_types_and_format: String
#   size_of_data_being_uploaded: Float
#   size_of_data_being_uploaded_unit: String
#   size_of_data_being_uploaded_original: Float
#   size_of_data_being_uploaded_original_unit: String
#   acl: String
#   study_access: String
#   program: program
#   participants(first: Int, offset: Int, orderBy: [_participantOrdering!], filter: _participantFilter): [participant]
#   files(first: Int, offset: Int, orderBy: [_fileOrdering!], filter: _fileFilter): [file]
# }
# 
#   study(study_name: String, study_acronym: String, study_description: String, short_description: String, study_external_url: String, primary_investigator_name: String, primary_investigator_email: String, co_investigator_name: String, co_investigator_email: String, phs_accession: String, bioproject_accession: String, index_date: String, cds_requestor: String, funding_agency: String, funding_source_program_name: String, grant_id: String, clinical_trial_system: String, clinical_trial_identifier: String, clinical_trial_arm: String, organism_species: String, adult_or_childhood_study: String, data_types: String, file_types: String, data_access_level: String, cds_primary_bucket: String, cds_secondary_bucket: String, cds_tertiary_bucket: String, number_of_participants: Float, number_of_samples: Float, study_data_types: String, file_types_and_format: String, size_of_data_being_uploaded: Float, size_of_data_being_uploaded_unit: String, size_of_data_being_uploaded_original: Float, size_of_data_being_uploaded_original_unit: String, acl: String, study_access: String, filter: _studyFilter, first: Int, offset: Int, orderBy: [_studyOrdering!]): [study!]!

scalar_study_fields = [
    'study_name',
    'study_acronym',
    'study_description',
    'short_description',
    'study_external_url',
    'primary_investigator_name',
    'primary_investigator_email',
    'co_investigator_name',
    'co_investigator_email',
    'phs_accession',
    'bioproject_accession',
    'index_date',
    'cds_requestor',
    'funding_agency',
    'funding_source_program_name',
    'grant_id',
    'clinical_trial_system',
    'clinical_trial_identifier',
    'clinical_trial_arm',
    'organism_species',
    'adult_or_childhood_study',
    'data_types',
    'file_types',
    'data_access_level',
    'cds_primary_bucket',
    'cds_secondary_bucket',
    'cds_tertiary_bucket',
    'number_of_participants',
    'number_of_samples',
    'study_data_types',
    'file_types_and_format',
    'size_of_data_being_uploaded',
    'size_of_data_being_uploaded_unit',
    'size_of_data_being_uploaded_original',
    'size_of_data_being_uploaded_original_unit',
    'acl',
    'study_access'
]

# type program {
#     program_name: String
#     program_acronym: String
#     program_short_description: String
#     program_full_description: String
#     program_external_url: String
#     program_sort_order: Int
#     studies(first: Int, offset: Int, orderBy: [_studyOrdering!], filter: _studyFilter): [study]
#   }
# 
# program(program_name: String, program_acronym: String, program_short_description: String, program_full_description: String, program_external_url: String, program_sort_order: Int, filter: _programFilter, first: Int, offset: Int, orderBy: [_programOrdering!]): [program!]!

scalar_program_fields = [
    'program_name',
    'program_acronym',
    'program_short_description',
    'program_full_description',
    'program_external_url',
    'program_sort_order'
]

program_api_query_json = {
    
    'query': f'''    {{
        
        program( first: {page_size}, offset: 0, orderBy: [ program_name_asc ] ) {{
            ''' + '''
            '''.join( scalar_program_fields ) + f'''
            studies( first: {page_size}, offset: 0, orderBy: [ study_name_asc ] ) {{
                ''' + '''
                '''.join( scalar_study_fields ) + f'''
            }}
        }}
    }}'''
}

study_associations_api_query_json_template = f'''    {{
        
        study( study_name: "__STUDY_NAME__", orderBy: [ study_name_asc ] ) {{
            study_name
            files( first: {page_size}, offset: __OFFSET__, orderBy: [ file_id_asc ] ) {{
                file_id
            }}
            participants( first: {page_size}, offset: __OFFSET__, orderBy: [ participant_id_asc ] ) {{
                participant_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ program_out_dir, study_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Send the query to the API server.

response = requests.post( api_url, json=program_api_query_json )

# If the HTTP response code is not OK (200), dump the query, print the
# error result and exit.

if not response.ok:
    
    print( program_api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads( response.content )

# Save a version of the returned data as raw JSON.

with open( program_study_output_json, 'w' ) as JSON:
    
    print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

# Save program data as a TSV.

with open( program_output_tsv, 'w' ) as PROGRAM, open( program_studies_output_tsv, 'w' ) as PROGRAM_STUDIES, open( study_output_tsv, 'w' ) as STUDY, open( study_files_output_tsv, 'w' ) as STUDY_FILES, open( study_participants_output_tsv, 'w' ) as STUDY_PARTICIPANTS:
    
    print( *scalar_program_fields, sep='\t', file=PROGRAM )
    print( *[ 'program_name', 'study_name' ], sep='\t', file=PROGRAM_STUDIES )
    print( *scalar_study_fields, sep='\t', file=STUDY )
    print( *[ 'study_name', 'file_id' ], sep='\t', file=STUDY_FILES )
    print( *[ 'study_name', 'participant_id' ], sep='\t', file=STUDY_PARTICIPANTS )

    seen_study_names = set()

    program_count = len( result['data']['program'] )

    if program_count >= page_size:
        
        print( f"WARNING: Global program count {program_count} equals page-size parameter ({page_size} records per page); add paging controls to prevent loss of data", file=sys.stderr )

    else:
        
        print( f"Got {program_count} program records.", file=sys.stderr )

    for program in result['data']['program']:
        
        program_row = list()

        for field_name in scalar_program_fields:
            
            # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

            field_value = json.dumps( program[field_name] ).strip( '"' )

            if field_value == 'null':
                
                field_value = ''

            program_row.append( field_value )

        print( *program_row, sep='\t', file=PROGRAM )

        # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

        program_name = json.dumps( program['program_name'] ).strip( '"' )

        study_count = len( program['studies'] )

        if study_count >= page_size:
            
            print( f"WARNING: Number of studies ({study_count}) for program '{program_name}' equals page-size parameter ({page_size} records per page); add paging controls to prevent loss of data", file=sys.stderr )

        else:
            
            print( f"    Got {study_count} study records for program '{program_name}'.", file=sys.stderr )

        for study in program['studies']:
            
            # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

            study_name = json.dumps( study['study_name'] ).strip( '"' )

            if study_name in seen_study_names:
                
                print( f"WARNING: Study name '{study_name}' encountered in multiple programs; please debug.", file=sys.stderr )

            else:
                
                seen_study_names.add( study_name )

                print( *[ program_name, study_name ], sep='\t', file=PROGRAM_STUDIES )

                study_row = list()

                for field_name in scalar_study_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( study[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    study_row.append( field_value )

                print( *study_row, sep='\t', file=STUDY )

                offset = 0

                null_result = False

                while not null_result:
                    
                    query_text = re.sub( r'__STUDY_NAME__', study_name, re.sub( r'__OFFSET__', str( offset ), study_associations_api_query_json_template ) )

                    study_associations_api_query_json = {
                        'query': query_text
                    }

                    study_response = requests.post( api_url, json=study_associations_api_query_json )

                    if not study_response.ok:
                        
                        print( study_associations_api_query_json['query'], file=sys.stderr )

                        study_response.raise_for_status()

                    study_result = json.loads( study_response.content )

                    if len( study_result['data']['study'] ) != 1:
                        
                        print( f"WARNING: Multiple studies returned for name '{study_name}'; please debug.", file=sys.stderr )

                    else:
                        
                        file_list = study_result['data']['study'][0]['files']

                        participant_list = study_result['data']['study'][0]['participants']

                        if len( file_list ) == 0 and len( participant_list ) == 0:
                            
                            null_result = True

                        else:
                            
                            for file in file_list:
                                
                                file_id = file['file_id']

                                print( *[ study_name, file_id ], sep='\t', file=STUDY_FILES )

                            for participant in participant_list:
                                
                                participant_id = participant['participant_id']

                                print( *[ study_name, participant_id ], sep='\t', file=STUDY_PARTICIPANTS )

                    offset = offset + page_size



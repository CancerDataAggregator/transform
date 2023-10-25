#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://caninecommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'icdc' )

############################################################################################################################################################

study_out_dir = path.join( output_root, 'study' )
study_output_tsv = path.join( study_out_dir, 'study.tsv' )

study_site_out_dir = path.join( output_root, 'study_site' )
study_study_site_output_tsv = path.join( study_site_out_dir, 'study_site.tsv' )

image_collection_out_dir = path.join( output_root, 'image_collection' )
study_image_collection_output_tsv = path.join( image_collection_out_dir, 'image_collection.tsv' )

publication_out_dir = path.join( output_root, 'publication' )
study_publication_output_tsv = path.join( publication_out_dir, 'publication.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )
study_program_output_tsv = path.join( relationship_validation_out_dir, 'study.program_acronym.tsv' )
study_case_output_tsv = path.join( relationship_validation_out_dir, 'study.case_id.tsv' )
study_cohort_output_tsv = path.join( relationship_validation_out_dir, 'study.cohort_id.tsv' )
study_file_output_tsv = path.join( relationship_validation_out_dir, 'study.file_uuid.tsv' )
study_principal_investigator_output_tsv = path.join( relationship_validation_out_dir, 'study.principal_investigator.tsv' )
study_study_arm_output_tsv = path.join( relationship_validation_out_dir, 'study.study_arm.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
study_output_json = path.join( json_out_dir, 'study_metadata.json' )

# type study {
#     clinical_study_designation: String
#     clinical_study_id: String
#     clinical_study_name: String
#     clinical_study_description: String
#     clinical_study_type: String
#     date_of_iacuc_approval: String
#     dates_of_conduct: String
#     accession_id: String
#     study_disposition: String
#     program {
#         # Just for validation.
#         program_acronym
#     }
#     study_sites( first: {page_size}, offset: __OFFSET__, orderBy: [ site_short_name_asc ] ) {
#         site_short_name: String
#         veterinary_medical_center: String
#         registering_institution: String
#         studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {
#             # Just for validation.
#             clinical_study_designation
#         }
#     }
#     image_collections( first: {page_size}, offset: __OFFSET__, orderBy: [ image_collection_name_asc ] ) {
#         image_collection_name: String
#         image_type_included: String
#         image_collection_url: String
#         repository_name: String
#         collection_access: String
#         study {
#             # Just for validation.
#             clinical_study_designation
#         }
#     }
#     publications( first: {page_size}, offset: __OFFSET__, orderBy: [ publication_title_asc ] ) {
#         publication_title: String
#         authorship: String
#         year_of_publication: Float
#         journal_citation: String
#         digital_object_id: String
#         pubmed_id: Float
#         study {
#             # Just for validation.
#             clinical_study_designation
#         }
#     }
#     cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {
#         # Just for validation.
#         case_id
#     }
#     cohorts( first: {page_size}, offset: __OFFSET__, orderBy: [ cohort_id_asc ] ) {
#         # Just for validation.
#         cohort_id
#     }
#     files( first: {page_size}, offset: __OFFSET__, orderBy: [ uuid_asc ] ) {
#         # Just for validation
#         uuid
#     }
#     principal_investigators( first: {page_size}, offset: __OFFSET__, orderBy: [ pi_last_name_asc, pi_first_name_asc ] ) {
#         pi_first_name: String
#         pi_last_name: String
#         pi_middle_initial: String
#         studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {
#             # Just for validation.
#             clinical_study_designation
#         }
#     }
#     study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {
#         # Just for validation.
#         arm_id
#         arm
#     }
# }

scalar_study_fields = [
    'clinical_study_designation',
    'clinical_study_id',
    'clinical_study_name',
    'clinical_study_description',
    'clinical_study_type',
    'date_of_iacuc_approval',
    'dates_of_conduct',
    'accession_id',
    'study_disposition'
]

# type study_site {
#     site_short_name: String
#     veterinary_medical_center: String
#     registering_institution: String
#     studies(first: Int, offset: Int, orderBy: [_studyOrdering!], filter: _studyFilter): [study]
# }

scalar_study_site_fields = [
    'site_short_name',
    'veterinary_medical_center',
    'registering_institution'
]

# type image_collection {
#     image_collection_name: String
#     image_type_included: String
#     image_collection_url: String
#     repository_name: String
#     collection_access: String
#     study: study
# }

scalar_image_collection_fields = [
    'image_collection_name',
    'image_type_included',
    'image_collection_url',
    'repository_name',
    'collection_access'
]

# type publication {
#     publication_title: String
#     authorship: String
#     year_of_publication: Float
#     journal_citation: String
#     digital_object_id: String
#     pubmed_id: Float
#     study: study
# }

scalar_publication_fields = [
    'publication_title',
    'authorship',
    'year_of_publication',
    'journal_citation',
    'digital_object_id',
    'pubmed_id'
]

# type principal_investigator {
#     pi_first_name: String
#     pi_last_name: String
#     pi_middle_initial: String
#     studies(first: Int, offset: Int, orderBy: [_studyOrdering!], filter: _studyFilter): [study]
# }

scalar_principal_investigator_fields = [
    'pi_first_name',
    'pi_last_name',
    'pi_middle_initial'
]

# study(clinical_study_id: String, clinical_study_designation: String, clinical_study_name: String, clinical_study_description: String, clinical_study_type: String, date_of_iacuc_approval: String, dates_of_conduct: String, accession_id: String, study_disposition: String, filter: _studyFilter, first: Int, offset: Int, orderBy: [_studyOrdering!]): [study!]!

study_api_query_json_template = f'''    {{
        
        study( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {{
            ''' + '''
            '''.join( scalar_study_fields ) + f'''
            program {{
                program_acronym
            }}
            study_sites( first: {page_size}, offset: __OFFSET__, orderBy: [ site_short_name_asc ] ) {{
                ''' + '''
                '''.join( scalar_study_site_fields ) + f'''
                studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {{
                    clinical_study_designation
                }}
            }}
            image_collections( first: {page_size}, offset: __OFFSET__, orderBy: [ image_collection_name_asc ] ) {{
                ''' + '''
                '''.join( scalar_image_collection_fields ) + f'''
                study {{
                    clinical_study_designation
                }}
            }}
            publications( first: {page_size}, offset: __OFFSET__, orderBy: [ publication_title_asc ] ) {{
                ''' + '''
                '''.join( scalar_publication_fields ) + f'''
                study {{
                    clinical_study_designation
                }}
            }}
            cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {{
                case_id
            }}
            cohorts( first: {page_size}, offset: __OFFSET__, orderBy: [ cohort_id_asc ] ) {{
                cohort_id
            }}
            files( first: {page_size}, offset: __OFFSET__, orderBy: [ uuid_asc ] ) {{
                uuid
            }}
            principal_investigators( first: {page_size}, offset: __OFFSET__, orderBy: [ pi_last_name_asc, pi_first_name_asc ] ) {{
                ''' + '''
                '''.join( scalar_principal_investigator_fields ) + f'''
                studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {{
                    clinical_study_designation
                }}
            }}
            study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {{
                arm_id
                arm
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ study_out_dir, study_site_out_dir, image_collection_out_dir, publication_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( study_output_json, 'w' ) as JSON, \
    open( study_output_tsv, 'w' ) as STUDY, \
    open( study_program_output_tsv, 'w' ) as STUDY_PROGRAM, \
    open( study_study_site_output_tsv, 'w' ) as STUDY_STUDY_SITE, \
    open( study_image_collection_output_tsv, 'w' ) as STUDY_IMAGE_COLLECTION, \
    open( study_publication_output_tsv, 'w' ) as STUDY_PUBLICATION, \
    open( study_case_output_tsv, 'w' ) as STUDY_CASE, \
    open( study_cohort_output_tsv, 'w' ) as STUDY_COHORT, \
    open( study_file_output_tsv, 'w' ) as STUDY_FILE, \
    open( study_principal_investigator_output_tsv, 'w' ) as STUDY_PRINCIPAL_INVESTIGATOR, \
    open( study_study_arm_output_tsv, 'w' ) as STUDY_STUDY_ARM:

    print( *scalar_study_fields, sep='\t', file=STUDY )
    print( *[ 'clinical_study_designation', 'program_acronym' ], sep='\t', file=STUDY_PROGRAM )
    print( *( [ 'clinical_study_designation' ] + scalar_study_site_fields ), sep='\t', file=STUDY_STUDY_SITE )
    print( *( [ 'clinical_study_designation' ] + scalar_image_collection_fields ), sep='\t', file=STUDY_IMAGE_COLLECTION )
    print( *( [ 'clinical_study_designation' ] + scalar_publication_fields ), sep='\t', file=STUDY_PUBLICATION )
    print( *[ 'clinical_study_designation', 'case_id' ], sep='\t', file=STUDY_CASE )
    print( *[ 'clinical_study_designation', 'cohort_id' ], sep='\t', file=STUDY_COHORT )
    print( *[ 'clinical_study_designation', 'file.uuid' ], sep='\t', file=STUDY_FILE )
    print( *( [ 'clinical_study_designation' ] + scalar_principal_investigator_fields ), sep='\t', file=STUDY_PRINCIPAL_INVESTIGATOR )
    print( *[ 'clinical_study_designation', 'arm_id', 'arm' ], sep='\t', file=STUDY_STUDY_ARM )

    while not null_result:
    
        study_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), study_api_query_json_template )
        }

        study_response = requests.post( api_url, json=study_api_query_json )

        if not study_response.ok:
            
            print( study_api_query_json['query'], file=sys.stderr )

            study_response.raise_for_status()

        study_result = json.loads( study_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( study_result, indent=4, sort_keys=False ), file=JSON )

        study_count = len( study_result['data']['study'] )

        if study_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for study in study_result['data']['study']:
                
                clinical_study_designation = study['clinical_study_designation']

                study_row = list()

                for field_name in scalar_study_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( study[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    study_row.append( field_value )

                print( *study_row, sep='\t', file=STUDY )

                if 'program' in study and study['program'] is not None and study['program']['program_acronym'] is not None:
                    
                    print( *[ clinical_study_designation, study['program']['program_acronym'] ], sep='\t', file=STUDY_PROGRAM )

                for study_site in study['study_sites']:
                    
                    study_site_row = [ clinical_study_designation ]

                    for field_name in scalar_study_site_fields:
                        
                        field_value = ''

                        if field_name in study_site and study_site[field_name] is not None and study_site[field_name] != '':
                            
                            field_value = json.dumps( study_site[field_name] ).strip( '"' )

                        study_site_row.append( field_value )

                    sanity_check_passed = False

                    if 'studies' in study_site and study_site['studies'] is not None and len( study_site['studies'] ) > 0:
                        
                        for sub_study in study_site['studies']:
                            
                            if sub_study['clinical_study_designation'] == clinical_study_designation:
                                
                                sanity_check_passed = True

                    if not sanity_check_passed:
                        
                        print( f"WARNING: study_site \"{study_site['site_short_name']}\" doesn't have a study sub-object with an ID matching \"{clinical_study_designation}\" -- please investigate", file=sys.stderr )

                    print( *study_site_row, sep='\t', file=STUDY_STUDY_SITE )

                study_site_count = len( study['study_sites'] )

                if study_site_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} study_site records: implement paging here or risk data loss.", file=sys.stderr )

                for image_collection in study['image_collections']:
                    
                    image_collection_row = [ clinical_study_designation ]

                    for field_name in scalar_image_collection_fields:
                        
                        field_value = ''

                        if field_name in image_collection and image_collection[field_name] is not None and image_collection[field_name] != '':
                            
                            field_value = json.dumps( image_collection[field_name] ).strip( '"' )

                        image_collection_row.append( field_value )

                    sanity_check_passed = False

                    if 'study' in image_collection and image_collection['study'] is not None and image_collection['study']['clinical_study_designation'] == clinical_study_designation:
                        
                        sanity_check_passed = True

                    if not sanity_check_passed:
                        
                        print( f"WARNING: image_collection \"{image_collection['image_collection_name']}\" doesn't have a study sub-object with an ID matching \"{clinical_study_designation}\" -- please investigate", file=sys.stderr )

                    print( *image_collection_row, sep='\t', file=STUDY_IMAGE_COLLECTION )

                image_collection_count = len( study['image_collections'] )

                if image_collection_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} image_collection records: implement paging here or risk data loss.", file=sys.stderr )

                for publication in study['publications']:
                    
                    publication_row = [ clinical_study_designation ]

                    for field_name in scalar_publication_fields:
                        
                        field_value = ''

                        if field_name in publication and publication[field_name] is not None and publication[field_name] != '':
                            
                            field_value = json.dumps( publication[field_name] ).strip( '"' )

                        publication_row.append( field_value )

                    sanity_check_passed = False

                    if 'study' in publication and publication['study'] is not None and publication['study']['clinical_study_designation'] == clinical_study_designation:
                        
                        sanity_check_passed = True

                    if not sanity_check_passed:
                        
                        print( f"WARNING: publication \"{publication['publication_title']}\" doesn't have a study sub-object with an ID matching \"{clinical_study_designation}\" -- please investigate", file=sys.stderr )

                    print( *publication_row, sep='\t', file=STUDY_PUBLICATION )

                publication_count = len( study['publications'] )

                if publication_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} publication records: implement paging here or risk data loss.", file=sys.stderr )

                for case in study['cases']:
                    
                    print( *[ clinical_study_designation, case['case_id'] ], sep='\t', file=STUDY_CASE )

                case_count = len( study['cases'] )

                if case_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} case records: implement paging here or risk data loss.", file=sys.stderr )

                for cohort in study['cohorts']:
                    
                    cohort_id = ''

                    # This shouldn't happen, but it does.

                    if 'cohort_id' in cohort and cohort['cohort_id'] is not None and cohort['cohort_id'] != '':
                        
                        cohort_id = cohort['cohort_id']

                    print( *[ clinical_study_designation, cohort_id ], sep='\t', file=STUDY_COHORT )

                cohort_count = len( study['cohorts'] )

                if cohort_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} cohort records: implement paging here or risk data loss.", file=sys.stderr )

                for file in study['files']:
                    
                    print( *[ clinical_study_designation, file['uuid'] ], sep='\t', file=STUDY_FILE )

                file_count = len( study['files'] )

                if file_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} file records: implement paging here or risk data loss.", file=sys.stderr )

                for principal_investigator in study['principal_investigators']:
                    
                    principal_investigator_row = [ clinical_study_designation ]

                    for field_name in scalar_principal_investigator_fields:
                        
                        field_value = ''

                        if field_name in principal_investigator and principal_investigator[field_name] is not None and principal_investigator[field_name] != '':
                            
                            field_value = json.dumps( principal_investigator[field_name] ).strip( '"' )

                        principal_investigator_row.append( field_value )

                    sanity_check_passed = False

                    if 'studies' in principal_investigator and principal_investigator['studies'] is not None and len( principal_investigator['studies'] ) > 0:
                        
                        for sub_study in principal_investigator['studies']:
                            
                            if sub_study['clinical_study_designation'] == clinical_study_designation:
                                
                                sanity_check_passed = True

                    if not sanity_check_passed:
                        
                        if principal_investigator['pi_middle_initial'] is not None:
                            
                            print( f"WARNING: principal_investigator \"{principal_investigator['pi_first_name']} {principal_investigator['pi_middle_initial']} {principal_investigator['pi_last_name']}\" doesn't have a study sub-object with an ID matching \"{clinical_study_designation}\" -- please investigate", file=sys.stderr )

                        else:
                            
                            print( f"WARNING: principal_investigator \"{principal_investigator['pi_first_name']} {principal_investigator['pi_last_name']}\" doesn't have a study sub-object with an ID matching \"{clinical_study_designation}\" -- please investigate", file=sys.stderr )

                    print( *principal_investigator_row, sep='\t', file=STUDY_PRINCIPAL_INVESTIGATOR )

                principal_investigator_count = len( study['principal_investigators'] )

                if principal_investigator_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} principal_investigator records: implement paging here or risk data loss.", file=sys.stderr )

                for study_arm in study['study_arms']:
                    
                    arm_id = study_arm['arm_id']

                    arm = study_arm['arm']

                    # This shouldn't happen, but it does.

                    if arm_id is None:
                        
                        arm_id = ''

                    if arm is None:
                        
                        arm = ''

                    print( *[ clinical_study_designation, arm_id, arm ], sep='\t', file=STUDY_STUDY_ARM )

                study_arm_count = len( study['study_arms'] )

                if study_arm_count == page_size:
                    
                    print( f"WARNING: Study {clinical_study_designation} has at least {page_size} study_arm records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size



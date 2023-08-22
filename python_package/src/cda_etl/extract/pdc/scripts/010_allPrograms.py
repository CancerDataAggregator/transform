#!/usr/bin/env python -u

import re
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

program_out_dir = f"{output_root}/Program"
project_out_dir = f"{output_root}/Project"
study_out_dir = f"{output_root}/Study"

program_tsv = f"{program_out_dir}/Program.tsv"
program_projects_tsv = f"{program_out_dir}/Program.projects.tsv"

project_tsv = f"{project_out_dir}/Project.tsv"
project_studies_tsv = f"{project_out_dir}/Project.studies.tsv"
project_cases_tsv = f"{project_out_dir}/Project.cases.tsv"
project_program_tsv = f"{project_out_dir}/Project.program.tsv"

study_tsv = f"{study_out_dir}/Study.tsv"
study_files_count_tsv = f"{study_out_dir}/Study.filesCount.tsv"
study_disease_types_tsv = f"{study_out_dir}/Study.disease_types.tsv"
study_primary_sites_tsv = f"{study_out_dir}/Study.primary_sites.tsv"
study_contacts_tsv = f"{study_out_dir}/Study.contacts.tsv"
study_cases_tsv = f"{study_out_dir}/Study.cases.tsv"
study_diagnoses_tsv = f"{study_out_dir}/Study.diagnoses.tsv"
study_files_tsv = f"{study_out_dir}/Study.files.tsv"
study_project_tsv = f"{study_out_dir}/Study.project.tsv"
study_study_run_metadata_tsv = f"{study_out_dir}/Study.study_run_metadata.tsv"

# We pull these from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store this in the Study directory
# to indicate that's where it came from. This is half-normalization; will
# merge later during aggregation across (in this case) Study and UIStudy.

contact_tsv = f"{study_out_dir}/Contact.tsv"

allPrograms_json_output_file = f"{json_out_dir}/allPrograms.json"

study_names_json_output_file = f"{json_out_dir}/study.study_name.json"

scalar_program_fields = (
    'program_id',
    'program_submitter_id',
    'name',
    'sponsor',
    'start_date',
    'end_date',
    'program_manager',
    'data_size_TB',
    'project_count',
    'study_count',
    'data_file_count'
)

scalar_project_fields = (
    'project_id',
    'project_submitter_id',
    'name'
)

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

scalar_contact_fields = (
    'contact_id',
    'name',
    'institution',
    'email',
    'url'
)

scalar_study_files_count_fields = (
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

api_query_json = {
    'query': '''    {
        allPrograms( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_program_fields) + '''
            projects {
                ''' + '\n                '.join(scalar_project_fields) + '''
                studies {
                    ''' + '\n                    '.join(scalar_study_fields) + '''
                    disease_types
                    primary_sites
                    contacts {
                        ''' + '\n                        '.join(scalar_contact_fields) + '''
                    }
                    filesCount {
                        ''' + '\n                        '.join(scalar_study_files_count_fields) + '''
                    }
                    cases {
                        case_id
                    }
                    diagnoses {
                        diagnosis_id
                    }
                    files {
                        file_id
                    }
                    project {
                        project_id
                    }
                    study_run_metadata {
                        study_run_metadata_id
                    }
                }
                cases {
                    case_id
                }
                program {
                    program_id
                }
            }
        }
    }'''
}

study_name_query_json_template = '''    {
        study( study_id: "__STUDY_ID__" ) {
            study_name
        }
    }'''

# EXECUTION

for output_dir in ( json_out_dir, program_out_dir, project_out_dir, study_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the allPrograms() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as raw JSON.

with open( allPrograms_json_output_file, 'w' ) as JSON:
    
    print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

# Open handles for output files to save TSVs describing:
# 
#     * the returned Program, Project, Study, Contact, and Study.filesCount (File) objects
#     * association TSVs enumerating containment relationships between objects and sub-objects (e.g. Program->Project)
#     * association TSVs enumerating relationships between objects and keyword-style dictionaries (e.g. Study->disease_type)
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

output_tsv_keywords = (
    'PROGRAM',
    'PROJECT',
    'STUDY',
    'CONTACT',
    'STUDY_FILES_COUNT',
    'PROGRAM_PROJECTS',
    'PROJECT_STUDIES',
    'PROJECT_CASES',
    'PROJECT_PROGRAM',
    'STUDY_CONTACTS',
    'STUDY_CASES',
    'STUDY_DIAGNOSES',
    'STUDY_FILES',
    'STUDY_PROJECT',
    'STUDY_STUDY_RUN_METADATA',
    'STUDY_DISEASE_TYPES',
    'STUDY_PRIMARY_SITES',
    'STUDY_NAMES_JSON'
)

output_tsv_filenames = (
    program_tsv,
    project_tsv,
    study_tsv,
    contact_tsv,
    study_files_count_tsv,
    program_projects_tsv,
    project_studies_tsv,
    project_cases_tsv,
    project_program_tsv,
    study_contacts_tsv,
    study_cases_tsv,
    study_diagnoses_tsv,
    study_files_tsv,
    study_project_tsv,
    study_study_run_metadata_tsv,
    study_disease_types_tsv,
    study_primary_sites_tsv,
    study_names_json_output_file
)

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_program_fields, sep='\t', end='\n', file=output_tsvs['PROGRAM'] )
print( *scalar_project_fields, sep='\t', end='\n', file=output_tsvs['PROJECT'] )
print( *scalar_study_fields, sep='\t', end='\n', file=output_tsvs['STUDY'] )
print( *scalar_contact_fields, sep='\t', end='\n', file=output_tsvs['CONTACT'] )

# We fully qualify "Study.study_id" here instead of just giving "study_id" because the File objects enumerated in Study.filesCount already have their own "study_id" field.

print( *(['Study.study_id'] + list(scalar_study_files_count_fields)), sep='\t', end='\n', file=output_tsvs['STUDY_FILES_COUNT'] )

print( *('program_id', 'project_id'), sep='\t', end='\n', file=output_tsvs['PROGRAM_PROJECTS'] )

print( *('project_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['PROJECT_STUDIES'] )
print( *('project_id', 'case_id'), sep='\t', end='\n', file=output_tsvs['PROJECT_CASES'] )
print( *('project_id', 'program_id'), sep='\t', end='\n', file=output_tsvs['PROJECT_PROGRAM'] )

print( *('study_id', 'contact_id'), sep='\t', end='\n', file=output_tsvs['STUDY_CONTACTS'] )
print( *('study_id', 'case_id'), sep='\t', end='\n', file=output_tsvs['STUDY_CASES'] )
print( *('study_id', 'diagnosis_id'), sep='\t', end='\n', file=output_tsvs['STUDY_DIAGNOSES'] )
print( *('study_id', 'file_id'), sep='\t', end='\n', file=output_tsvs['STUDY_FILES'] )
print( *('study_id', 'project_id'), sep='\t', end='\n', file=output_tsvs['STUDY_PROJECT'] )
print( *('study_id', 'study_run_metadata_id'), sep='\t', end='\n', file=output_tsvs['STUDY_STUDY_RUN_METADATA'] )

print( *('study_id', 'disease_type'), sep='\t', end='\n', file=output_tsvs['STUDY_DISEASE_TYPES'] )
print( *('study_id', 'primary_site'), sep='\t', end='\n', file=output_tsvs['STUDY_PRIMARY_SITES'] )

# Parse the returned data and save to TSV.

for program in result['data']['allPrograms']:
    
    program_row = list()

    for field_name in scalar_program_fields:
        
        if program[field_name] is not None:
            
            program_row.append(program[field_name])

        else:
            
            program_row.append('')

    print( *program_row, sep='\t', end='\n', file=output_tsvs['PROGRAM'] )

    if program['projects'] is not None and len(program['projects']) > 0:
        
        for project in program['projects']:
            
            project_row = list()

            for field_name in scalar_project_fields:
                
                if project[field_name] is not None:
                    
                    project_row.append(project[field_name])

                else:
                    
                    project_row.append('')

            print( *project_row, sep='\t', end='\n', file=output_tsvs['PROJECT'] )

            print( *( program['program_id'], project['project_id'] ), sep='\t', end='\n', file=output_tsvs['PROGRAM_PROJECTS'] )

            if project['cases'] is not None and len(project['cases']) > 0:
                
                for case in project['cases']:
                    
                    print( *(project['project_id'], case['case_id']), sep='\t', end='\n', file=output_tsvs['PROJECT_CASES'] )

            if project['program'] is not None:
                
                print( *(project['project_id'], project['program']['program_id']), sep='\t', end='\n', file=output_tsvs['PROJECT_PROGRAM'] )

            if project['studies'] is not None and len(project['studies']) > 0:
                
                for study in project['studies']:
                    
                    study_row = list()

                    for field_name in scalar_study_fields:
                        
                        # At time of writing, allPrograms() reports incorrect study_name values and must be corrected by a separate call to study().

                        if field_name != 'study_name' and study[field_name] is not None:
                            
                            study_row.append(study[field_name])

                        elif field_name == 'study_name':
                            
                            study_subquery = re.sub( r'__STUDY_ID__', study['study_id'], study_name_query_json_template )

                            subquery_json = {
                                
                                'query': study_subquery
                            }

                            subquery_response = requests.post( api_url, json=subquery_json )

                            if not subquery_response.ok:
                                
                                print( subquery_json['query'], file=sys.stderr )

                                subquery_response.raise_for_status()

                            subquery_result = json.loads( subquery_response.content )

                            print( json.dumps( subquery_result, indent=4, sort_keys=False ), file=output_tsvs['STUDY_NAMES_JSON'] )

                            study_name = ''

                            for subquery_study in subquery_result['data']['study']:
                                
                                if subquery_study['study_name'] is not None and subquery_study['study_name'] != '':
                                    
                                    study_name = subquery_study['study_name']

                            study_row.append( study_name )

                        else:
                            
                            study_row.append( '' )

                    print( *study_row, sep='\t', end='\n', file=output_tsvs['STUDY'] )

                    print( *( project['project_id'], study['study_id'] ), sep='\t', end='\n', file=output_tsvs['PROJECT_STUDIES'] )

                    if study['cases'] is not None and len(study['cases']) > 0:
                        
                        for case in study['cases']:
                            
                            print( *(study['study_id'], case['case_id']), sep='\t', end='\n', file=output_tsvs['STUDY_CASES'] )

                    if study['diagnoses'] is not None and len(study['diagnoses']) > 0:
                        
                        for diagnosis in study['diagnoses']:
                            
                            print( *(study['study_id'], diagnosis['diagnosis_id']), sep='\t', end='\n', file=output_tsvs['STUDY_DIAGNOSES'] )

                    if study['files'] is not None and len(study['files']) > 0:
                        
                        for file in study['files']:
                            
                            print( *(study['study_id'], file['file_id']), sep='\t', end='\n', file=output_tsvs['STUDY_FILES'] )

                    if study['project'] is not None:
                        
                        print( *(study['study_id'], study['project']['project_id']), sep='\t', end='\n', file=output_tsvs['STUDY_PROJECT'] )

                    if study['study_run_metadata'] is not None and len(study['study_run_metadata']) > 0:
                        
                        for study_run_metadata in study['study_run_metadata']:
                            
                            print( *(study['study_id'], study_run_metadata['study_run_metadata_id']), sep='\t', end='\n', file=output_tsvs['STUDY_STUDY_RUN_METADATA'] )

                    if study['contacts'] is not None and len(study['contacts']) > 0:
                        
                        for contact in study['contacts']:
                            
                            contact_row = list()

                            for field_name in scalar_contact_fields:
                                
                                if contact[field_name] is not None:
                                    
                                    contact_row.append(contact[field_name])

                                else:
                                    
                                    contact_row.append('')

                            print( *contact_row, sep='\t', end='\n', file=output_tsvs['CONTACT'] )

                            print( *(study['study_id'], contact['contact_id']), sep='\t', end='\n', file=output_tsvs['STUDY_CONTACTS'] )

                    if study['filesCount'] is not None and len(study['filesCount']) > 0:
                        
                        for files_count in study['filesCount']:
                            
                            files_count_row = [study['study_id']]

                            for field_name in scalar_study_files_count_fields:
                                
                                if files_count[field_name] is not None:
                                    
                                    files_count_row.append(files_count[field_name])

                                else:
                                    
                                    files_count_row.append('')

                            print( *files_count_row, sep='\t', end='\n', file=output_tsvs['STUDY_FILES_COUNT'] )

                    if study['disease_types'] is not None and len(study['disease_types']) > 0:
                        
                        for disease_type in study['disease_types']:
                            
                            print( *(study['study_id'], disease_type), sep='\t', end='\n', file=output_tsvs['STUDY_DISEASE_TYPES'] )

                    if study['primary_sites'] is not None and len(study['primary_sites']) > 0:
                        
                        for primary_site in study['primary_sites']:
                            
                            print( *(study['study_id'], primary_site), sep='\t', end='\n', file=output_tsvs['STUDY_PRIMARY_SITES'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )



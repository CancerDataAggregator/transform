#!/usr/bin/env python3

import re

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

# PARAMETERS

input_dir = path.join( 'extracted_data', 'pdc_postprocessed' )

input_data = {
    
    'program' : load_tsv_as_dict( path.join( input_dir, 'Program', 'Program.tsv' ) ),
    'project' : load_tsv_as_dict( path.join( input_dir, 'Project', 'Project.tsv' ) ),
    'study' : load_tsv_as_dict( path.join( input_dir, 'Study', 'Study.tsv' ) ),
    'project_in_program' : map_columns_one_to_many( path.join( input_dir, 'Program', 'Program.project_id.tsv' ), 'program_id', 'project_id' ),
    'study_in_project' : map_columns_one_to_many( path.join( input_dir, 'Project', 'Project.study_id.tsv' ), 'project_id', 'study_id' ),
    'study_disease_types' : map_columns_one_to_many( path.join( input_dir, 'Study', 'Study.disease_type.tsv' ), 'study_id', 'disease_type' ),
    'study_primary_sites' : map_columns_one_to_many( path.join( input_dir, 'Study', 'Study.primary_site.tsv' ), 'study_id', 'primary_site' ),
    'study_dbgap_id' : map_columns_one_to_one( path.join( input_dir, 'Study', 'Study.dbgap_id.tsv' ), 'study_id', 'dbgap_id' )
}

output_dir = path.join( 'auxiliary_metadata', '__PDC_supplemental_metadata' )

output_file = path.join( output_dir, 'PDC_all_programs_projects_and_studies.tsv' )

output_fields = [
    
    'program.program_id',
    'program.program_submitter_id',
    'program.name',
    'program.program_manager',

    'project.project_id',
    'project.project_submitter_id',
    'project.name',

    'study.study_id',
    'study.study_submitter_id',
    'study.pdc_study_id',
    'study.dbgap_id',
    'study.submitter_id_name',
    'study.study_name',
    'study.study_shortname',
    'study.disease_types_studied',
    'study.primary_sites_studied'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

with open( output_file, 'w' ) as OUT:
    
    print( *output_fields, sep='\t', end='\n', file=OUT )

    for program_id in sorted( input_data['program'] ):
        
        program_submitter_id = input_data['program'][program_id]['program_submitter_id']
        program_name = input_data['program'][program_id]['name']
        program_manager = input_data['program'][program_id]['program_manager']

        for project_id in sorted( input_data['project_in_program'][program_id] ):
            
            project_submitter_id = input_data['project'][project_id]['project_submitter_id']
            project_name = input_data['project'][project_id]['name']

            if project_id not in input_data['study_in_project']:
                
                print( *[ program_id, program_submitter_id, program_name, program_manager, project_id, project_submitter_id, project_name, '', '', '', '', '', '', '', '', '' ], sep='\t', end='\n', file=OUT )

            else:
                
                for study_id in sorted( input_data['study_in_project'][project_id] ):
                    
                    study_submitter_id = input_data['study'][study_id]['study_submitter_id']
                    pdc_study_id = input_data['study'][study_id]['pdc_study_id']
                    submitter_id_name = input_data['study'][study_id]['submitter_id_name']
                    study_name = input_data['study'][study_id]['study_name']
                    study_shortname = input_data['study'][study_id]['study_shortname']

                    dbgap_id = ''

                    if study_id in input_data['study_dbgap_id']:
                        
                        dbgap_id = input_data['study_dbgap_id'][study_id]

                    disease_type_string = '[]'

                    if study_id in input_data['study_disease_types']:
                        
                        disease_types = list()

                        for disease_type in sorted( input_data['study_disease_types'][study_id] ):
                            
                            disease_type = re.sub( r',', r'\,', disease_type )
                            disease_type = re.sub( r"'", r"\'", disease_type )

                            disease_types.append( disease_type )

                        disease_type_string = r"['" + "', '".join( disease_types ) + r"']"

                    primary_site_string = '[]'

                    if study_id in input_data['study_primary_sites']:
                        
                        primary_sites = list()

                        for primary_site in sorted( input_data['study_primary_sites'][study_id] ):
                            
                            primary_site = re.sub( r',', r'\,', primary_site )
                            primary_site = re.sub( r"'", r"\'", primary_site )

                            primary_sites.append( primary_site )

                        primary_site_string = r"['" + "', '".join( primary_sites ) + r"']"

                    print( *[ program_id, program_submitter_id, program_name, program_manager, \
                                project_id, project_submitter_id, project_name, \
                                study_id, study_submitter_id, pdc_study_id, dbgap_id, submitter_id_name, study_name, study_shortname, disease_type_string, primary_site_string ], sep='\t', end='\n', file=OUT )



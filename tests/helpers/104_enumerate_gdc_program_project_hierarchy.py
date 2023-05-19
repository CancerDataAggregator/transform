#!/usr/bin/env python3

import re

from os import path

from etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

# PARAMETERS

input_dir = path.join( 'extracted_data', 'gdc', 'projects', 'tsv' )

input_data = {
    
    'program' : load_tsv_as_dict( path.join( input_dir, 'program.tsv' ) ),
    'project' : load_tsv_as_dict( path.join( input_dir, 'project.tsv' ) ),
    'project_in_program' : map_columns_one_to_many( path.join( input_dir, 'project_in_program.tsv' ), 'program_id', 'project_id' ),
    'project_studies_disease_type' : map_columns_one_to_many( path.join( input_dir, 'project_studies_disease_type.tsv' ), 'project_id', 'disease_type' ),
    'project_studies_primary_site' : map_columns_one_to_many( path.join( input_dir, 'project_studies_primary_site.tsv' ), 'project_id', 'primary_site' )
}

output_dir = path.join( 'extracted_data', '__supplemental_metadata' )

output_file = path.join( output_dir, 'GDC_all_programs_and_projects.tsv' )

output_fields = [
    
    'program.program_id',
    'program.name',
    'program.dbgap_accession_number',
    'project.project_id',
    'project.name',
    'project.dbgap_accession_number',
    'project.disease_types_studied',
    'project.primary_sites_studied'
]

# EXECUTION

with open( output_file, 'w' ) as OUT:
    
    print( *output_fields, sep='\t', end='\n', file=OUT )

    for program_id in sorted( input_data['program'] ):
        
        program_name = input_data['program'][program_id]['name']
        program_dbgap_accession_number = input_data['program'][program_id]['dbgap_accession_number']

        for project_id in sorted( input_data['project_in_program'][program_id] ):
            
            project_name = input_data['project'][project_id]['name']
            project_dbgap_accession_number = input_data['project'][project_id]['dbgap_accession_number']

            disease_type_string = '[]'

            if project_id in input_data['project_studies_disease_type']:
                
                disease_types = list()

                for disease_type in sorted( input_data['project_studies_disease_type'][project_id] ):
                    
                    disease_type = re.sub( r',', r'\,', disease_type )
                    disease_type = re.sub( r"'", r"\'", disease_type )

                    disease_types.append( disease_type )

                disease_type_string = r"['" + "', '".join( disease_types ) + r"']"

            primary_site_string = '[]'

            if project_id in input_data['project_studies_primary_site']:
                    
                primary_sites = list()

                for primary_site in sorted( input_data['project_studies_primary_site'][project_id] ):
                    
                    primary_site = re.sub( r',', r'\,', primary_site )
                    primary_site = re.sub( r"'", r"\'", primary_site )

                    primary_sites.append( primary_site )

                primary_site_string = r"['" + "', '".join( primary_sites ) + r"']"

            print( *[ program_id, program_name, program_dbgap_accession_number, project_id, project_name, project_dbgap_accession_number, disease_type_string, primary_site_string ], sep='\t', end='\n', file=OUT )



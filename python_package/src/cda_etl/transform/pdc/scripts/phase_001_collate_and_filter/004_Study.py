#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Study' )

study_input_tsv = path.join( input_dir, 'Study.tsv' )

study_disease_types_input_tsv = path.join( input_dir, 'Study.disease_types.tsv' )

study_primary_sites_input_tsv = path.join( input_dir, 'Study.primary_sites.tsv' )

study_catalog_input_tsv = path.join( input_root, 'StudyCatalog', 'Study.tsv' )

experimental_metadata_to_study_run_metadata_tsv = path.join( input_root, 'ExperimentalMetadata', 'ExperimentalMetadata.study_run_metadata.tsv' )

uistudy_tsv = path.join( input_root, 'UIStudy', 'UIStudy.tsv' )

output_root = 'extracted_data/pdc_postprocessed'

output_dir = path.join( output_root, 'Study' )

study_output_tsv = path.join( output_dir, 'Study.tsv' )

study_instrument_output_tsv = path.join( output_dir, 'Study.instrument.tsv' )

study_disease_type_output_tsv = path.join( output_dir, 'Study.disease_type.tsv' )

study_primary_site_output_tsv = path.join( output_dir, 'Study.primary_site.tsv' )

input_files_to_scan = {
    'Study': study_input_tsv,
    'StudyCatalog': study_catalog_input_tsv
}

fields_to_ignore = {
    'Study': {
        'disease_type',
        'primary_site',
        'cases_count',
        'aliquots_count',
        'program_id',
        'program_name',
        'project_id',
        'project_submitter_id',
        'project_name'
    },
    'StudyCatalog': {
        'disease_type',
        'primary_site',
        'cases_count',
        'aliquots_count',
        'program_id',
        'program_name',
        'project_id',
        'project_submitter_id',
        'project_name'
    }
}

values_to_ignore = {
}

processing_order = [
    'Study',
    'StudyCatalog'
]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load the main Study data.

studies = dict()

study_fields = list()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            # We ignore every StudyCatalog record whose study_id was not first loaded
            # from Study.tsv via allPrograms() (because we don't need to track obsolete Study versions).

            if current_id not in studies and input_file_label != 'StudyCatalog':
                
                studies[current_id] = dict()

            if current_id in studies:
                
                if id_field_name not in study_fields:
                    
                    study_fields.append( id_field_name )

                for i in range( 1, len( column_names ) ):
                    
                    current_column_name = column_names[i]

                    current_value = values[i]

                    if current_column_name not in fields_to_ignore[input_file_label]:
                        
                        if current_column_name not in study_fields:
                            
                            study_fields.append( current_column_name )

                        # If we scanned this before,

                        if current_column_name in studies[current_id]:
                            
                            # If the current value isn't null or set to be skipped,

                            if current_value != '' and current_value not in values_to_ignore:
                                
                                # If the old value isn't null,

                                if studies[current_id][current_column_name] != '':
                                    
                                    # Make sure the two values match.

                                    if current_value != studies[current_id][current_column_name]:
                                        
                                        sys.exit(f"FATAL: Multiple non-null values ('{studies[current_id][current_column_name]}', '{current_value}') seen for field '{current_column_name}', {id_field_name} '{current_id}'. Aborting.\n")

                                # If the old value IS null,

                                else:
                                    
                                    # Overwrite it with the new one.

                                    studies[current_id][current_column_name] = current_value

                            # ( If we've scanned this before and the current value is null, do nothing. )

                        else:
                            
                            # If we haven't scanned this yet,

                            if current_value != '' and current_value not in values_to_ignore:
                                
                                # If the current value isn't null or set to be skipped, record it.

                                studies[current_id][current_column_name] = current_value

                            else:
                                
                                # If the current value IS null, save an empty string.

                                studies[current_id][current_column_name] = ''

# Get the study_description for each study_id from the UIStudy TSV.

study_id_to_study_description = map_columns_one_to_one( uistudy_tsv, 'study_id', 'study_description' )

# Add study_description, as loaded from the UIStudy TSV above, to all Study records.

for current_id in studies:
    
    if current_id in study_id_to_study_description:
        
        studies[current_id]['study_description'] = study_id_to_study_description[current_id]

    else:
        
        studies[current_id]['study_description'] = ''

study_fields.append( 'study_description' )

# Write the main Study table.

with open( study_output_tsv, 'w' ) as OUT:
    
    # Header row.

    print( *study_fields, sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( studies ):
        
        output_row = [ current_id ] + [ studies[current_id][field_name] for field_name in study_fields[1:] ]

        print( *output_row, sep='\t', end='\n', file=OUT )

# Load the Study->disease_type association.

study_id_to_disease_type = map_columns_one_to_many( study_disease_types_input_tsv, 'study_id', 'disease_type' )

# Write the Study->disease_type association.

with open( study_disease_type_output_tsv, 'w' ) as OUT:
    
    print( *[ 'study_id', 'disease_type' ], sep='\t', end='\n', file=OUT )

    for study_id in sorted( study_id_to_disease_type ):
        
        for disease_type in sorted( study_id_to_disease_type[study_id] ):
            
            if disease_type not in values_to_ignore:
                
                print( *[ study_id, disease_type ], sep='\t', end='\n', file=OUT )

# Load the Study->primary_site association.

study_id_to_primary_site = map_columns_one_to_many( study_primary_sites_input_tsv, 'study_id', 'primary_site' )

# Write the Study->primary_site association.

with open( study_primary_site_output_tsv, 'w' ) as OUT:
    
    print( *[ 'study_id', 'primary_site' ], sep='\t', end='\n', file=OUT )

    for study_id in sorted( study_id_to_primary_site ):
        
        for primary_site in sorted( study_id_to_primary_site[study_id] ):
            
            if primary_site not in values_to_ignore:
                
                print( *[ study_id, primary_site ], sep='\t', end='\n', file=OUT )

# Load the Study->instrument association.

study_id_to_instrument = map_columns_one_to_many( experimental_metadata_to_study_run_metadata_tsv, 'study_id', 'instrument' )

# Write the Study->instrument association.

with open( study_instrument_output_tsv, 'w' ) as OUT:
    
    print( *[ 'study_id', 'instrument' ], sep='\t', end='\n', file=OUT )

    for study_id in sorted( study_id_to_instrument ):
        
        for instrument in sorted( study_id_to_instrument[study_id] ):
            
            print( *[ study_id, instrument ], sep='\t', end='\n', file=OUT )



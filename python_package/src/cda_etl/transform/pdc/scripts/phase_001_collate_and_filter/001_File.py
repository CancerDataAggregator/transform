#!/usr/bin/env python -u

import sys

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

from os import makedirs, path

# PARAMETERS

input_root = 'extracted_data/pdc'

srm_supplemental_input_tsv = path.join( input_root, 'ExperimentalMetadata', 'StudyRunMetadata.tsv' )

srm_to_file_supplemental_input_tsv = path.join( input_root, 'ExperimentalMetadata', 'StudyRunMetadata.files.tsv' )

input_files_to_scan = {
    'File': f"{input_root}/File/File.tsv",
    'FilePerStudy': f"{input_root}/FilePerStudy/FilePerStudy.tsv",
    'FileMetadata': f"{input_root}/FileMetadata/FileMetadata.tsv"
}

association_fields = {
    'File': {
        'study_id'
    },
    'FilePerStudy': {
        'study_id'
    },
    'FileMetadata': {
        'instrument'
    }
}

fields_to_ignore = {
    'File': {
        'files_count',
        'pdc_study_id',
        'study_submitter_id'
    },
    'FilePerStudy': {
        'pdc_study_id',
        'study_name',
        'study_submitter_id'
    },
    'FileMetadata' : {
        'study_run_metadata_id',
        'study_run_metadata_submitter_id'
    }
}

values_to_ignore = {
}

processing_order = [
    'File',
    'FilePerStudy',
    'FileMetadata'
]

output_root = 'extracted_data/pdc_postprocessed'

file_output_dir = f"{output_root}/File"

file_output_tsv = f"{file_output_dir}/File.tsv"

file_id_to_aliquot_id_tsv = f"{file_output_dir}/File.aliquot_id.tsv"

file_id_to_study_run_metadata_ids_tsv = f"{file_output_dir}/File.study_run_metadata_ids.tsv"

file_metadata_aliquot_tsv = f"{input_root}/FileMetadata/Aliquot.tsv"

file_metadata_to_aliquot_id_tsv = f"{input_root}/FileMetadata/FileMetadata.aliquots.tsv"

# This is the only place to get `File.access` values.

uifile_tsv = f"{input_root}/UIFile/UIFile.tsv"

log_dir = path.join( output_root, '__filtration_logs' )

file_ids_sharing_file_location_filtration_log = path.join( log_dir, 'File.multiple_files_sharing_same_file_location.removed_ids.txt' )

# EXECUTION

for output_dir in [ file_output_dir, log_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Load the map between aliquot_id and case_id.

aliquot_id_to_case_id = map_columns_one_to_one( file_metadata_aliquot_tsv, 'aliquot_id', 'case_id' )

# Load the map between file_id and access.

file_id_to_access = map_columns_one_to_one( uifile_tsv, 'file_id', 'access' )

# Load the map between file_id and aliquot_id.

file_id_to_aliquot_id = map_columns_one_to_many( file_metadata_to_aliquot_id_tsv, 'file_id', 'aliquot_id' )

files = dict()

file_fields = list()

file_associations = dict()

# Compute file_id <-> case_id as we go, transitively collapsing
# file_id <-> aliquot_id <-> case_id as needed (noted in ingest policy
# document. note further: this is the only way to recover this relationship).

file_associations['case_id'] = dict()

# Maintain (file_id, study_run_metadata_id, study_run_metadata_submitter_id) association triples
# instead of pairwise maps, because the SRM IDs can't be recoupled if disconnected from one another.

file_id_to_srm_ids = dict()

# We'll load this automatically from the first column in each input file.

id_field_name = ''

# There are a few records which reference the same file_location but have been
# assigned multiple distinct file_ids. We ignore these according to
# documented ingest policy (see ingest policy document for rationale).

ids_by_file_location = dict()

for input_file_label in processing_order:
    
    with open( input_files_to_scan[input_file_label] ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            id_field_name = column_names[0]

            current_id = values[0]

            if current_id not in files and input_file_label == 'File':
                
                files[current_id] = dict()

            elif current_id not in files:
                
                sys.exit( f"001_File.py: FATAL: File id '{current_id}' present in {input_file_label} records but not found in master table {input_files_to_scan['File']}; aborting." )

            if input_file_label == 'FileMetadata':
                
                # Save File<->StudyRunMetadata ID associations as clusters, not pairwise. Unavailability of StudyRunMetadata records anywhere else makes recoupling impossible once separated.

                srm_uuid_field_index = column_names.index( 'study_run_metadata_id' )
                srm_submitter_id_field_index = column_names.index( 'study_run_metadata_submitter_id' )

                srm_uuid = values[srm_uuid_field_index]
                srm_submitter_id = values[srm_submitter_id_field_index]

                # This isn't value filtration/harmonization: it's referential-integrity checking -- leave as-is unless safe replacement is desired/identified.

                if srm_uuid is not None and srm_uuid != '' and srm_uuid != 'N/A' and srm_submitter_id is not None and srm_submitter_id != '' and srm_submitter_id != 'N/A':
                    
                    if current_id not in file_id_to_srm_ids:
                        
                        file_id_to_srm_ids[current_id] = dict()

                    if srm_uuid not in file_id_to_srm_ids[current_id]:
                        
                        file_id_to_srm_ids[current_id][srm_uuid] = set()

                    file_id_to_srm_ids[current_id][srm_uuid].add( srm_submitter_id )

            # Link file_id to case_id from pre-loaded maps (via aliquot_id).

            if current_id in file_id_to_aliquot_id:
                
                for aliquot_id in file_id_to_aliquot_id[current_id]:
                    
                    if aliquot_id in aliquot_id_to_case_id:
                        
                        if current_id not in file_associations['case_id']:
                            
                            file_associations['case_id'][current_id] = set()

                        file_associations['case_id'][current_id].add(aliquot_id_to_case_id[aliquot_id])

            if id_field_name not in file_fields:
                
                file_fields.append( id_field_name )

            for i in range( 1, len( column_names ) ):
                
                current_column_name = column_names[i]

                current_value = values[i]

                if current_column_name == 'file_location':
                    
                    if current_value not in ids_by_file_location:
                        
                        ids_by_file_location[current_value] = set()

                    ids_by_file_location[current_value].add(current_id)

                if current_column_name not in fields_to_ignore[input_file_label]:
                    
                    if current_column_name in association_fields[input_file_label]:
                        
                        if current_column_name not in file_associations:
                            
                            file_associations[current_column_name] = dict()

                        if current_id not in file_associations[current_column_name]:
                            
                            file_associations[current_column_name][current_id] = set()

                        if current_value != '' and current_value not in values_to_ignore:
                            
                            file_associations[current_column_name][current_id].add(current_value)

                    else:
                        
                        if current_column_name not in file_fields:
                            
                            file_fields.append( current_column_name )

                        # If we scanned this before,

                        if current_column_name in files[current_id]:
                            
                            # If the current value isn't null or set to be skipped,

                            if current_value != '' and current_value not in values_to_ignore:
                                
                                # If the old value isn't null,

                                if files[current_id][current_column_name] != '':
                                    
                                    # Make sure the two values match.

                                    if current_value != files[current_id][current_column_name]:
                                        
                                        sys.exit(f"FATAL: Multiple non-null values ('{files[current_id][current_column_name]}', '{current_value}') seen for field '{current_column_name}', {id_field_name} '{current_id}'. Aborting.\n")

                                # If the old value IS null,

                                else:
                                    
                                    # Overwrite it with the new one.

                                    files[current_id][current_column_name] = current_value

                            # ( If we've scanned this before and the current value is null, do nothing. )

                        else:
                            
                            # If we haven't scanned this yet,

                            if current_value != '' and current_value not in values_to_ignore:
                                
                                # If the current value isn't null or set to be skipped, record it.

                                files[current_id][current_column_name] = current_value

                            else:
                                
                                # If the current value IS null, save an empty string.

                                files[current_id][current_column_name] = ''

# Write the main File table.

with open( file_output_tsv, 'w' ) as OUT, open( file_ids_sharing_file_location_filtration_log, 'w' ) as LOG:
    
    # Header row.

    # Until access data becomes available elsewhere, splice it in here from UIFile.

    print( *( file_fields + [ 'access' ] ), sep='\t', end='\n', file=OUT )

    # Data rows.

    for current_id in sorted( files ):
        
        current_file_location = files[current_id]['file_location']

        if len( ids_by_file_location[current_file_location] ) == 1:
            
            output_row = [ current_id ]

            for field_name in file_fields[1:]:
                
                if field_name not in files[current_id]:
                    
                    output_row.append( '' )

                else:
                    
                    output_row.append( files[current_id][field_name] )

            # Until access data becomes available elsewhere, splice it in here from UIFile.

            if current_id in file_id_to_access:
                
                output_row.append( file_id_to_access[current_id] )

            else:
                
                output_row.append( '' )

            print( *output_row, sep='\t', end='\n', file=OUT )

        else:
            
            print( current_id, file=LOG )

# Write the map between file IDs and aliquot IDs.

with open( file_id_to_aliquot_id_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'aliquot_id' ], sep='\t', end='\n', file=OUT )

    for file_id in sorted( file_id_to_aliquot_id ):
        
        if file_id in files:
            
            for aliquot_id in sorted( file_id_to_aliquot_id[file_id] ):
                
                print( *[ file_id, aliquot_id ], sep='\t', end='\n', file=OUT )

# Load some (at time of writing) File->StudyRunMetadata links not retrievable via fileMetadata().

supplemental_srm_id_to_file_location = map_columns_one_to_many( srm_to_file_supplemental_input_tsv, 'study_run_metadata_id', 'file_location' )

# Cache the map from study_run_metadata_id to study_run_metadata_submitter_id for the supplemental StudyRunMetadata record set.

supplemental_srm_id_to_srm_submitter_id = map_columns_one_to_many( srm_supplemental_input_tsv, 'study_run_metadata_id', 'study_run_metadata_submitter_id' )

for srm_id in supplemental_srm_id_to_file_location:
    
    for file_location in supplemental_srm_id_to_file_location[srm_id]:
        
        # Does the referenced File exist (and is its file_id well-defined)?

        if file_location in ids_by_file_location and len( ids_by_file_location[file_location] ) == 1:
            
            file_id = sorted( ids_by_file_location[file_location] )[0]

            if file_id not in file_id_to_srm_ids:
                
                file_id_to_srm_ids[file_id] = dict()

            if srm_id not in file_id_to_srm_ids[file_id]:
                
                file_id_to_srm_ids[file_id][srm_id] = set()

            for srm_submitter_id in sorted( supplemental_srm_id_to_srm_submitter_id[srm_id] ):
                
                file_id_to_srm_ids[file_id][srm_id].add( srm_submitter_id )

# Write the map (at time of writing, updated) map between File IDs and StudyRunMetadata IDs.

with open( file_id_to_study_run_metadata_ids_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'study_run_metadata_id', 'study_run_metadata_submitter_id' ], sep='\t', file=OUT )

    for file_id in sorted( file_id_to_srm_ids ):
        
        for study_run_metadata_id in sorted( file_id_to_srm_ids[file_id] ):
            
            for study_run_metadata_submitter_id in sorted( file_id_to_srm_ids[file_id][study_run_metadata_id] ):
                
                print( *[ file_id, study_run_metadata_id, study_run_metadata_submitter_id ], sep='\t', file=OUT )

# Write the other association tables.

for target_name in file_associations:
    
    association_output_tsv = f"{file_output_dir}/File.{target_name}.tsv"

    with open( association_output_tsv, 'w' ) as OUT:
        
        # Header row.

        print( *[ id_field_name, target_name ], sep='\t', end='\n', file=OUT )

        # Data rows.

        for current_id in sorted( file_associations[target_name] ):
            
            current_file_location = files[current_id]['file_location']

            if len( ids_by_file_location[current_file_location] ) == 1:
                
                for current_target in sorted( file_associations[target_name][current_id] ):
                    
                    print( *[ current_id, current_target ], sep='\t', end='\n', file=OUT )



#!/usr/bin/env python3 -u

import re, sys

from os import listdir, path, makedirs
from shutil import copy

from cda_etl.lib import deduplicate_and_sort_unsorted_file_with_header, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, sort_and_uniquify_file_with_header

# PARAMETERS

cda_root = path.join( 'cda_tsvs' )

full_last_merged_dataset_name = 'merged_GDC_and_PDC'

abbreviated_last_merged_dataset_name = 'GDC_PDC'

last_merged_cda_tsv_dir = path.join( cda_root, f"{full_last_merged_dataset_name.lower()}_002_decorated_harmonized" )

last_merged_upstream_identifiers_tsv = path.join( last_merged_cda_tsv_dir, 'upstream_identifiers.tsv' )

to_merge_dataset_name = 'CDS'

to_merge_cda_tsv_dir = path.join( cda_root, f"{to_merge_dataset_name.lower()}_002_decorated_harmonized" )

to_merge_upstream_identifiers_tsv = path.join( to_merge_cda_tsv_dir, 'upstream_identifiers.tsv' )

tsv_output_dir = path.join( cda_root, 'merged_gdc_pdc_and_cds_002_decorated_harmonized' )

upstream_identifiers_output_tsv = path.join( tsv_output_dir, 'upstream_identifiers.tsv' )

map_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

merge_map = {
    
    'project' : path.join( map_root, 'projects', f"{to_merge_dataset_name}_CDA_projects_merged_into_{abbreviated_last_merged_dataset_name}_CDA_projects.tsv" ),
    'subject' : path.join( map_root, 'subjects', f"{to_merge_dataset_name}_CDA_subjects_merged_into_{abbreviated_last_merged_dataset_name}_CDA_subjects.tsv" )
}

duplicates_possible = {
    
    'project_in_project.tsv',
    'subject_in_project.tsv',
    'upstream_identifiers.tsv'
}

# These may have groups of rows that are duplicates of one another except for CDA-assigned IDs. Collapse each such group of rows into a single row.

duplicates_possible_modulo_ids = {
    
    'observation.tsv',
    'treatment.tsv'
}

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_value_output_dir = path.join( aux_output_root, 'values' )

data_clash_log = {
    
    'project': path.join( aux_value_output_dir, f"{to_merge_dataset_name}_into_{abbreviated_last_merged_dataset_name}_project_merge_clashes.all_fields.tsv" ),
    'subject': path.join( aux_value_output_dir, f"{to_merge_dataset_name}_into_{abbreviated_last_merged_dataset_name}_subject_merge_clashes.all_fields.tsv" )
}

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

for output_dir in [ tsv_output_dir, aux_value_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Load replacement data.

replace_with = dict()

map_targets = dict()

for table in sorted( merge_map ):
    
    if table not in replace_with:
        
        replace_with[table] = dict()

    if table not in map_targets:
        
        map_targets[table] = dict()

    map_file = merge_map[table]

    with open( map_file ) as IN:
        
        column_names = next( IN ).split( '\t' )

        from_column = f"{to_merge_dataset_name}_{table}_alias"

        with_column = f"{abbreviated_last_merged_dataset_name}_{table}_alias"

        to_column = f"new_{table}_alias"

        for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
            
            if from_column not in record or with_column not in record or to_column not in record:
                
                sys.exit( f"FATAL: One or more of expected columns [ '{from_column}', '{with_column}', '{to_column}' ] not present in {map_file}; cannot continue, aborting." )

            elif record[with_column] != record[to_column]:
                
                sys.exit( f"FATAL: Map file {map_file} has different values between columns '{with_column}' and '{to_column}'; this is unexpected, please write code to handle this situation. Aborting." )

            else:
                
                replace_with[table][record[from_column]] = record[with_column]

                if record[with_column] not in map_targets[table]:
                    
                    map_targets[table][record[with_column]] = set()

                map_targets[table][record[with_column]].add( record[from_column] )

# First, map tables with records to merge.
# 
# ASSUMPTION (safe at time of writing): Tables in this pass will not contain
# foreign keys (aliases) whose values might need updating due to their
# own (referenced entities') merges: i.e., only `id` and `id_alias` columns
# will be used to establish whether and how to merge each record, with all
# other fields subject to value-based data merging rules (generally, first
# non-null value wins, although please do explicitly check the current
# rules if it matters).

for table in sorted( merge_map ):
    
    print( f"Merging data for {table}...", end='', file=sys.stderr )

    last_merged_input_file = path.join( last_merged_cda_tsv_dir, table + '.tsv' )

    to_merge_input_file = path.join( to_merge_cda_tsv_dir, table + '.tsv' )

    output_file = path.join( tsv_output_dir, table + '.tsv' )

    last_merged_table = load_tsv_as_dict( last_merged_input_file )

    to_merge_table = load_tsv_as_dict( to_merge_input_file )

    # Enable lookups by alias.

    to_merge_alias_to_id = map_columns_one_to_one( to_merge_input_file, 'id_alias', 'id' )

    header = ''

    with open( last_merged_input_file ) as IN:
        
        header = next( IN ).rstrip( '\n' )

    column_names = header.split( '\t' )

    processed_records_to_merge = set()

    data_clashes = dict()

    with open( output_file, 'w' ) as OUT:
        
        print( header, file=OUT )

        # Process the rows from `last_merged_dataset_name` first, updating as we go with new data from incoming merges.

        for last_merged_id in sorted( last_merged_table ):
            
            last_merged_record = dict()

            for field_name in last_merged_table[last_merged_id]:
                
                last_merged_record[field_name] = last_merged_table[last_merged_id][field_name]

            output_row = list()

            last_merged_alias = last_merged_record['id_alias']

            if last_merged_alias not in map_targets[table]:
                
                # This record will not be merged. Forward unmodified to output.

                for column_name in column_names:
                    
                    output_row.append( last_merged_record[column_name] )

            else:
                
                # Initialize a copy of last_merged_record as originally represented in `last_merged_table`. We'll modify the copy as we go.

                output_record = dict()

                for column_name in column_names:
                    
                    output_record[column_name] = last_merged_record[column_name]

                for to_merge_alias in sorted( map_targets[table][last_merged_alias] ):
                    
                    for column_name in column_names:
                        
                        if re.search( r'^data_at_', column_name ) is not None:
                            
                            # Update provenance booleans by joining existing records via logical OR. This is incredibly cumbersome because Python is quite bad about implicit boolean conversion. I am handling everything as strings as a result.

                            last_updated_value = output_record[column_name]

                            to_merge_value = to_merge_table[to_merge_alias_to_id[to_merge_alias]][column_name]

                            new_value = 'False'

                            if last_updated_value == 'True' or to_merge_value == 'True':
                                
                                new_value = 'True'

                            output_record[column_name] = new_value

                        elif column_name not in [ 'id', 'id_alias', 'data_source_count' ]:
                            
                            original_last_merged_value = last_merged_record[column_name]

                            last_updated_value = output_record[column_name]

                            to_merge_value = to_merge_table[to_merge_alias_to_id[to_merge_alias]][column_name]

                            record_id = last_merged_record['id']

                            if last_updated_value == '' and to_merge_value != '':
                                
                                # Replace existing nulls with the first-encountered non-null alternative.

                                output_record[column_name] = to_merge_value

                                if record_id not in data_clashes:
                                    
                                    data_clashes[record_id] = dict()

                                if column_name not in data_clashes[record_id]:
                                    
                                    data_clashes[record_id][column_name] = dict()

                                if original_last_merged_value not in data_clashes[record_id][column_name]:
                                    
                                    data_clashes[record_id][column_name][original_last_merged_value] = dict()

                                if to_merge_value not in data_clashes[record_id][column_name][original_last_merged_value]:
                                    
                                    data_clashes[record_id][column_name][original_last_merged_value][to_merge_value] = to_merge_value

                            elif last_updated_value != '' and to_merge_value != last_updated_value:
                                
                                # Does the existing value match a pattern we know will be deleted later?

                                if re.sub( r'\s', r'', last_updated_value.strip().lower() ) in delete_everywhere:
                                    
                                    # Is the new value any better?

                                    if to_merge_value == '' or re.sub( r'\s', r'', to_merge_value.strip().lower() ) in delete_everywhere:
                                        
                                        # Both old and new values will eventually be deleted. Go with the original, but log the clash.

                                        if record_id not in data_clashes:
                                            
                                            data_clashes[record_id] = dict()

                                        if column_name not in data_clashes[record_id]:
                                            
                                            data_clashes[record_id][column_name] = dict()

                                        if original_last_merged_value not in data_clashes[record_id][column_name]:
                                            
                                            data_clashes[record_id][column_name][original_last_merged_value] = dict()

                                        data_clashes[record_id][column_name][original_last_merged_value][to_merge_value] = last_updated_value

                                    else:
                                        
                                        # Replace the old value with the new one.

                                        output_record[column_name] = to_merge_value

                                        if record_id not in data_clashes:
                                            
                                            data_clashes[record_id] = dict()

                                        if column_name not in data_clashes[record_id]:
                                            
                                            data_clashes[record_id][column_name] = dict()

                                        if original_last_merged_value not in data_clashes[record_id][column_name]:
                                            
                                            data_clashes[record_id][column_name][original_last_merged_value] = dict()

                                        data_clashes[record_id][column_name][original_last_merged_value][to_merge_value] = to_merge_value

                                else:
                                    
                                    # Make no replacement for non-null value clashes, but log the difference.

                                    if record_id not in data_clashes:
                                        
                                        data_clashes[record_id] = dict()

                                    if column_name not in data_clashes[record_id]:
                                        
                                        data_clashes[record_id][column_name] = dict()

                                    if original_last_merged_value not in data_clashes[record_id][column_name]:
                                        
                                        data_clashes[record_id][column_name][original_last_merged_value] = dict()

                                    data_clashes[record_id][column_name][original_last_merged_value][to_merge_value] = last_updated_value

                    processed_records_to_merge.add( to_merge_alias )

                # Compute data_source_count directly from merged results.

                new_data_source_count = 0

                for column_name in column_names:
                    
                    if re.search( r'^data_at_', column_name ) is not None:
                        
                        if output_record[column_name] == 'True':
                            
                            new_data_source_count = new_data_source_count + 1

                output_record['data_source_count'] = new_data_source_count

                for column_name in column_names:
                    
                    output_row.append( output_record[column_name] )

            print( *output_row, sep='\t', file=OUT )

        # Next, process the rows from `to_merge_dataset_name`, skipping any already merged into existing records.

        for to_merge_id in sorted( to_merge_table ):
            
            to_merge_record = dict()

            for field_name in to_merge_table[to_merge_id]:
                
                to_merge_record[field_name] = to_merge_table[to_merge_id][field_name]

            output_row = list()

            to_merge_alias = to_merge_record['id_alias']

            if to_merge_alias not in processed_records_to_merge:
                
                # This record was not merged with an existing CDA record. Forward unmodified to output.

                for column_name in column_names:
                    
                    output_row.append( to_merge_record[column_name] )

                print( *output_row, sep='\t', file=OUT )

    with open( data_clash_log[table], 'w' ) as OUT:
        
        print( *[ f"CDA_{table}_id", 'CDA_field_name', f"original_value_from_{abbreviated_last_merged_dataset_name}", f"observed_clashing_value_from_{to_merge_dataset_name}", f"CDA_kept_value" ], sep='\t', file=OUT )

        for record_id in sorted( data_clashes ):
            
            for column_name in sorted( data_clashes[record_id] ):
                
                for last_merged_value in sorted( data_clashes[record_id][column_name] ):
                    
                    for to_merge_value in sorted( data_clashes[record_id][column_name][last_merged_value] ):
                        
                        print( *[ record_id, column_name, last_merged_value, to_merge_value, data_clashes[record_id][column_name][last_merged_value][to_merge_value] ], sep='\t', file=OUT )

    print( 'done.', file=sys.stderr )

# Next, map all tables present in `last_merged_cda_tsv_dir` that we haven't yet
# processed, except upstream_identifiers (handled separately), column_metadata and release_metadata
# (both regenerated after merge build completes).

completed_files = set()

for file_basename in sorted( listdir( last_merged_cda_tsv_dir ) ):
    
    if re.search( r'\.tsv$', file_basename ) is not None:
        
        table = re.sub( r'^(.*)\.tsv$', r'\1', file_basename )

        if table not in merge_map and table != 'release_metadata' and table != 'upstream_identifiers' and table != 'column_metadata':
            
            print( f"Updating {file_basename}...", end='', file=sys.stderr )

            last_merged_file = path.join( last_merged_cda_tsv_dir, file_basename )

            output_file = path.join( tsv_output_dir, file_basename )

            # First, copy the last_merged data to the destination, updating as needed.

            requires_update = False

            update_with = dict()

            with open( last_merged_file ) as IN, open( output_file, 'w' ) as OUT:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                print( *column_names, sep='\t', file=OUT )

                for column_name in column_names:
                    
                    match_list = re.findall( r'([^_]+)_alias$', column_name )

                    if len( match_list ) > 0:
                        
                        aliased_table = match_list[0]

                        if aliased_table in replace_with:
                            
                            requires_update = True

                            update_with[column_name] = aliased_table

                if not requires_update:
                    
                    # Just copy data rows on over without modification.

                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        print( line, file=OUT )

                else:
                    
                    # Copy rows, substituting all target alias values as needed.

                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        record = dict( zip( column_names, line.split( '\t' ) ) )

                        output_row = list()

                        # Main row data.

                        for column_name in column_names:
                            
                            if column_name not in update_with:
                                
                                output_row.append( record[column_name] )

                            else:
                                
                                aliased_table = update_with[column_name]

                                current_alias_value = record[column_name]

                                if current_alias_value in replace_with[aliased_table]:
                                    
                                    current_alias_value = replace_with[aliased_table][current_alias_value]

                                output_row.append( current_alias_value )

                        print( *output_row, sep='\t', file=OUT )

            if file_basename in listdir( to_merge_cda_tsv_dir ):
                
                # This file also exists in the to_merge dataset. Bring that data on over, updating aliases as needed.

                to_merge_file = path.join( to_merge_cda_tsv_dir, file_basename )

                with open( to_merge_file ) as IN, open( output_file, 'a' ) as OUT:
                    
                    header = next( IN )

                    if not requires_update:
                        
                        # Just copy data rows on over without modification.

                        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                            
                            print( line, file=OUT )

                    else:
                        
                        # Copy rows, substituting all target alias values as needed.

                        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                            
                            record = dict( zip( column_names, line.split( '\t' ) ) )

                            output_row = list()

                            # Main row data.

                            for column_name in column_names:
                                
                                if column_name not in update_with:
                                    
                                    output_row.append( record[column_name] )

                                else:
                                    
                                    aliased_table = update_with[column_name]

                                    current_alias_value = record[column_name]

                                    if current_alias_value in replace_with[aliased_table]:
                                        
                                        current_alias_value = replace_with[aliased_table][current_alias_value]

                                    output_row.append( current_alias_value )

                            print( *output_row, sep='\t', file=OUT )

            negative_modifier = 'not '

            if requires_update:
                
                negative_modifier = ''

            print( f"done. (Data update {negative_modifier}performed.)", file=sys.stderr )

            completed_files.add( file_basename )

# Next, map all tables present in `to_merge_cda_tsv_dir` that we haven't yet
# processed, except upstream_identifiers (handled separately) and release_metadata
# (regenerated after merge build completes).

for file_basename in sorted( listdir( to_merge_cda_tsv_dir ) ):
    
    if file_basename not in completed_files and re.search( r'\.tsv$', file_basename ) is not None:
        
        table = re.sub( r'^(.*)\.tsv$', r'\1', file_basename )

        if table not in merge_map and table != 'release_metadata' and table != 'upstream_identifiers':
            
            print( f"Updating {file_basename} (exists only in '{to_merge_cda_tsv_dir}')...", end='', file=sys.stderr )

            to_merge_file = path.join( to_merge_cda_tsv_dir, file_basename )

            output_file = path.join( tsv_output_dir, file_basename )

            # Copy to the destination, updating as needed.

            requires_update = False

            update_with = dict()

            with open( to_merge_file ) as IN, open( output_file, 'w' ) as OUT:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                print( *column_names, sep='\t', file=OUT )

                for column_name in column_names:
                    
                    match_list = re.findall( r'([^_]+)_alias$', column_name )

                    if len( match_list ) > 0:
                        
                        aliased_table = match_list[0]

                        if aliased_table in replace_with:
                            
                            requires_update = True

                            update_with[column_name] = aliased_table

                if not requires_update:
                    
                    # Just copy data rows on over without modification.

                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        print( line, file=OUT )

                else:
                    
                    # Copy rows, substituting all target alias values as needed.

                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        record = dict( zip( column_names, line.split( '\t' ) ) )

                        output_row = list()

                        # Main row data.

                        for column_name in column_names:
                            
                            if column_name not in update_with:
                                
                                output_row.append( record[column_name] )

                            else:
                                
                                aliased_table = update_with[column_name]

                                current_alias_value = record[column_name]

                                if current_alias_value in replace_with[aliased_table]:
                                    
                                    current_alias_value = replace_with[aliased_table][current_alias_value]

                                output_row.append( current_alias_value )

                        print( *output_row, sep='\t', file=OUT )

            negative_modifier = 'not '

            if requires_update:
                
                negative_modifier = ''

            print( f"done. (Data update {negative_modifier}performed.)", file=sys.stderr )

# Next, combine upstream_identifiers information, updating alias values as needed.

print( 'Updating upstream_identifiers.tsv...', end='', file=sys.stderr )

# upstream_identifiers.tsv from `last_merged_cda_tsv_dir` will require no modification.

copy( last_merged_upstream_identifiers_tsv, upstream_identifiers_output_tsv )

# upstream_identifiers.tsv from `to_merge_cda_tsv_dir` will need aliases updated.

with open( to_merge_upstream_identifiers_tsv ) as IN, open( upstream_identifiers_output_tsv, 'a' ) as OUT:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) ) for next_line in IN ]:
        
        table = record['cda_table']

        output_row = list()

        if table in replace_with:
            
            # Aliases are subject to change. Make sure they're up to date.

            current_alias = record['id_alias']

            if current_alias in replace_with[table]:
                
                record['id_alias'] = replace_with[table][current_alias]

        for column_name in column_names:
            
            output_row.append( record[column_name] )

        print( *output_row, sep='\t', file=OUT )

print( 'done.', file=sys.stderr )

# Finally: eliminate duplicate rows wherever they might have been created by our alias updates.

for file_basename in sorted( duplicates_possible ):
    
    if file_basename in listdir( tsv_output_dir ):
        
        print( f"Deduplicating {file_basename}...", end='', file=sys.stderr )

        target_file_path = path.join( tsv_output_dir, file_basename )

        gzipped = False

        if re.search( r'\.gz$', file_basename ) is not None:
            
            gzipped = True

        deduplicate_and_sort_unsorted_file_with_header( target_file_path, gzipped )

        print( 'done.', file=sys.stderr )

for file_basename in sorted( duplicates_possible_modulo_ids ):
    
    if file_basename in listdir( tsv_output_dir ):
        
        print( f"Deduplicating {file_basename}...", end='', file=sys.stderr )

        target_file_path = path.join( tsv_output_dir, file_basename )

        gzipped = False

        if re.search( r'\.gz$', file_basename ) is not None:
            
            gzipped = True

        deduplicate_and_sort_unsorted_file_with_header( target_file_path, gzipped, ignore_primary_id_field=True )

        print( 'done.', file=sys.stderr )



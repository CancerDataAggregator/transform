#!/usr/bin/env python -u

import gzip, re, sys

from os import listdir, makedirs, path

from cda_etl.lib import get_current_timestamp, get_column_metadata

# SUBROUTINE

def find_input_tsvs( scan_dir, skip_files=dict() ):
    
    found_tsvs = set()

    for basename in listdir( scan_dir ):
        
        # Ignore files and directories beginning with '__' by convention.

        if re.search( r'^__', basename ) is None and basename not in skip_files:
            
            usable_path = path.join( scan_dir, basename )

            if path.isdir( usable_path ):
                
                found_tsvs = found_tsvs | find_input_tsvs( usable_path )

            elif re.search( r'\.tsv', basename ) is not None:
                
                found_tsvs.add( path.join( scan_dir, basename ) )

    return found_tsvs

# ARGUMENTS

if ( len( sys.argv ) < 8 ) or ( ( len( sys.argv ) - 2 ) % 3 != 0 ):
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <TSV directory to scan> <desired output file> <CDA release version> <current date, e.g. 2024-08-19> [<data source, e.g. 'gdc'> <extraction_date, e.g. '2024-08-18'> <version string, e.g. \"Data Release 40.0\">]+\n" )

input_root = sys.argv[1]

output_file = sys.argv[2]

version_string = dict()

version_string['CDA'] = sys.argv[3]

extraction_date = dict()

extraction_date['CDA'] = sys.argv[4]

arg_index = 5

while arg_index < len( sys.argv ):
    
    data_source = sys.argv[arg_index].upper()

    extraction_date[data_source] = sys.argv[arg_index + 1]

    version_string[data_source] = sys.argv[arg_index + 2]

    arg_index = arg_index + 3

match_list = re.findall( r'^(.*)\/[^\/]+$', output_file )

output_dir = '.'

if len( match_list ) > 0:
    
    output_dir = match_list[0]

# PARAMETERS

# Don't scan CDA release-metadata files if they've been built out of sequence;
# also skip dicom_series_instance.tsv.gz because everything we need is summarized
# in dicom_series.tsv and its association tables; also skip upstream_identifiers.tsv
# because it's not exposed to the user; also skip association tables that introduce no
# new values.

files_to_skip = {
    
    'column_metadata.tsv',
    'dicom_series_describes_subject.tsv',
    'dicom_series_in_project.tsv',
    'dicom_series_instance.tsv.gz',
    'file_describes_subject.tsv',
    'file_in_project.tsv',
    'project_in_project.tsv',
    'subject_in_project.tsv',
    'release_metadata.tsv',
    'upstream_identifiers.tsv'
}

# Don't increment row counts when loading virtual (aliased) column values from association tables
# (because we'll overcount if we do).
# 
# Note that association tables not listed here won't be scanned in the first place, because they
# have no countable columns.
# 
# Save the (always unique: Sep 2024) FK reference for each of the listed association tables for later
# alias resolution so we can document provenance.
# 
# Note that these will differ from `virtual_table` values on individual columns (e.g. dicom_series_anatomic_site.anatomic_site -> file, not dicom_series).

association_tables = {
    
    'file_anatomic_site': 'file',
    'dicom_series_anatomic_site': 'dicom_series',
    'file_tumor_vs_normal': 'file',
    'dicom_series_tumor_vs_normal': 'dicom_series',
    'dicom_series_type': 'dicom_series'
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Locate input TSVs.

input_files = find_input_tsvs( input_root, skip_files=files_to_skip )

display_filename = dict()

for input_file in input_files:
    
    display_filename[input_file] = re.sub( r'^' + re.escape( input_root ) + r'\/?', r'', input_file )

column_has_nulls = dict()
column_all_numbers = dict()
column_null_count = dict()
column_distinct_values = dict()
row_count = dict()

alias_to_data_source = {
    
    'file': dict(),
    'dicom_series': dict()
}

dicom_series_instance_count = dict()

# First pass: skip association tables and load maps between aliases in referenced tables and data sources.

for input_file_path in sorted( input_files ):
    
    file_label = display_filename[input_file_path]

    if re.search( r'\.tsv', file_label ) is not None:
        
        table_name = re.sub( r'\.tsv(?:\.gz)?$', r'', file_label )

        if table_name not in association_tables:
            
            print( f"   [{get_current_timestamp()}] Processing {table_name}...", end='', file=sys.stderr )

            IN = open( input_file_path )

            if re.search( r'\.tsv\.gz$', input_file_path ) is not None:
                
                # Sometimes things are gzipped.

                IN.close()

                IN = gzip.open( input_file_path, 'rt' )

            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            data_source_column_to_source = dict()

            # We can't count everything (e.g. file.checksum_value for 45M [virtual] files). Use get_column_metadata() to decide what and how to count.

            count_columns = list()

            count_column_as = dict()

            for column_name in column_names:
                
                # Identify data_at_* columns and hook each up with its associated data_source.

                match_list = re.findall( r'^data_at_(.+)$', column_name )

                if len( match_list ) > 0:
                    
                    data_source = match_list[0].upper()

                    data_source_column_to_source[column_name] = data_source

                else:
                    
                    column_result = get_column_metadata( table_name, column_name )

                    if ( table_name == 'dicom_series_type' and column_name == 'type' ) or \
                       ( 'column_type' in column_result and column_result['column_type'] == 'categorical' and 'fetch_rows_returns' in column_result and column_result['fetch_rows_returns'] == True ):
                        
                        count_columns.append( column_name )

                        if 'process_before_display' in column_result and ( column_result['process_before_display'] in { 'dicom_series_type', 'virtual_field', 'virtual_list' } ):
                            
                            count_column_as[column_name] = column_result['virtual_table']

            if table_name not in column_has_nulls:
                
                # We've not yet recorded any data for this table.

                column_has_nulls[table_name] = dict()
                column_all_numbers[table_name] = dict()
                column_null_count[table_name] = dict()
                column_distinct_values[table_name] = dict()
                row_count[table_name] = dict()

                for data_source in extraction_date:
                    
                    column_has_nulls[table_name][data_source] = dict()
                    column_all_numbers[table_name][data_source] = dict()
                    column_null_count[table_name][data_source] = dict()
                    column_distinct_values[table_name][data_source] = dict()
                    row_count[table_name][data_source] = 0

            # Track all tables that will be affected by updates.

            affected_tables = set()

            if len( count_columns ) > len( count_column_as ):
                
                # We sometimes don't count anything (e.g. file_describes_subject, in which case both lengths above are zero) or don't count anything local (e.g. dicom_series, in which case the lengths above are equal), hence this check.

                affected_tables.add( table_name )

            for column_name in count_columns:
                
                if column_name not in count_column_as:
                    
                    # 'CDA' counts everything. If this column has no data for 'CDA', we haven't recorded anything for it yet.

                    if column_name not in column_has_nulls[table_name]['CDA']:
                        
                        for data_source in extraction_date:
                            
                            column_has_nulls[table_name][data_source][column_name] = False
                            column_all_numbers[table_name][data_source][column_name] = True
                            column_null_count[table_name][data_source][column_name] = 0
                            column_distinct_values[table_name][data_source][column_name] = set()

                else:
                    
                    virtual_table = count_column_as[column_name]

                    affected_tables.add( virtual_table )

                    if virtual_table not in column_has_nulls:
                        
                        # We've not yet recorded any data for this table.

                        column_has_nulls[virtual_table] = dict()
                        column_all_numbers[virtual_table] = dict()
                        column_null_count[virtual_table] = dict()
                        column_distinct_values[virtual_table] = dict()
                        row_count[virtual_table] = dict()

                        for data_source in extraction_date:
                            
                            column_has_nulls[virtual_table][data_source] = dict()
                            column_all_numbers[virtual_table][data_source] = dict()
                            column_null_count[virtual_table][data_source] = dict()
                            column_distinct_values[virtual_table][data_source] = dict()
                            row_count[virtual_table][data_source] = 0

                    # 'CDA' counts everything. If this column has no data for 'CDA', we haven't recorded anything for it yet.

                    if column_name not in column_has_nulls[virtual_table]['CDA']:
                        
                        for data_source in extraction_date:
                            
                            column_has_nulls[virtual_table][data_source][column_name] = False
                            column_all_numbers[virtual_table][data_source][column_name] = True
                            column_null_count[virtual_table][data_source][column_name] = 0
                            column_distinct_values[virtual_table][data_source][column_name] = set()

            # Keep all tracking structures on par with the number of (virtual) `file` rows being tracked despite no 'type' column being present in `dicom_series`: we'll adjust this info later after loading the relevant data from `dicom_series_type`.

            if table_name == 'dicom_series':
                
                # Note (safe) assumption: `affected_tables` has exactly one element for 'dicom_series': 'file'.

                for affected_table in affected_tables:
                    
                    for data_source in column_has_nulls[affected_table]:
                        
                        if 'type' not in column_null_count[affected_table][data_source]:
                            
                            column_null_count[affected_table][data_source]['type'] = 0
                            column_has_nulls[affected_table][data_source]['type'] = True
                            column_all_numbers[affected_table][data_source]['type'] = True
                            column_distinct_values[affected_table][data_source]['type'] = set()

            # Scan {table_name} rows.

            for next_line in IN:
                
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                # Identify which upstream data sources contributed to this record (of the ones we're tracking based on the version and extraction-date information we received as arguments).

                current_data_sources = set()

                # Data source 'CDA' counts everything.

                current_data_sources.add( 'CDA' )

                for data_source_column in data_source_column_to_source:
                    
                    if record[data_source_column].lower() == 'true':
                        
                        current_data_sources.add( data_source_column_to_source[data_source_column] )

                # Save alias<->data_source information for second-pass association table processing.

                current_alias = record['id_alias']

                if table_name in alias_to_data_source:
                    
                    if current_alias in alias_to_data_source[table_name]:
                        
                        sys.exit( f"FATAL: We've seen alias {current_alias} in {table_name} more than once. Please handle; aborting." )

                    alias_to_data_source[table_name][current_alias] = set()

                    for data_source in current_data_sources:
                        
                        alias_to_data_source[table_name][current_alias].add( data_source )

                # Keep track of the number of files this row represents.

                current_dicom_series_instance_count = 1

                if table_name == 'dicom_series':
                    
                    current_dicom_series_instance_count = int( record['instance_count'] )

                    dicom_series_instance_count[current_alias] = current_dicom_series_instance_count

                for data_source in current_data_sources:
                    
                    for affected_table in affected_tables:
                        
                        # Safe assumption (Sep 2024): affected_tables contains no association tables. If it did, this would overcount.

                        row_count[affected_table][data_source] = row_count[affected_table][data_source] + current_dicom_series_instance_count

                        # Keep all tracking structures on par with the number of (virtual) `file` rows being tracked despite no 'type' column being present in `dicom_series`: we'll adjust this info later after loading the relevant data from `dicom_series_type`.

                        if table_name == 'dicom_series':
                            
                            column_null_count[affected_table][data_source]['type'] = column_null_count[affected_table][data_source]['type'] + current_dicom_series_instance_count

                for column_name in count_columns:
                    
                    value = record[column_name]

                    affected_table = table_name

                    if column_name in count_column_as:
                        
                        affected_table = count_column_as[column_name]

                    for data_source in current_data_sources:
                        
                        if value == '':
                            
                            column_has_nulls[affected_table][data_source][column_name] = True

                            column_null_count[affected_table][data_source][column_name] = column_null_count[affected_table][data_source][column_name] + current_dicom_series_instance_count

                        else:
                            
                            column_distinct_values[affected_table][data_source][column_name].add( value )

                            if re.search( r'^-?\d+(\.\d+([eE]-?\d+)?)?$', value ) is None:
                                
                                column_all_numbers[affected_table][data_source][column_name] = False

            IN.close()

            for affected_table in sorted(column_null_count):
                
                for data_source in sorted(column_null_count[affected_table]):
                    
                    for column_name in sorted(column_null_count[affected_table][data_source]):
                        
                        # print( f"{data_source}.{affected_table}.{column_name} == {column_null_count[affected_table][data_source][column_name]}" )

                        pass

            print( 'done.', file=sys.stderr )

# Second pass: scan association tables, using maps between aliases in referenced tables and data sources to count and bin values.

for input_file_path in sorted( input_files ):
    
    file_label = display_filename[input_file_path]

    if re.search( r'\.tsv', file_label ) is not None:
        
        table_name = re.sub( r'\.tsv(?:\.gz)?$', r'', file_label )

        if table_name in association_tables:
            
            print( f"   [{get_current_timestamp()}] Processing {table_name}...", end='', file=sys.stderr )

            IN = open( input_file_path )

            if re.search( r'\.tsv\.gz$', input_file_path ) is not None:
                
                # Sometimes things are gzipped.

                IN.close()

                IN = gzip.open( input_file_path, 'rt' )

            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            # We can't count everything (e.g. file.checksum_value for 45M [virtual] files). Use get_column_metadata() to decide what and how to count.

            count_columns = list()

            count_column_as = dict()

            for column_name in column_names:
                
                column_result = get_column_metadata( table_name, column_name )

                if ( table_name == 'dicom_series_type' and column_name == 'type' ) or \
                   ( 'column_type' in column_result and column_result['column_type'] == 'categorical' and 'fetch_rows_returns' in column_result and column_result['fetch_rows_returns'] == True ):
                    
                    count_columns.append( column_name )

                    if 'process_before_display' in column_result and ( column_result['process_before_display'] in { 'dicom_series_type', 'virtual_field', 'virtual_list' } ):
                        
                        count_column_as[column_name] = column_result['virtual_table']

            # Track all tables that will be affected by updates. All counted columns in this pass will
            # reference other tables than those in which they appear, hence the iteration on `count_column_as`.

            affected_tables = set()

            for column_name in count_column_as:
                
                virtual_table = count_column_as[column_name]

                affected_tables.add( virtual_table )

                if virtual_table not in column_has_nulls:
                    
                    # We've not yet recorded any data for this table. Fail and honk all the alarm horns.

                    sys.exit( f"FATAL: {virtual_table} not present in data tracking structures prior to association table pass: cannot continue, please fix." )

                # 'CDA' counts everything. If this column has no data for 'CDA', we haven't recorded anything for it yet.

                if column_name not in column_has_nulls[virtual_table]['CDA']:
                    
                    for data_source in extraction_date:
                        
                        column_has_nulls[virtual_table][data_source][column_name] = False
                        column_all_numbers[virtual_table][data_source][column_name] = True
                        # If this number doesn't exist yet, we need a value we can handle differently from the earlier default of 0, which means "table was scanned and no nulls were found" if it exists at this point:
                        column_null_count[virtual_table][data_source][column_name] = -1
                        column_distinct_values[virtual_table][data_source][column_name] = set()

            # Scan {table_name} rows.

            non_null_ids = dict()

            non_null_dicom_series_type_rows = dict()

            referenced_table = association_tables[table_name]

            for next_line in IN:
                
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                alias_field_name = f"{referenced_table}_alias"

                current_alias = record[alias_field_name]

                # Identify which upstream data sources contributed to this record (of the ones we're tracking based on the version and extraction-date information we received as arguments).

                current_data_sources = set()

                for data_source in alias_to_data_source[referenced_table][current_alias]:
                    
                    current_data_sources.add( data_source )

                current_dicom_series_instance_count = 1

                if alias_field_name == 'dicom_series_alias':
                    
                    current_dicom_series_instance_count = dicom_series_instance_count[current_alias]

                for column_name in count_column_as:
                    
                    value = record[column_name]

                    affected_table = count_column_as[column_name]

                    if value != '':
                        
                        for data_source in current_data_sources:
                            
                            if table_name == 'dicom_series_type' and column_name == 'type':
                                
                                # We've got instance counts per value in this table. Add those.

                                if data_source not in non_null_dicom_series_type_rows:
                                    
                                    non_null_dicom_series_type_rows[data_source] = 0

                                non_null_dicom_series_type_rows[data_source] = non_null_dicom_series_type_rows[data_source] + int( record['instance_count'] )

                            else:
                                
                                if affected_table not in non_null_ids:
                                    
                                    non_null_ids[affected_table] = dict()

                                if data_source not in non_null_ids[affected_table]:
                                    
                                    non_null_ids[affected_table][data_source] = dict()

                                if column_name not in non_null_ids[affected_table][data_source]:
                                    
                                    non_null_ids[affected_table][data_source][column_name] = set()

                                non_null_ids[affected_table][data_source][column_name].add( current_alias )

                            column_distinct_values[affected_table][data_source][column_name].add( value )

                            if re.search( r'^-?\d+(\.\d+([eE]-?\d+)?)?$', value ) is None:
                                
                                column_all_numbers[affected_table][data_source][column_name] = False

                    else:
                        
                        # This shouldn't happen.

                        sys.exit( f"FATAL: Null value encountered in association table '{table_name}' for {alias_field_name}={current_alias}; please fix, aborting." )

            IN.close()

            for column_name in count_column_as:
                
                affected_table = count_column_as[column_name]

                for data_source in row_count[affected_table]:
                    
                    if table_name == 'dicom_series_type' and column_name == 'type' and data_source in non_null_dicom_series_type_rows:
                        
                        if column_null_count[affected_table][data_source][column_name] == -1:
                            
                            # We haven't seen this field prior to scanning this association table.

                            # ...which should never happen.

                            sys.exit( f"FATAL: column_null_count[{affected_table}][{data_source}][{column_name}] is -1; something has gone wrong." )

                        else:
                            
                            # column_null_count[affected_table][data_source][column_name] already exists. By design, this total includes one (initial) null for every dicom_series_instance.

                            previous_null_count = column_null_count[affected_table][data_source][column_name]

                            total_rows = row_count[affected_table][data_source]

                            previous_non_null_rows = total_rows - previous_null_count

                            new_non_null_rows = previous_non_null_rows + non_null_dicom_series_type_rows[data_source]

                            if new_non_null_rows > total_rows:
                                
                                # This is a hacky sanity check, but this code can be quite confusing, so it seems prudent to have this here just in case.
                                # 
                                # ...also, I've tripped it several times.

                                sys.exit( f"FATAL: '{affected_table}.{column_name}' from {data_source} computed to have {new_non_null_rows} non-null rows of {total_rows} total: please fix. previous_null_count:{previous_null_count}" )

                            new_null_count = total_rows - new_non_null_rows

                            column_null_count[affected_table][data_source][column_name] = new_null_count

                    elif affected_table in non_null_ids and data_source in non_null_ids[affected_table] and column_name in non_null_ids[affected_table][data_source]:
                        
                        non_null_rows_to_add = len( non_null_ids[affected_table][data_source][column_name] )

                        if re.search( r'^dicom_series', table_name ) is not None:
                            
                            non_null_rows_to_add = 0

                            for series_alias in non_null_ids[affected_table][data_source][column_name]:
                                
                                non_null_rows_to_add = non_null_rows_to_add + dicom_series_instance_count[series_alias]

                        if column_null_count[affected_table][data_source][column_name] == -1:
                            
                            # We haven't seen this field prior to scanning this association table.

                            total_rows = row_count[affected_table][data_source]

                            column_null_count[affected_table][data_source][column_name] = total_rows - non_null_rows_to_add

                        else:
                            
                            # column_null_count[affected_table][data_source][column_name] already exists. We assume that the non-null rows counted in this association table are disjoint from those already counted.

                            previous_null_count = column_null_count[affected_table][data_source][column_name]

                            total_rows = row_count[affected_table][data_source]

                            previous_non_null_rows = total_rows - previous_null_count

                            new_non_null_rows = previous_non_null_rows + non_null_rows_to_add

                            if new_non_null_rows > total_rows:
                                
                                # This is a hacky sanity check, but this code can be quite confusing, so it seems prudent to have this here just in case.
                                # 
                                # ...also, I've tripped it several times.

                                sys.exit( f"FATAL: '{affected_table}.{column_name}' computed to have {new_non_null_rows} non-null rows of {total_rows} total: please fix." )

                            new_null_count = total_rows - new_non_null_rows

                            column_null_count[affected_table][data_source][column_name] = new_null_count

                    if column_null_count[affected_table][data_source][column_name] == 0:
                        
                        column_has_nulls[affected_table][data_source][column_name] = False

                    else:
                        
                        column_has_nulls[affected_table][data_source][column_name] = True

            print( 'done.', file=sys.stderr )

with open( output_file, 'w' ) as OUT:
    
    print( *['table_name', 'column_name', 'data_source', 'data_source_version', 'data_source_extraction_date', 'distinct_non_null_values', 'number_of_data_rows', 'has_nulls', 'number_of_rows_with_nulls', 'numeric_column' ], sep='\t', end='\n', file=OUT )

    for table_name in column_has_nulls:
        
        for column_name in column_has_nulls[table_name]['CDA']:
            
            for data_source in sorted( extraction_date ):
                
                has_nulls = str( column_has_nulls[table_name][data_source][column_name] )
                is_all_numbers = str( column_all_numbers[table_name][data_source][column_name] )
                null_count = column_null_count[table_name][data_source][column_name]
                distinct_values = str( len( column_distinct_values[table_name][data_source][column_name] ) )

                if row_count[table_name][data_source] == 0:
                    
                    has_nulls = 'NA'
                    is_all_numbers = 'NA'
                    null_count = 'NA'
                    distinct_values = 'NA'

                if column_null_count[table_name][data_source][column_name] == row_count[table_name][data_source]:
                    
                    is_all_numbers = 'NA'

                print( *[ table_name, column_name, data_source, version_string[data_source], extraction_date[data_source], distinct_values, str( row_count[table_name][data_source] ), has_nulls, null_count, is_all_numbers ], sep='\t', end='\n', file=OUT )



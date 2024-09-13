#!/usr/bin/env python -u

import gzip, re, sys

from os import listdir, makedirs, path

from cda_etl.lib import get_current_timestamp

# SUBROUTINE

def find_input_tsvs( scan_dir ):
    
    found_tsvs = set()

    for basename in listdir( scan_dir ):
        
        # Ignore files and directories beginning with '__' by convention.

        if re.search( r'^__', basename ) is None:
            
            usable_path = path.join( scan_dir, basename )

            if path.isdir( usable_path ):
                
                found_tsvs = found_tsvs | find_input_tsvs( usable_path )

            elif re.search( r'\.tsv', basename ) is not None:
                
                found_tsvs.add( path.join( scan_dir, basename ) )

    return found_tsvs

# ARGUMENTS

if len( sys.argv ) != 3:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <TSV directory to scan> <desired output file>\n" )

input_root = sys.argv[1]

output_file = sys.argv[2]

match_list = re.findall( r'^(.*)\/[^\/]+$', output_file )

output_dir = '.'

if len( match_list ) > 0:
    
    output_dir = match_list[0]

# Don't scan CDA release-metadata files if they've been built out of sequence.

skip_files = {
    
    'column_metadata.tsv',
    'release_metadata.tsv'
}

# Don't attempt to load distinct value sets for certain columns covering tens of millions of DICOM files.

skip_columns = {
    
    'dicom_instance': {
        
        'SOPInstanceUID',
        'gcs_url',
        'crdc_instance_uuid',
        'instance_size',
        'instance_hash'
    },
    'dicom_series_instance': {
        
        'id',
        'crdc_id',
        'name',
        'drs_uri',
        'size',
        'checksum_value'
    }
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Locate input TSVs.

input_files = find_input_tsvs( input_root )

display_filename = dict()

for input_file in input_files:
    
    if input_file not in skip_files:
        
        display_filename[input_file] = re.sub( r'^' + re.escape( input_root ) + r'\/?', r'', input_file )

with open( output_file, 'w' ) as OUT:
    
    print( *['table_name', 'column_name', 'distinct_non_null_values', 'total_data_rows', 'has_nulls', 'number_of_rows_with_nulls', 'numeric_column' ], sep='\t', end='\n', file=OUT )

    for input_file_path in sorted( input_files ):
        
        file_label = display_filename[input_file_path]

        if file_label not in skip_files and re.search( r'\.tsv', file_label ) is not None:
            
            print( f"   [{get_current_timestamp()}] Processing {file_label}...", end='', file=sys.stderr )

            table = re.sub( r'\.tsv(\.gz)?$', r'', file_label )

            IN = open( input_file_path )

            if re.search( r'\.tsv\.gz$', file_label ) is not None:
                
                # Sometimes things are gzipped.

                IN.close()

                IN = gzip.open( input_file_path, 'rt' )
                
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            column_has_nulls = dict( zip( column_names, [ False ] * len( column_names ) ) )

            column_all_numbers = dict( zip( column_names, [ True ] * len( column_names ) ) )

            column_null_count = dict( zip( column_names, [ 0 ] * len( column_names ) ) )

            column_distinct_values = dict( zip( column_names, [ set() for column_name in column_names ] ) )

            row_count = 0

            for next_line in IN:
                
                line = next_line.rstrip( '\n' )
                
                row_count = row_count + 1

                values = line.split( '\t' )

                for i in range( 0, len( values ) ):
                    
                    if table not in skip_columns or column_names[i] not in skip_columns[table]:
                        
                        if values[i] == '':
                            
                            column_has_nulls[column_names[i]] = True

                            column_null_count[column_names[i]] = column_null_count[column_names[i]] + 1

                        else:
                            
                            column_distinct_values[column_names[i]].add(values[i])

                            if re.search( r'^-?\d+(\.\d+([eE]-?\d+)?)?$', values[i] ) is None:
                                
                                column_all_numbers[column_names[i]] = False

            for column_name in column_names:
                
                if table not in skip_columns or column_name not in skip_columns[table]:
                    
                    has_nulls = str( column_has_nulls[column_name] )

                    is_all_numbers = str( column_all_numbers[column_name] )

                    if row_count == 0:
                        
                        has_nulls = 'NA'
                        is_all_numbers = 'NA'

                    if column_null_count[column_name] == row_count:
                        
                        is_all_numbers = 'NA'

                    print( *[ f"{file_label}", column_name, str( len( column_distinct_values[column_name] ) ), str( row_count ), has_nulls, column_null_count[column_name], is_all_numbers ], sep='\t', end='\n', file=OUT )

            print( 'done.', file=sys.stderr )



#!/usr/bin/env python3 -u

import gzip
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import get_current_timestamp

# ARGUMENT

if len( sys.argv ) != 5:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <data source label, e.g. 'GDC'> <most recent aggregated TSV dir> <harmonized CDA TSV dir> <target output directory for decorated harmonized CDA TSVs>\n" )

data_source_label = sys.argv[1].lower()

last_merge_dir = sys.argv[2]

harmonized_tsv_dir = sys.argv[3]

decorated_harmonized_tsv_dir = sys.argv[4]

# PARAMETERS

debug=False

# New fields.

new_suffix_column_names = [
    
    'data_at_cds',
    'data_at_gdc',
    'data_at_icdc',
    'data_at_idc',
    'data_at_pdc',
    'data_source_count'
]

# Tables to which we want to add data_* columns and id_alias values.

base_tables = {
    
    'project',
    'file',
    'subject',
    'observation',
    'treatment',
    'mutation'
}

# Tables in which we want to substitute id_alias values for ids in
# foreign key fields (and the name and home table for each affected
# FK).

reference_tables = {
    
    'dicom_series_instance': [
        {
            'referencing_field': 'series_alias',
            'referenced_table': 'file',
            'referenced_field': 'id_alias'
        }
    ],
    'observation': [
        {
            'referencing_field': 'subject_alias',
            'referenced_table': 'subject',
            'referenced_field': 'id_alias'
        }
    ],
    'treatment': [
        {
            'referencing_field': 'subject_alias',
            'referenced_table': 'subject',
            'referenced_field': 'id_alias'
        }
    ],
    'mutation': [
        {
            'referencing_field': 'subject_alias',
            'referenced_table': 'subject',
            'referenced_field': 'id_alias'
        }
    ],
    'upstream_identifiers': [
        {
            'referencing_field': 'id_alias',
            'referenced_table': 'project,subject', # This will be the value of each row's `cda_table` field, which will vary across rows.
            'referenced_field': 'id_alias'
        }
    ],
    'file_describes_subject': [
        {
            'referencing_field': 'file_alias',
            'referenced_table': 'file',
            'referenced_field': 'id_alias'
        },
        {
            'referencing_field': 'subject_alias',
            'referenced_table': 'subject',
            'referenced_field': 'id_alias'
        }
    ],
    'file_anatomic_site': [
        {
            'referencing_field': 'file_alias',
            'referenced_table': 'file',
            'referenced_field': 'id_alias'
        }
    ],
    'file_tumor_vs_normal': [
        {
            'referencing_field': 'file_alias',
            'referenced_table': 'file',
            'referenced_field': 'id_alias'
        }
    ],
    'file_in_project': [
        {
            'referencing_field': 'file_alias',
            'referenced_table': 'file',
            'referenced_field': 'id_alias'
        },
        {
            'referencing_field': 'project_alias',
            'referenced_table': 'project',
            'referenced_field': 'id_alias'
        }
    ],
    'project_in_project': [
        {
            'referencing_field': 'child_project_alias',
            'referenced_table': 'project',
            'referenced_field': 'id_alias'
        },
        {
            'referencing_field': 'parent_project_alias',
            'referenced_table': 'project',
            'referenced_field': 'id_alias'
        }
    ],
    'subject_in_project': [
        {
            'referencing_field': 'subject_alias',
            'referenced_table': 'subject',
            'referenced_field': 'id_alias'
        },
        {
            'referencing_field': 'project_alias',
            'referenced_table': 'project',
            'referenced_field': 'id_alias'
        }
    ]
}

# EXECUTION

for output_dir in [ decorated_harmonized_tsv_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Update { base_tables } \ { reference_tables } first so id_alias values exist when we need them.
# 
# Save output locations by table name for later reference.

print( f"[{get_current_timestamp()}] Updating core base tables with ID aliases...", end='\n\n', file=sys.stderr )

output_files_by_table = dict()

for file_basename in sorted( listdir( harmonized_tsv_dir ) ):
    
    if re.search( r'\.tsv', file_basename ) is not None:
        
        table = re.sub( r'^(.*)\.tsv', r'\1', file_basename )

        if table in base_tables and table not in reference_tables:
            
            print( f"   [{get_current_timestamp()}] ...{table}...", file=sys.stderr )

            old_table_file = path.join( harmonized_tsv_dir, file_basename )

            new_table_file = path.join( decorated_harmonized_tsv_dir, file_basename )

            output_files_by_table[table] = new_table_file

            if debug:
                
                print( f"table: {table}", file=sys.stderr )

                print( f"old file: {old_table_file}", file=sys.stderr )

                print( f"new file: {new_table_file}", end='\n\n', file=sys.stderr )

            # If it exists, load the maximum already-assigned id_alias value.

            max_existing_alias = -1

            if path.exists( last_merge_dir ):
                
                # Sometimes new versions of plaintext antecedent TSVs are gzipped. Handle that.

                if re.search( r'\.tsv\.gz$', file_basename ) is not None:
                    
                    alt_basename = re.sub( r'\.gz$', r'', alt_basename )

                if not path.exists( path.join( last_merge_dir, file_basename ) ):
                    
                    file_basename = alt_basename

                merged_table_file = path.join( last_merge_dir, file_basename )

                if path.exists( merged_table_file ):
                    
                    IN = open( merged_table_file )

                    if re.search( r'\.tsv\.gz$', merged_table_file ) is not None:
                        
                        IN.close()

                        IN = gzip.open( merged_table_file, 'rt' )

                    colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                        
                        input_record = dict( zip( colnames, line.split( '\t' ) ) )

                        current_alias = int( input_record['id_alias'] )

                        if current_alias > max_existing_alias:
                            
                            max_existing_alias = current_alias

                elif debug:
                    
                    print( f"DEBUG NOTE (base table pass): (may not be pathological) Can't find sought-for '{merged_table_file}' in '{last_merge_dir}'; skipping alias-index update.", file=sys.stderr )
    
            next_alias = max_existing_alias + 1

            # Get one same-named filehandle variable for each input file, whether or not it's gzipped.

            IN = open( old_table_file )

            OUT = open( new_table_file, 'w' )

            if re.search( r'\.tsv\.gz$', file_basename ) is not None:
                
                # Sometimes things are gzipped.

                IN.close()

                OUT.close()

                IN = gzip.open( old_table_file, 'rt' )

                OUT = gzip.open( new_table_file, 'wt' )

            colnames = next( IN ).rstrip( '\n' ).split( '\t' )

            if colnames[0] != 'id':
                
                sys.exit( f"FATAL: First column of base table {table} is not 'id'; aborting." )

            output_colnames = [ 'id', 'id_alias' ] + colnames[1:] + new_suffix_column_names

            target_data_source_column = f"data_at_{data_source_label}"

            if target_data_source_column not in output_colnames:
                
                sys.exit( f"FATAL: there is no '{target_data_source_column}' -- please check the given data_source_label ('{data_source_label}'). Cannot continue." )

            print( *output_colnames, sep='\t', file=OUT )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                input_record = dict( zip( colnames, line.split( '\t' ) ) )

                output_row = [ input_record['id'] ] + [ next_alias ]

                next_alias = next_alias + 1

                for column in colnames[1:]:
                    
                    old_value = input_record[column]

                    output_row.append( old_value )

                for column in new_suffix_column_names:
                    
                    if column == 'data_source_count':
                        
                        output_row.append( 1 )

                    elif column == target_data_source_column:
                        
                        output_row.append( True )

                    else:
                        
                        output_row.append( False )

                print( *output_row, sep='\t', file=OUT )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

# Update reference tables, substituting id_alias values wherever foreign ids are found.
# 
# Don't forget! There are tables that are both base tables and reference tables. These
# must be handled here.

print( f"[{get_current_timestamp()}] Updating reference tables with ID aliases...", end='\n\n', file=sys.stderr )

for file_basename in sorted( listdir( harmonized_tsv_dir ) ):
    
    if re.search( r'\.tsv', file_basename ) is not None:
        
        table = re.sub( r'^(.*)\.tsv', r'\1', file_basename )

        if table in reference_tables:
            
            print( f"   [{get_current_timestamp()}] ...{table}...", file=sys.stderr )

            old_table_file = path.join( harmonized_tsv_dir, file_basename )

            new_table_file = path.join( decorated_harmonized_tsv_dir, file_basename )

            if debug:
                
                print( f"table: {table}", file=sys.stderr )

                print( f"old file: {old_table_file}", file=sys.stderr )

                print( f"new file: {new_table_file}", end='\n\n', file=sys.stderr )

            # Load all needed id->id_alias maps.

            fields_to_replace = dict()

            get_alias_from = dict()

            alias_field = dict()

            for reference_dict in reference_tables[table]:
                
                # 'observation': [
                #     {
                #         'referencing_field': 'subject_alias',
                #         'referenced_table': 'subject',
                #         'referenced_field': 'id_alias'
                #     }
                # ],

                new_referencing_field = reference_dict['referencing_field']

                original_referencing_field = re.sub( r'_alias$', r'_id', new_referencing_field )

                # Special case: upstream_identifiers.

                if table == 'upstream_identifiers':
                    
                    original_referencing_field = re.sub( r'^id_alias$', r'id', new_referencing_field )

                fields_to_replace[original_referencing_field] = new_referencing_field

                # `referenced_table` will have a comma in it, for upstream_identifiers.

                referenced_table = reference_dict['referenced_table']

                get_alias_from[original_referencing_field] = referenced_table

                if re.search( r',', referenced_table ) is not None:
                    
                    referenced_tables = referenced_table.split( ',' )

                    for next_referenced_table in referenced_tables:
                        
                        referenced_field = reference_dict['referenced_field']

                        alias_field[next_referenced_table] = referenced_field

                else:
                    
                    referenced_field = reference_dict['referenced_field']

                    alias_field[referenced_table] = referenced_field

            alias_map = dict()

            for alias_table in alias_field:
                
                alias_map[alias_table] = dict()

                alias_table_file = output_files_by_table[alias_table]

                IN = open( alias_table_file )

                if re.search( r'\.tsv\.gz$', alias_table_file ) is not None:
                    
                    IN.close()

                    IN = gzip.open( alias_table_file, 'rt' )

                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    input_record = dict( zip( colnames, line.split( '\t' ) ) )

                    alias_map[alias_table][input_record['id']] = input_record[alias_field[alias_table]]

            # Get one same-named filehandle variable for each input file, whether or not it's gzipped.

            IN = open( old_table_file )

            OUT = open( new_table_file, 'w' )

            if re.search( r'\.tsv\.gz$', file_basename ) is not None:
                
                # Sometimes things are gzipped.

                IN.close()

                OUT.close()

                IN = gzip.open( old_table_file, 'rt' )

                OUT = gzip.open( new_table_file, 'wt' )

            input_colnames = next( IN ).rstrip( '\n' ).split( '\t' )

            output_colnames = input_colnames.copy()

            # Rename affected *_id columns for output.

            for i in range( 0, len( output_colnames ) ):
                
                if output_colnames[i] in fields_to_replace:
                    
                    output_colnames[i] = fields_to_replace[output_colnames[i]]

            # Next: if this is also a base table, add the new columns and load the max existing id_alias value.

            target_data_source_column = f"data_at_{data_source_label}"

            max_existing_alias = -1
    
            if table in base_tables:
                
                if input_colnames[0] != 'id':
                    
                    sys.exit( f"FATAL: First column of base/reference table {table} is not 'id'; aborting." )

                output_colnames = [ 'id_alias' ] + output_colnames[1:] + new_suffix_column_names

                if target_data_source_column not in output_colnames:
                    
                    sys.exit( f"FATAL: there is no '{target_data_source_column}' -- please check the given data_source_label ('{data_source_label}'). Cannot continue." )

                # If it exists, load the maximum already-assigned id_alias value.
    
                if path.exists( last_merge_dir ):
                    
                    # Sometimes new versions of plaintext antecedent TSVs are gzipped. Handle that.
    
                    if re.search( r'\.tsv\.gz$', file_basename ) is not None:
                        
                        alt_basename = re.sub( r'\.gz$', r'', alt_basename )
    
                    if not path.exists( path.join( last_merge_dir, file_basename ) ):
                        
                        file_basename = alt_basename
    
                    merged_table_file = path.join( last_merge_dir, file_basename )
    
                    if path.exists( merged_table_file ):
                        
                        OLD = open( merged_table_file )
    
                        if re.search( r'\.tsv\.gz$', merged_table_file ) is not None:
                            
                            OLD.close()
    
                            OLD = gzip.open( merged_table_file, 'rt' )
    
                        colnames = next( OLD ).rstrip( '\n' ).split( '\t' )
    
                        for line in [ next_line.rstrip( '\n' ) for next_line in OLD ]:
                            
                            input_record = dict( zip( colnames, line.split( '\t' ) ) )
    
                            current_alias = int( input_record['id_alias'] )
    
                            if current_alias > max_existing_alias:
                                
                                max_existing_alias = current_alias

                    elif debug:
                        
                        print( f"DEBUG NOTE (reference table pass): (may not be pathological) Can't find sought-for '{merged_table_file}' in '{last_merge_dir}'; skipping alias-index update.", file=sys.stderr )
    
            next_alias = max_existing_alias + 1

            print( *output_colnames, sep='\t', file=OUT )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                input_record = dict( zip( input_colnames, line.split( '\t' ) ) )

                output_row = list()

                for column in input_colnames:
                    
                    old_value = input_record[column]

                    if table in base_tables and column == 'id':
                        
                        # observation, subject, mutation: (temporary) id -> (new) id_alias

                        output_row.append( next_alias )

                        next_alias = next_alias + 1

                    elif column in fields_to_replace:
                        
                        alias_source_table = get_alias_from[column]

                        if re.search( r',', alias_source_table ) is not None:
                            
                            # Special case: upstream_identifiers denotes the name of this table dynamically by row, using the value of its `cda_table` field.

                            alias_source_table = input_record['cda_table']

                        # This should break if there's a referential-integrity error.

                        new_alias_value = alias_map[alias_source_table][old_value]

                        output_row.append( new_alias_value )

                    else:
                        
                        output_row.append( old_value )

                if table in base_tables:
                    
                    for column in new_suffix_column_names:
                        
                        if column == 'data_source_count':
                            
                            output_row.append( 1 )

                        elif column == target_data_source_column:
                            
                            output_row.append( True )

                        else:
                            
                            output_row.append( False )

                print( *output_row, sep='\t', file=OUT )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )



import gzip
import re
import sys

from os import rename

def add_to_map( association_map, id_one, id_two ):
    
    if id_one not in association_map:
        
        association_map[id_one] = set()

    association_map[id_one].add(id_two)

def associate_id_list_with_parent( parent, parent_id, list_field_name, list_element_id_field_name, association_map, reverse_column_order=False ):
    
    if list_field_name in parent:
        
        for item in parent[list_field_name]:
            
            if list_element_id_field_name not in item:
                
                sys.exit(f"FATAL: The given list element does not have the expected {list_element_id_field_name} field; aborting.")

            child_id = item[list_element_id_field_name]

            if not reverse_column_order:
                
                add_to_map( association_map, parent_id, child_id )

            else:
                
                add_to_map( association_map, child_id, parent_id )

def get_safe_value( record, field_name ):
    
    if field_name in record:
        
        return record[field_name]

    else:
        
        sys.exit(f"FATAL: The given record does not have the expected {field_name} field; aborting.")

def load_qualified_id_association( input_file, qualifier_field_name, id_one_field_name, id_two_field_name ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
            
            values = line.split( '\t' )

            record = dict( zip( colnames, values ) )

            qualifier = record[qualifier_field_name]

            id_one = record[id_one_field_name]

            id_two = record[id_two_field_name]

            if qualifier not in result:
                
                result[qualifier] = dict()

            if id_one not in result[qualifier]:
                
                result[qualifier][id_one] = set()

            result[qualifier][id_one].add( id_two )

    return result

def load_tsv_as_dict( input_file, id_column_count=1 ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        if id_column_count == 1:
            
            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                values = line.split( '\t' )

                # First column is a unique ID column, according to id_column_count.
                # If this doesn't end up being true, only the last record will
                # be stored for any repeated ID.

                result[values[0]] = dict( zip( colnames, values ) )

        elif id_column_count < 1:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count cannot be less than 1 (your value: '{id_column_count}'). Aborting." )

        elif id_column_count > 2:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count values greater than 2 (your value: '{id_column_count}') are not currently supported. Aborting." )

        elif id_column_count != 2:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count value (your value: '{id_column_count}') must be a nonnegative integer. Aborting." )

        else:
            
            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                values = line.split( '\t' )

                key_one = values[0]

                key_two = values[1]

                if key_one not in result:
                    
                    result[key_one] = dict()

                # First two columns comprise a unique composite ID, according to id_column_count.
                # If this doesn't end up being true, only the last record will be stored for any repeated composite ID.

                result[key_one][key_two] = dict( zip( colnames, values ) )

    return result

def map_columns_one_to_one( input_file, from_field, to_field, where_field=None, where_value=None ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            passed_where = True

            if where_field is not None:
                
                passed_where = False

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == where_field:
                    
                    if where_value is not None and values[i] is not None and where_value == values[i]:
                        
                        passed_where = True

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            if passed_where:
                
                return_map[current_from] = current_to

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field, gzipped=False ):
    
    return_map = dict()

    if gzipped:
        
        IN = gzip.open( input_file, 'rt' )

    else:
        
        IN = open( input_file )

    column_names = next(IN).rstrip('\n').split('\t')

    if from_field not in column_names or to_field not in column_names:
        
        sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        values = line.split('\t')

        current_from = ''

        current_to = ''

        for i in range( 0, len( column_names ) ):
            
            if column_names[i] == from_field:
                
                current_from = values[i]

            if column_names[i] == to_field:
                
                current_to = values[i]

        if current_from not in return_map:
            
            return_map[current_from] = set()

        return_map[current_from].add(current_to)

    IN.close()

    return return_map

def singularize( name ):
    
    if name in [ 'aliquots',
                 'analytes',
                 'annotations',
                 'cases',
                 'exposures',
                 'files',
                 'follow_ups',
                 'molecular_tests',
                 'pathology_details',
                 'portions',
                 'projects',
                 'read_groups',
                 'read_group_qcs',
                 'samples',
                 'slides',
                 'treatments' ]:
        
        return re.sub(r's$', r'', name)

    elif name == 'diagnoses':
        
        return 'diagnosis'

    elif name in [ 'data_categories',
                   'family_histories',
                   'experimental_strategies' ]:
        
        return re.sub(r'ies$', r'y', name)

    else:
        
        return name

def sort_file_with_header( file_path, gzipped=False ):
    
    if not gzipped:
        
        with open( file_path ) as IN:
            
            header = next(IN).rstrip('\n')

            lines = [ line.rstrip('\n') for line in sorted( IN ) ]

        if len( lines ) > 0:
            
            with open( file_path + '.tmp', 'w' ) as OUT:
                
                print( header, sep='', end='\n', file=OUT )

                print( *lines, sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

    else:
        
        with gzip.open( file_path, 'rt' ) as IN:
            
            header = next(IN).rstrip('\n')

            lines = [ line.rstrip('\n') for line in sorted(IN) ]

        if len( lines ) > 0:
            
            with gzip.open( file_path + '.tmp', 'wt' ) as OUT:
                
                print( header, sep='', end='\n', file=OUT )

                print( *lines, sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

def update_field_lists( field_name, field_lists ):
    """
        Take a (possibly mutiply nested) field name and recursively parse it to
        find atomic substructures, then bind those to the right entity types.
    """

    if len(re.findall(r'\.', field_name)) == 1:
        
        ( entity, field ) = re.split(r'\.', field_name)

        if entity not in field_lists:
            
            field_lists[entity] = set()

        field_lists[entity].add(field)

    else:
        
        field_name = re.sub(r'^[^\.]*\.', '', field_name)

        update_field_lists(field_name, field_lists)

def write_association_pairs( association_map, tsv_filename, field_one_name, field_two_name ):
    
    sys.stderr.write(f"Making {tsv_filename}...")

    sys.stderr.flush()

    with open( tsv_filename, 'w' ) as OUT:
        
        print( *[field_one_name, field_two_name], sep='\t', file=OUT )
    
        for value_one in sorted(association_map):
            
            for value_two in sorted(association_map[value_one]):
                
                print( *[value_one, value_two], sep='\t', file=OUT )

    sys.stderr.write("done.\n")



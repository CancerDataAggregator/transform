import gzip
import re
import sys

from cda_etl.lib import get_column_metadata, get_unique_values_from_tsv_column

from os import listdir, makedirs, path, rename

class CDA_loader:
    
    def __init__( self ):
        
        self.sql_output_dir = 'SQL_data'

        # DDL schema file (extracted from previous populated DB instance) that will
        # serve as a basis for constructing and indexing the new instance we're going
        # to build using the SQL dump file this module generates.

        self.ddl_schema_file = path.join( 'ddl_schema', 'cda_database_ddl_schema.sql' )

        # List of indexes and constraints currently applied to the RDBMS. This file
        # will need to be replaced once the cloud environment is understood, as will
        # the out-of-band script that queries the PostgreSQL instance to generate it.

        self.index_and_constraint_def_file = 'indexes_and_constraints.txt'

        # Postgres configuration to invoke when parsing language blocks into lexeme vectors (tsvectors).

        self.default_text_search_config = 'english'

    def complete_file_describes_subject( self, input_dir ):
        file_tsv = path.join( input_dir, 'file.tsv' )
        subject_tsv = path.join( input_dir, 'subject.tsv' )
        file_describes_subject_tsv = path.join( input_dir, 'file_describes_subject.tsv' )
        temp_tsv = path.join( input_dir, 'file_describes_subject.temp.tsv' )

        unseen_file_aliases = set( get_unique_values_from_tsv_column( file_tsv, 'id_alias' ) )
        unseen_subject_aliases = set( get_unique_values_from_tsv_column( subject_tsv, 'id_alias' ) )

        with open( file_describes_subject_tsv ) as IN, open( temp_tsv, 'w' ) as OUT:
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )
            print( *column_names, sep='\t', file=OUT )
            for next_line in IN:
                current_record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                unseen_file_aliases.discard( current_record['file_alias'] )
                unseen_subject_aliases.discard( current_record['subject_alias'] )
                print( *[ current_record['file_alias'], current_record['subject_alias'] ], sep='\t', file=OUT )
            for file_alias in sorted( unseen_file_aliases ):
                print( *[ file_alias, '' ], sep='\t', file=OUT )
            for subject_alias in sorted( unseen_subject_aliases ):
                print( *[ '', subject_alias ], sep='\t', file=OUT )

        rename( temp_tsv, file_describes_subject_tsv )

    def make_null_TSV( self, input_dir, input_table ):
        
        input_tsv = path.join( input_dir, f"{input_table}.tsv" )

        output_tsv = path.join( input_dir, f"{input_table}_nulls.tsv" )

        print( f"Making {output_tsv}...", end='', file=sys.stderr )

        try:
            
            with open( input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                foreign_table = ''

                columns_to_keep = list()

                for colname in colnames:
                    
                    if foreign_table == '' and re.search( r'^.+_alias$', colname ) is None:
                        sys.exit( f"Cannot find foreign table from which '{input_table}' is derived; aborting. Observed columns:\n\n{colnames}\n\n" )

                    elif foreign_table == '' and colname != 'id_alias':
                        foreign_table = re.search( r'^(.+)_alias$', colname ).group(1)

                    elif foreign_table != '' and colname != 'data_source_count' and re.search( r'^data_at_.+$', colname ) is None:
                        columns_to_keep.append( colname )

                foreign_alias_column_name = f"{foreign_table}_alias"

                foreign_table_aliases = get_unique_values_from_tsv_column( path.join( input_dir, f"{foreign_table}_in_project.tsv" ), foreign_alias_column_name )

                null_data = dict()

                for next_line in IN:
                    
                    record = dict( zip( colnames, next_line.rstrip( '\n' ).split( '\t' ) ) )

                    foreign_alias = record[foreign_alias_column_name]

                    if foreign_alias not in null_data:
                        null_data[foreign_alias] = dict( zip( columns_to_keep, [ True ] * len( columns_to_keep ) ) )

                    for colname in columns_to_keep:
                        if record[colname] is not None and record[colname] != '':
                            null_data[foreign_alias][colname] = False

        except Exception as e:
            sys.exit( e )

        with open( output_tsv, 'w' ) as OUT:
            
            if input_table in { 'file_anatomic_site', 'file_tumor_vs_normal' }:
                
                # Just list one column of file_alias values whose files have none of the specified tags.
                print( 'file_alias', file=OUT )

                for file_alias in foreign_table_aliases:
                    if file_alias not in null_data:
                        print( file_alias, file=OUT )

            else:
                
                print( *( [ foreign_alias_column_name ] + [ f"{column_to_keep}_null" for column_to_keep in columns_to_keep ] ), sep='\t', file=OUT )

                for foreign_alias in foreign_table_aliases:
                    
                    if foreign_alias not in null_data:
                        
                        # We never saw this `foreign_alias`. It has no records in `input_table`.
                        print( *( [ foreign_alias ] + ( [ True ] * len( columns_to_keep ) ) ), sep='\t', file=OUT )

                    else:
                        
                        print( *( [ foreign_alias ] + [ null_data[foreign_alias][column_to_keep] for column_to_keep in columns_to_keep ] ), sep='\t', file=OUT )

        print( 'done.', file=sys.stderr )

    def __flatten_ancestry( self, ancestry_map, target_id ):
        # Return a set of all ancestors, allowing for multiple parents (i.e. processing ancestry as a DAG, not a tree).
        if target_id not in ancestry_map:
            # A root node (assuming we haven't been passed meaningless junk,
            # which is a question that cannot be resolved within the scope of
            # this function).
            return set()
        else:
            return_set = set( ancestry_map[target_id] )
            for target_parent in ancestry_map[target_id]:
                return_set = return_set | self.__flatten_ancestry( ancestry_map, target_parent )
            return return_set

    def collect_keyword_data( self, input_dir, text_fields, exact_match_fields ):
        
        # Find out which columns are harmonized.
        column_metadata = get_column_metadata()
        harmonized_fields = dict()

        for table_name in sorted( column_metadata ):
            # Python 3 preserves insert order for dicts. That means column data will be displayed
            # in the order in which columns are listed in the definition (in lib.py) of
            # get_column_metadata(). Handy. Also worth noting because it's not obvious.
            for column_name in column_metadata[table_name]:
                current_record = column_metadata[table_name][column_name]
                if 'concept' in current_record and current_record['concept'] is not None and current_record['concept'] != '':
                    if table_name not in harmonized_fields:
                        harmonized_fields[table_name] = dict()
                    harmonized_fields[table_name][column_name] = current_record['concept']

        # Load controlled-term data.
        controlled_term_tsv = path.join( input_dir, 'controlled_term.tsv' )
        containing_term_tsv = path.join( input_dir, 'containing_term.tsv' )
        slim_term_tsv = path.join( input_dir, 'slim_term.tsv' )
        synonym_term_tsv = path.join( input_dir, 'synonym_term.tsv' )

        alias_to_id = dict()
        alias_to_name = dict()
        containing_terms = dict()
        slim_terms = dict()
        synonym_terms = dict()

        with open( controlled_term_tsv ) as IN:
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                # Initialize upcoming data structures for each term.
                containing_terms[record['id_alias']] = set()
                slim_terms[record['id_alias']] = set()
                synonym_terms[record['id_alias']] = set()

                if record['id'] is not None and record['id'] != '':
                    alias_to_id[record['id_alias']] = record['id']
                if record['name'] is not None and record['name'] != '':
                    alias_to_name[record['id_alias']] = record['name']

        with open( containing_term_tsv ) as IN:
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )
            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                containing_terms[record['specific_term_alias']].add( record['general_term_alias'] )

        with open( slim_term_tsv ) as IN:
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )
            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                slim_terms[record['specific_term_alias']].add( record['general_term_alias'] )

        # Yes, these are directional and paired (as opposed to clustered into groups of more than two).
        # All of that's often up to upstream ontology curators, and we're not going to presume
        # bidirectionality (or, worse, transitivity) in cases when it could be expressed but isn't.
        with open( synonym_term_tsv ) as IN:
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )
            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                synonym_terms[record['synonym_one_alias']].add( record['synonym_two_alias'] )

        for entity_to_describe in sorted( exact_match_fields ):
            
            print( f"Collating {entity_to_describe}-associated keyword literals including synonyms and containers for controlled terms...", end='', file=sys.stderr )

            target_tables = set()

            for target_field in sorted( exact_match_fields[entity_to_describe] ):
                match_result = re.search( r'^([^\.]+)\.', target_field )
                if match_result is None:
                    sys.exit( f"FATAL: Cannot parse table name from received `exact_match_fields[{entity_to_describe}]` parameter '{target_field}'; cannot continue. Please reconfigure and retry." )
                else:
                    target_table = match_result.group(1)
                    target_tables.add( target_table )

            # This may require caching if the data we're collating gets prohibitively large, but for the foreseeable future, we should be good. (Jan 2026)
            keyword_list = set()
            associated_keyword_data = dict()

            for target_table in sorted( target_tables ):
                if target_table in [ 'mutation' ]:
                    IN = gzip.open( path.join( input_dir, f"{target_table}.tsv.gz" ), 'rt' )
                else:
                    IN = open( path.join( input_dir, f"{target_table}.tsv" ) )
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )
                for next_line in IN:
                    record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                    current_id = None
                    if f"{entity_to_describe}_alias" in column_names:
                        current_id = record[f"{entity_to_describe}_alias"]
                    else:
                        # We want this to break if there's an access error: one or more key assumptions isn't correct if that happens.
                        current_id = record['id_alias']
                    for column_name in column_names:
                        if record[column_name] != '' and f"{target_table}.{column_name}" in exact_match_fields[entity_to_describe]:
                            if current_id not in associated_keyword_data:
                                associated_keyword_data[current_id] = set()
                            # harmonized_fields[table_name][column_name] = current_record['concept']
                            if target_table not in harmonized_fields or column_name not in harmonized_fields[target_table]:
                                associated_keyword_data[current_id].add( record[column_name] )
                                keyword_list.add( record[column_name] )
                            else:
                                # This column is harmonized. Resolve indirection and add in containing terms, slims and synonyms.
                                base_term_alias = record[column_name]
                                terms_to_scan = { base_term_alias } | self.__flatten_ancestry( containing_terms, base_term_alias ) | slim_terms[base_term_alias] | synonym_terms[base_term_alias]
                                for term_to_scan in sorted( terms_to_scan ):
                                    if term_to_scan not in alias_to_id and term_to_scan not in alias_to_name:
                                        sys.exit( f"FATAL: Controlled term ID {term_to_scan} has neither an ID nor a name. Please handle." )
                                    if term_to_scan in alias_to_id:
                                        associated_keyword_data[current_id].add( alias_to_id[term_to_scan] )
                                        keyword_list.add( alias_to_id[term_to_scan] )
                                    if term_to_scan in alias_to_name:
                                        associated_keyword_data[current_id].add( alias_to_name[term_to_scan] )
                                        keyword_list.add( alias_to_name[term_to_scan] )

            keyword_list_file = path.join( input_dir, f"{entity_to_describe}_keywords.tsv" )

            print( f"done.\nWriting {keyword_list_file}...", end='', file=sys.stderr )

            keyword_id_alias = dict()

            with open( keyword_list_file, 'w' ) as OUT:
                print( *[ 'id_alias', 'keyword' ], sep='\t', file=OUT )
                current_id_alias = 1
                for keyword in sorted( keyword_list ):
                    keyword_id_alias[keyword] = current_id_alias
                    print( *[ current_id_alias, keyword ], sep='\t', file=OUT )
                    current_id_alias = current_id_alias + 1

            keyword_association_file = path.join( input_dir, f"keyword_describes_{entity_to_describe}.tsv" )

            print( f"done.\nWriting {keyword_association_file}...", end='', file=sys.stderr )

            with open( keyword_association_file, 'w' ) as OUT:
                print( *[ 'keyword_alias', f"{entity_to_describe}_alias" ], sep='\t', file=OUT )
                for current_id in sorted( associated_keyword_data ):
                    for keyword in sorted( associated_keyword_data[current_id] ):
                        print( *[ keyword_id_alias[keyword], current_id ], sep='\t', file=OUT )

            print( 'done.', file=sys.stderr )

        for entity_to_describe in sorted( text_fields ):
            
            print( f"Collating {entity_to_describe}-associated lexeme blocks...", end='', file=sys.stderr )

            target_tables = set()

            for target_field in sorted( text_fields[entity_to_describe] ):
                match_result = re.search( r'^([^\.]+)\.', target_field )
                if match_result is None:
                    sys.exit( f"FATAL: Cannot parse table name from received `text_fields[{entity_to_describe}]` parameter '{target_field}'; cannot continue. Please reconfigure and retry." )
                else:
                    target_table = match_result.group(1)
                    target_tables.add( target_table )

            # This may require caching if the data we're collating gets prohibitively large, but for the foreseeable future, we should be good. (Jan 2026)

            associated_text_data = dict()

            for target_table in sorted( target_tables ):
                if target_table in [ 'mutation' ]:
                    IN = gzip.open( path.join( input_dir, f"{target_table}.tsv.gz" ), 'rt' )
                else:
                    IN = open( path.join( input_dir, f"{target_table}.tsv" ) )
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )
                for next_line in IN:
                    record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                    current_id = None
                    if f"{entity_to_describe}_alias" in column_names:
                        current_id = record[f"{entity_to_describe}_alias"]
                    else:
                        # We want this to break if there's an access error: one or more key assumptions isn't correct if that happens.
                        current_id = record['id_alias']
                    for column_name in column_names:
                        if record[column_name] != '' and f"{target_table}.{column_name}" in text_fields[entity_to_describe]:
                            # harmonized_fields[table_name][column_name] = current_record['concept']
                            if target_table not in harmonized_fields or column_name not in harmonized_fields[target_table]:
                                if current_id not in associated_text_data:
                                    associated_text_data[current_id] = record[column_name]
                                else:
                                    associated_text_data[current_id] = f"{associated_text_data[current_id]} {record[column_name]}"
                            else:
                                # This column is harmonized. Resolve indirection and add in containing terms, slims and synonyms.
                                base_term_alias = record[column_name]
                                terms_to_scan = { base_term_alias } | self.__flatten_ancestry( containing_terms, base_term_alias ) | slim_terms[base_term_alias] | synonym_terms[base_term_alias]
                                for term_to_scan in sorted( terms_to_scan ):
                                    # (Only load lexemes from term names, not term IDs.)
                                    if term_to_scan in alias_to_name:
                                        if current_id not in associated_text_data:
                                            associated_text_data[current_id] = alias_to_name[term_to_scan]
                                        else:
                                            associated_text_data[current_id] = f"{associated_text_data[current_id]} {alias_to_name[term_to_scan]}"

            output_file = path.join( input_dir, f"{entity_to_describe}_text_search_inputs.tsv" )

            print( f"done.\nWriting {output_file}...", end='', file=sys.stderr )

            with open( output_file, 'w' ) as OUT:
                print( *[ f"{entity_to_describe}_alias", 'search_vector_input' ], sep='\t', file=OUT )
                for current_id in sorted( associated_text_data ):
                    print( *[ current_id, associated_text_data[current_id] ], sep='\t', file=OUT )

            print( 'done.', file=sys.stderr )

    def transform_dir_to_SQL( self, input_dir ):
        
        # THIS FUNCTION IS OBSOLETE AND OUT OF SYNC AS OF THE ADDITION OF text_search SUPPORT
        print( 'Transforming CDA TSVs to SQL...', file=sys.stderr )

        for target_dir in [ self.sql_output_dir ]:
            if not path.isdir( target_dir ):
                makedirs( target_dir )

        preprocess_command_file = path.join( self.sql_output_dir, 'clear_table_data_indices_and_constraints.sql' )

        table_file_dir = path.join( self.sql_output_dir, 'new_table_data' )

        postprocess_command_file = path.join( self.sql_output_dir, 'rebuild_indices_and_constraints.sql' )

        for output_dir in [ table_file_dir ]:
            
            if not path.exists( output_dir ):
                
                makedirs( output_dir )

        # Drop all indexes and constraints prior to data refresh, then rebuild
        # after data rows have been inserted. Avoids validation overhead during insertion.

        preprocess_unique_constraints = list()

        preprocess_foreign_keys = list()

        preprocess_primary_keys = list()

        preprocess_indexes = list()

        postprocess_unique_constraints = list()

        postprocess_foreign_keys = list()

        postprocess_primary_keys = list()

        postprocess_indexes = list()

        with open( self.index_and_constraint_def_file ) as IN:
            
            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                # Ignore indexes built automatically as side effects of key constraint definitions. Sorry about the
                # awkward composition of logical operators, but the terser version (without the 'not' wrapping the whole
                # thing) is really hard to understand without getting snagged for 5 minutes when trying to read
                # this code.

                if ( not( re.search( r'CREATE UNIQUE INDEX', line ) is not None and re.search( r'_p?key ', line ) is not None ) ):
                    
                    if re.search( r'^CREATE', line ) is not None:
                        
                        postprocess_indexes.append( f"{line};" )

                        line = re.sub( r'^CREATE.*(INDEX\s+\S+).*$', r'DROP \1', line )

                        preprocess_indexes.append( f"{line};" )

                    elif re.search( r'^ALTER TABLE ONLY.* UNIQUE', line ) is not None:
                        
                        postprocess_unique_constraints.append( f"{line};" )

                        line = re.sub( r'^(ALTER TABLE ONLY.*) ADD CONSTRAINT (\S+) UNIQUE.*$', r'\1 DROP CONSTRAINT \2', line )

                        preprocess_unique_constraints.append( f"{line};" )

                    elif re.search( r'^ALTER TABLE ONLY.* FOREIGN KEY', line ) is not None:
                        
                        postprocess_foreign_keys.append( f"{line};" )

                        line = re.sub( r'^(ALTER TABLE ONLY.*) ADD CONSTRAINT (\S+) FOREIGN KEY.*$', r'\1 DROP CONSTRAINT \2', line )

                        preprocess_foreign_keys.append( f"{line};" )

                    elif re.search( r'^ALTER TABLE ONLY.* PRIMARY KEY', line ) is not None:
                        
                        postprocess_primary_keys.append( f"{line};" )

                        line = re.sub( r'^(ALTER TABLE ONLY.*) ADD CONSTRAINT (\S+) PRIMARY KEY.*$', r'\1 DROP CONSTRAINT \2', line )

                        preprocess_primary_keys.append( f"{line};" )

                    elif re.search( r'^ALTER TABLE', line ) is not None:
                        
                        sys.exit( f"Unexpected ALTER TABLE statement encountered; aborting. Offending statement:\n\n{line}\n\n" )

                    else:
                        
                        sys.exit( f"Unexpected line encountered in SQL index and constraint definitions file; aborting. Offending statement:\n\n{line}\n" )

        table_drop_commands = list()

        print( '   ...transcoding CDA TSVs to SQL command sets...', file=sys.stderr )

        for input_file_basename in sorted( listdir( input_dir ) ):
            
            if re.search( r'\.tsv(\.gz)?$', input_file_basename ) is not None:
                
                input_file = path.join( input_dir, input_file_basename )

                target_table = re.sub( r'\.tsv(\.gz)?$', '', input_file_basename )

                # Clear previous table data via TRUNCATE.

                table_drop_commands.append( f"TRUNCATE {target_table};" )

                # Transcode TSV rows into the body of a prepared SQL COPY statement, to populate the postgres table corresponding to the TSV being scanned.

                output_file_basename = re.sub( r'\.tsv(\.gz)?$', '.sql.gz', input_file_basename )

                output_file = path.join( table_file_dir, output_file_basename )

                # Transcode the TSV data directly into a COPY block.

                print( f"      ...{input_file_basename} -> {output_file_basename}...", file=sys.stderr )

                IN = open( input_file )

                if re.search( r'\.tsv\.gz$', input_file ) is not None:
                    
                    IN.close()

                    IN = gzip.open( input_file, 'rt' )

                with gzip.open( output_file, 'wt' ) as OUT:
                    
                    # COPY diagnosis (id, primary_diagnosis, age_at_diagnosis, morphology, stage, grade, method_of_diagnosis) FROM stdin;

                    print( f"COPY {target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )

                    for next_line in IN:
                        
                        record = dict( zip( colnames, [ value for value in next_line.rstrip( '\n' ).split( '\t' ) ] ) )

                        print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), end='\n', file=OUT )

                    print( r'\.', end='\n\n', file=OUT )

        print( '   ...done transcoding unaliased TSVs to SQL command sets.', file=sys.stderr )

        print( '   ...preparing pre-INSERT directives (index and constraint drops)...', end='', file=sys.stderr )

        # Remove foreign keys first, then (non-PK) uniqueness constraints, then primary keys (which also
        # drops their btree indexes), then the remaining (non-PK) indexes.

        with open( preprocess_command_file, 'w' ) as PRE_CMD:
            
            print( '--', file=PRE_CMD )

            print( '-- drop foreign key constraints:', file=PRE_CMD )

            print( '--', file=PRE_CMD )

            for line in preprocess_foreign_keys:
                
                print( line, file=PRE_CMD )

            print( '--', file=PRE_CMD )

            print( '-- drop (non-PK) uniqueness constraints:', file=PRE_CMD )

            print( '--', file=PRE_CMD )

            for line in preprocess_unique_constraints:
                
                print( line, file=PRE_CMD )

            print( '--', file=PRE_CMD )

            print( '-- drop primary key constraints:', file=PRE_CMD )

            print( '--', file=PRE_CMD )

            for line in preprocess_primary_keys:
                
                print( line, file=PRE_CMD )

            print( '--', file=PRE_CMD )

            print( '-- drop (non-PK) indexes:', file=PRE_CMD )

            print( '--', file=PRE_CMD )

            for line in preprocess_indexes:
                
                print( line, file=PRE_CMD )

            print( '--', file=PRE_CMD )

            print( '-- delete old table rows:', file=PRE_CMD )

            print( '--', file=PRE_CMD )

            for line in table_drop_commands:
                
                print( line, file=PRE_CMD )

            print( end='\n\n', file=PRE_CMD )

        print( 'done.', file=sys.stderr )

        # Then delete all table rows and replace them with the new data
        # (via the .sql files in table_file_dir/ ).

        print( '   ...preparing post-INSERT processing directives (index and constraint replacements)...', end='', file=sys.stderr )

        # Then rebuild indexes and key constraints grouped in the reverse
        # order of that in which they were dropped, i.e. first rebuild indexes,
        # then primary key constraints, then (non-PK) uniqueness constraints, then foreign key constraints.

        with open( postprocess_command_file, 'w' ) as POST_CMD:
            
            print( '--', file=POST_CMD )

            print( '-- rebuild (non-PK) indexes:', file=POST_CMD )

            print( '--', file=POST_CMD )

            for line in postprocess_indexes:
                
                print( line, file=POST_CMD )

            print( '--', file=POST_CMD )

            print( '-- rebuild primary key constraints:', file=POST_CMD )

            print( '--', file=POST_CMD )

            for line in postprocess_primary_keys:
                
                print( line, file=POST_CMD )

            print( '--', file=POST_CMD )

            print( '-- rebuild (non-PK) uniqueness constraints:', file=POST_CMD )

            print( '--', file=POST_CMD )

            for line in postprocess_unique_constraints:
                
                print( line, file=POST_CMD )

            print( '--', file=POST_CMD )

            print( '-- rebuild foreign key constraints:', file=POST_CMD )

            print( '--', file=POST_CMD )

            for line in postprocess_foreign_keys:
                
                print( line, file=POST_CMD )

            print( end='\n\n', file=POST_CMD )

        print( 'done.', file=sys.stderr )

        print( '...done transforming CDA TSVs to SQL.', file=sys.stderr )

    def transform_dir_to_SQL_dump_file( self, input_dir ):
        
        for target_dir in [ self.sql_output_dir ]:
            if not path.isdir( target_dir ):
                makedirs( target_dir )

        output_dump_file = path.join( self.sql_output_dir, 'cda_release.sql.gz' )

        print( f"Transforming CDA TSVs to SQL dump file at {output_dump_file}...", file=sys.stderr )

        # Create tables and add column definitions as comments; then load row data; then
        # build indexes and constraints.

        with gzip.open( output_dump_file, 'wt' ) as OUT, open( self.ddl_schema_file ) as SCHEMA:
            
            # Before constructing constraints and indexes, convert bare strings to
            # tsvector data where needed.
            dump_file_conversion_segment = ''
            # Cache the schema DDL directives starting at the first instance of a
            # primary key assignment, so we can load in table data via COPY before
            # assigning constraints and building indexes and thus avoid update overhead
            # incurred by declaring these structures too soon.
            final_segment_head_reached = False
            dump_file_final_segment = ''
            previous_line = None
            for next_line in SCHEMA:
                if final_segment_head_reached:
                    dump_file_final_segment = dump_file_final_segment + next_line
                elif re.search( r'Type:\s+CONSTRAINT', next_line ) is not None:
                    if previous_line is None:
                        sys.exit( f"FATAL: This condition should never occur; please debug immediately. (previous_line == None)" )
                    else:
                        dump_file_final_segment = previous_line
                        final_segment_head_reached = True
                        dump_file_final_segment = dump_file_final_segment + next_line
                # The psql processing flow doesn't seem to understand the transaction_timeout parameter for SET.
                elif previous_line is not None and re.search( r'SET\s+transaction_timeout', previous_line ) is None:
                    print( previous_line, end='', file=OUT )
                previous_line = next_line

            # Populate all tables using COPY blocks.
            for input_file_basename in sorted( listdir( input_dir ) ):
                if re.search( r'\.tsv(\.gz)?$', input_file_basename ) is not None:
                    input_file = path.join( input_dir, input_file_basename )
                    target_table = re.sub( r'\.tsv(\.gz)?$', '', input_file_basename )
                    # Will we need to translate this data to tsvector after initial load?
                    tsvector_input_data_match = re.search( r'^(\S+)(_text_search)_inputs\.tsv', input_file_basename )
                    if tsvector_input_data_match is not None:
                        target_entity = tsvector_input_data_match.group(1)
                        destination_table = f"{target_entity}{tsvector_input_data_match.group(2)}"
                        dump_file_conversion_segment = dump_file_conversion_segment + f"INSERT INTO public.{destination_table} SELECT {target_entity}_alias, to_tsvector( '{self.default_text_search_config}', search_vector_input ) FROM public.{destination_table}_inputs ;\n\nTRUNCATE public.{destination_table}_inputs ;\n\n"
                    # Transcode TSV rows into the body of a prepared SQL COPY statement,
                    # to populate the postgres table corresponding to the TSV being scanned.
                    print( f"      ...{input_file_basename} -> {target_table}...", file=sys.stderr )
                    IN = open( input_file )
                    if re.search( r'\.tsv\.gz$', input_file ) is not None:
                        IN.close()
                        IN = gzip.open( input_file, 'rt' )
                    colnames = next( IN ).rstrip( '\n' ).split( '\t' )
                    # COPY diagnosis (id, primary_diagnosis, age_at_diagnosis, morphology, stage, grade, method_of_diagnosis) FROM stdin;
                    print( f"COPY public.{target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )
                    for next_line in IN:
                        record = dict( zip( colnames, [ value for value in next_line.rstrip( '\n' ).split( '\t' ) ] ) )
                        print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), end='\n', file=OUT )
                    print( r'\.', end='\n\n', file=OUT )

            # Perform any designated data conversions.
            print( dump_file_conversion_segment, end='', file=OUT )

            # Now paste in the constraint and index construction commands.
            print( dump_file_final_segment, end='', file=OUT )

        print( '...done transforming CDA TSVs to SQL.', file=sys.stderr )



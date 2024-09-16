import gzip
import re
import sys

from os import listdir, makedirs, path

class CDA_loader:
    
    def __init__( self ):
        
        # Enumerated (and ordered: this will determine column order for all X_data_source tables) list of data sources for which we expect entity identifiers to exist. These are lowercase largely because postgresql is a pain about capital letters in column names.

        self.expected_data_sources = [
            
            'gdc',
            'pdc',
            'idc',
            'cds',
            'icdc'
        ]

        # Maximum number of file ID/alias pairs to cache in memory before each pass building the file_subject and file_specimen alias link tables.

        self.merged_tsv_dir = path.join( 'cda_tsvs', 'last_merge' )

        self.sql_output_dir = 'SQL_data'

        # List of indexes and constraints currently applied to the RDBMS. This file
        # will need to be replaced once the cloud environment is understood, as will
        # the out-of-band script that queries the PostgreSQL instance to generate it.

        self.index_and_constraint_def_file = 'indexes_and_constraints.txt'

        for target_dir in [ self.sql_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def transform_dir_to_SQL( self, input_dir ):
        
        print( 'Transforming CDA TSVs to SQL...', file=sys.stderr )

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
                    
                    colnames = next( IN ).rstrip( '\n' ).split( '\t' )

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



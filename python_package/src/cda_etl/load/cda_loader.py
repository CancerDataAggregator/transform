import gzip
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

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

        self.max_alias_cache_size = 25000000

        self.merged_tsv_dir = path.join( 'cda_tsvs', 'merged_icdc_cds_idc_gdc_and_pdc_tables' )

        self.sql_output_dir = 'SQL_data'

        # List of indexes and constraints currently applied to the RDBMS. This file
        # will need to be replaced once the cloud environment is understood, as will
        # the out-of-band script that queries the PostgreSQL instance to generate it.

        self.index_and_constraint_def_file = 'indexes_and_constraints.txt'

        for target_dir in [ self.merged_tsv_dir, self.sql_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def create_integer_alias_TSVs_for_main_entity_tables( self ):
        
        print( 'Creating integer ID alias TSVs for main entity tables...', file=sys.stderr )

        for input_file_basename in [ 'diagnosis.tsv.gz', 'file.tsv.gz', 'researchsubject.tsv.gz', 'specimen.tsv.gz', 'subject.tsv.gz', 'treatment.tsv.gz' ]:
            
            input_file = path.join( self.merged_tsv_dir, input_file_basename )

            output_file = path.join( self.merged_tsv_dir, re.sub( r'^(.*).tsv.gz', r'\1' + '_integer_aliases.tsv.gz', input_file_basename ) )

            print( f"   {output_file} ...", file=sys.stderr )

            with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                
                header = next(IN)

                current_alias = 0

                for next_line in IN:
                    
                    values = next_line.rstrip('\n').split('\t')

                    print( *[ values[0], current_alias ], sep='\t', file=OUT )

                    current_alias = current_alias + 1

        print( '...done.', file=sys.stderr )

    def create_data_source_TSVs_and_SQL_for_main_entity_tables( self ):
        
        print( 'Creating data_source TSVs and SQL for main entity tables...', file=sys.stderr )

        table_file_dir = path.join( self.sql_output_dir, 'new_table_data' )

        for output_dir in [ table_file_dir ]:
            
            if not path.exists( output_dir ):
                
                makedirs( output_dir )

        # Process everything but `file_identifier.tsv.gz` (on which see below): load
        # a list of all distinct data_sources for every record ID of each entity type,
        # and cache results as a separate table (one table per entity type).
        # 
        # In this case, we're physically making two tables, encoding the same information:
        # one TSV destined for the human-readable "CDA TSV" directory, and one SQL command file,
        # for direct DB loading. This process happens (early) in the "load" phase, and not
        # earlier, in the "transform" phase, because all record aggregation and value harmonization
        # (i.e. all transformations) must be complete before this postprocessing run can happen.

        for input_file_basename in [ 'diagnosis_identifier.tsv.gz', 'researchsubject_identifier.tsv.gz', 'specimen_identifier.tsv.gz', 'subject_identifier.tsv.gz', 'treatment_identifier.tsv.gz' ]:
            
            input_file = path.join( self.merged_tsv_dir, input_file_basename )

            entity_type = re.sub( r'^(.*)_identifier.tsv.gz$', r'\1', input_file_basename )

            print( f"   {entity_type}_data_source...", file=sys.stderr )

            # Load alias information for `entity_type`.

            alias_file = path.join( self.merged_tsv_dir, f"{entity_type}_integer_aliases.tsv.gz" )

            entity_alias = dict()

            with gzip.open( alias_file, 'rt' ) as IN:
                
                for next_line in IN:
                    
                    [ record_id, record_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                    entity_alias[record_id] = record_alias

            # Compute names for output columns.

            output_columns = [ f"{entity_type}_alias" ]

            for data_source in self.expected_data_sources:
                
                # Example: `subject_from_GDC`

                output_columns.append( f"{entity_type}_from_{data_source}" )

            # Load data_source associations.

            data_sources = dict()

            with gzip.open( input_file, 'rt' ) as IN:
                
                input_header_fields = next( IN ).rstrip( '\n' ).split( '\t' )

                for next_line in IN:
                    
                    line_values = next_line.rstrip( '\n' ).split( '\t' )

                    line_dict = dict( zip( input_header_fields, line_values ) )

                    record_id = line_dict[f"{entity_type}_id"]

                    if record_id not in entity_alias:
                        
                        sys.exit( f"Loaded record ID '{record_id}' from {input_file_basename}, but could find no corresponding integer alias (alias table size: {len(entity_alias)}). Aborting.\n" )

                    record_alias = entity_alias[record_id]

                    if record_alias not in data_sources:
                        
                        data_sources[record_alias] = set()

                    data_source = line_dict['system'].lower()

                    if data_source not in self.expected_data_sources:
                        
                        sys.exit( f"Unexpected `system` (data source) value encountered in {input_file_basename}: '{data_source}'; cannot continue, aborting.\n" )

                    data_sources[record_alias].add( data_source )

            # Build output files, substituting alias information for IDs as we go.

            output_tsv = path.join( self.merged_tsv_dir, re.sub( r'^(.*)_identifier.tsv.gz$', r'\1' + '_data_source.tsv.gz', input_file_basename ) )

            output_sql = path.join( table_file_dir, re.sub( r'^(.*)_identifier.tsv.gz$', r'\1' + '_data_source.sql.gz', input_file_basename ) )

            with gzip.open( output_tsv, 'wt' ) as TSV, gzip.open( output_sql, 'wt' ) as SQL:
                
                print( *output_columns, sep='\t', file=TSV )

                sql_header = f"COPY {entity_type}_data_source (" + ', '.join( output_columns ) + ') FROM stdin;'

                print( sql_header, end='\n', file=SQL )

                for record_alias_int in sorted( [ int( record_alias ) for record_alias in data_sources ] ):
                    
                    record_alias = str( record_alias_int )

                    output_line = [ record_alias ]

                    for data_source in self.expected_data_sources:
                        
                        if data_source in data_sources[record_alias]:
                            
                            output_line.append( 'true' )

                        else:
                            
                            output_line.append( 'false' )

                    print( *output_line, sep='\t', file=TSV )
                    print( *output_line, sep='\t', file=SQL )

                print( r'\.', end='\n\n', file=SQL )

        # Assumption for `file`: no file will ever be hosted by multiple DCs. If this
        # assumption breaks in the future, it'll break our unique ID assumptions (and possibly
        # some other critical things) -- so assuming, here, that this assumption will
        # remain true isn't adding a whole lot of extra risk, but it will save us a bunch
        # of compute time. Process `file_identifier.tsv` under the assumption that there
        # will be exactly one record for each file: i.e., don't waste time scanning for
        # multiple rows for each file (which, incidentally, would be nonadjacent, since
        # file_identifier records are concatenated in blocks (by DC) -- the entire table is
        # not sorted).
        # 
        # We also know (because we generated the files involved) that file_identifier.tsv
        # lists files in the same order as in file.tsv (and file_integer_id_alias.tsv), so
        # we can just magically know the alias for each file ID without having to look
        # them all up and store them in memory.

        for input_file_basename in [ 'file_identifier.tsv.gz' ]:
            
            input_file = path.join( self.merged_tsv_dir, input_file_basename )

            entity_type = re.sub( r'^(.*)_identifier.tsv.gz$', r'\1', input_file_basename )

            print( f"   {entity_type}_data_source...", file=sys.stderr )

            # Compute names for output columns.

            output_columns = [ f"{entity_type}_alias" ]

            for data_source in self.expected_data_sources:
                
                # Example: `subject_from_GDC`

                output_columns.append( f"{entity_type}_from_{data_source}" )

            # Scan the file_identifiers table and write results to our output files
            # (TSV and SQL, as above) as we go.

            output_tsv = path.join( self.merged_tsv_dir, re.sub( r'^(.*)_identifier.tsv.gz$', r'\1' + '_data_source.tsv.gz', input_file_basename ) )

            output_sql = path.join( table_file_dir, re.sub( r'^(.*)_identifier.tsv.gz$', r'\1' + '_data_source.sql.gz', input_file_basename ) )

            with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_tsv, 'wt' ) as TSV, gzip.open( output_sql, 'wt' ) as SQL:
                
                input_header_fields = next( IN ).rstrip( '\n' ).split( '\t' )

                print( *output_columns, sep='\t', file=TSV )

                sql_header = f"COPY {entity_type}_data_source (" + ', '.join( output_columns ) + ') FROM stdin;'

                print( sql_header, end='\n', file=SQL )

                record_alias = 0

                for next_line in IN:
                    
                    line_values = next_line.rstrip( '\n' ).split( '\t' )

                    line_dict = dict( zip( input_header_fields, line_values ) )

                    output_line = [ str( record_alias ) ]

                    line_data_source = line_dict['system'].lower()

                    if line_data_source not in self.expected_data_sources:
                        
                        sys.exit( f"Unexpected `system` (data source) value encountered in {input_file_basename}: '{data_source}'; cannot continue, aborting.\n" )

                    # Redundant sanity check.

                    found = False

                    for reference_data_source in self.expected_data_sources:
                        
                        if line_data_source == reference_data_source:
                            
                            output_line.append( 'true' )

                            found = True

                        else:
                            
                            output_line.append( 'false' )

                    if not found:
                        
                        record_id = line_dict[f"{entity_type}_id"]

                        sys.exit( f"No `system` recognized for {entity_type}_id '{record_id}' -- this shouldn't happen. Please contact the CDA devs and report this event. Aborting.\n" )

                    print( *output_line, sep='\t', file=TSV )
                    print( *output_line, sep='\t', file=SQL )

                    record_alias = record_alias + 1

                print( r'\.', end='\n\n', file=SQL )

        print( '...done.', file=sys.stderr )

    def transform_main_entity_tables_to_SQL( self ):
        
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
                
                # Ignore indexes built automatically as side effects of primary key constraint definitions.

                if ( re.search( r'CREATE UNIQUE INDEX', line ) is None or re.search( r'_pkey ', line ) is None ):
                    
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

        # Data in most of these tables needs to be refactored to use integer aliases (TSV versions
        # in self.merged_tsv_dir typically contain full ID strings and not aliases) before pushing
        # to postgres. SQL command sets for all of these tables are built in other subroutines (see
        # create_entity_data_source_tables(), create_integer_aliases_for_file_association_tables(),
        # and create_integer_aliases_for_other_association_tables(), as well as the related subroutines
        # in the mutation_transformer code).

        tables_with_aliases = {
            
            'diagnosis_data_source',
            'diagnosis_identifier',
            'diagnosis_treatment',
            'file_associated_project',
            'file_data_source',
            'file_identifier',
            'file_specimen',
            'file_subject',
            'mutation',
            'researchsubject_data_source',
            'researchsubject_diagnosis',
            'researchsubject_identifier',
            'researchsubject_specimen',
            'researchsubject_treatment',
            'specimen_data_source',
            'specimen_identifier',
            'subject_associated_project',
            'subject_data_source',
            'subject_identifier',
            'subject_mutation',
            'subject_researchsubject',
            'treatment_data_source',
            'treatment_identifier'
        }

        print( '   ...transcoding unaliased TSVs to SQL command sets...', file=sys.stderr )

        for input_file_basename in sorted( listdir( self.merged_tsv_dir ) ):
            
            if re.search( r'_integer_aliases\.tsv\.gz$', input_file_basename ) is None and re.search( r'\.tsv\.gz$', input_file_basename ) is not None:
                
                input_file = path.join( self.merged_tsv_dir, input_file_basename )

                target_table = re.sub( r'\.tsv\.gz$', '', input_file_basename )

                # Clear previous table data via TRUNCATE.

                table_drop_commands.append( f"TRUNCATE {target_table};" )

                # Transcode TSV rows into the body of a prepared SQL COPY statement, to populate the postgres table corresponding to the TSV being scanned.

                if target_table not in tables_with_aliases:
                    
                    output_file_basename = re.sub( r'\.tsv\.gz$', '.sql.gz', input_file_basename )

                    output_file = path.join( table_file_dir, output_file_basename )

                    integer_alias_file = path.join( self.merged_tsv_dir, f"{target_table}_integer_aliases.tsv.gz" )

                    if not path.exists( integer_alias_file ):
                        
                        # No integer aliases have been created for this table. This table is either an unaliased main-entity table (like the "treatment"
                        # table, at the time this comment was written*) or an association table that doesn't use integer aliases (like "diagnosis_identifier"
                        # at time of writing). Either way, transcode the TSV data directly into a COPY block.
                        # 
                        # *Update 2024-06-22: all main-entity tables now get integer aliases.

                        print( f"      ...{input_file_basename} -> {output_file_basename} (no integer aliases loaded)...", file=sys.stderr )

                        with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                            
                            colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                            # COPY diagnosis (id, primary_diagnosis, age_at_diagnosis, morphology, stage, grade, method_of_diagnosis) FROM stdin;

                            print( f"COPY {target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )

                            for next_line in IN:
                                
                                line = next_line.rstrip( '\n' )
    
                                record = dict( zip( colnames, [ value for value in line.split( '\t' ) ] ) )
    
                                print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), end='\n', file=OUT )
    
                            print( r'\.', end='\n\n', file=OUT )
    
                    else:
                        
                        # Integer aliases have been created for this table: note that this will be a main entity table (like file or specimen) and not
                        # an association table (like diagnosis_identifier): we take care of the latter in the section just above, and also below in
                        # the create_integer_aliases_for_file_association_tables() and create_integer_aliases_for_other_association_tables() functions.
                        # 
                        # Transcode the TSV data into a COPY block, patching in the integer ID alias assigned to each entity record (adding
                        # a new "integer_id_alias" column for that purpose).

                        print( f"      ...{input_file_basename} -> {output_file_basename} (co-loading integer aliases)...", file=sys.stderr )

                        with gzip.open( input_file, 'rt' ) as IN, gzip.open( integer_alias_file, 'rt' ) as ALIAS, gzip.open( output_file, 'wt' ) as OUT:
                            
                            colnames = next( IN ).rstrip( '\n' ).split( '\t' )
    
                            colnames.append( 'integer_id_alias' )
    
                            print( f"COPY {target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )
    
                            for next_line in IN:
                                
                                line = next_line.rstrip( '\n' )
    
                                record = dict( zip( colnames[:-1], [ value for value in line.split( '\t' ) ] ) )
    
                                id_alias = next( ALIAS ).rstrip( '\n' ).split( '\t' )[1]
    
                                record['integer_id_alias'] = id_alias
    
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

        print( 'done.', file=sys.stderr )

        print( '...done transforming CDA TSVs to SQL.', file=sys.stderr )

    def create_integer_aliases_and_SQL_for_file_association_tables( self ):
        
        print( 'Creating integer aliases for association tables linked to the file table...', file=sys.stderr )

        table_file_dir = path.join( self.sql_output_dir, 'new_table_data' )

        full_link_table = {
            
            'associated_project': path.join( self.merged_tsv_dir, 'file_associated_project.tsv.gz' ),
            'identifier': path.join( self.merged_tsv_dir, 'file_identifier.tsv.gz' ),
            'specimen': path.join( self.merged_tsv_dir, 'file_specimen.tsv.gz' ),
            'subject': path.join( self.merged_tsv_dir, 'file_subject.tsv.gz' )
        }

        output_link_table = {
            
            'associated_project': path.join( table_file_dir, 'file_associated_project.sql.gz' ),
            'identifier': path.join( table_file_dir, 'file_identifier.sql.gz' ),
            'specimen': path.join( table_file_dir, 'file_specimen.sql.gz' ),
            'subject': path.join( table_file_dir, 'file_subject.sql.gz' )
        }

        file_id_alias_file = path.join( self.merged_tsv_dir, 'file_integer_aliases.tsv.gz' )

        for type_to_link in [ 'associated_project', 'identifier' ]:
            
            scan_complete = False

            first_pass = True

            lines_to_skip = 0

            input_file = full_link_table[type_to_link]

            output_file = output_link_table[type_to_link]

            print( f"   ...creating {output_file}...", file=sys.stderr )

            while not scan_complete:
                
                print( f"      ...skipping {lines_to_skip} lines; loading next <= {self.max_alias_cache_size}...", file=sys.stderr )

                # Load the next block from `file_id_alias_file`.

                file_aliases = dict()

                lines_read = 0

                current_cache_size = 0

                with gzip.open( file_id_alias_file, 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        if lines_read >= lines_to_skip:
                            
                            if current_cache_size < self.max_alias_cache_size:
                                
                                [ file_id, file_alias ] = next_line.rstrip('\n').split('\t')

                                file_aliases[file_id] = file_alias

                                current_cache_size = current_cache_size + 1
                            
                        lines_read = lines_read + 1

                # Translate all matching records to aliased versions.

                if first_pass:
                    
                    with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                        
                        if type_to_link == 'identifier':
                            
                            print( f"COPY file_{type_to_link} ( file_alias, system, field_name, value ) FROM stdin;", end='\n', file=OUT )

                            for next_line in IN:
                                
                                [ file_id, system, field_name, value ] = next_line.rstrip('\n').split('\t')

                                # Note we don't have to handle header lines separately: they'll always fail the following test.

                                if file_id in file_aliases:
                                    
                                    print( *[ file_aliases[file_id], system, field_name, value ], sep='\t', file=OUT )

                        elif type_to_link == 'associated_project':
                            
                            print( f"COPY file_{type_to_link} ( file_alias, associated_project ) FROM stdin;", end='\n', file=OUT )

                            for next_line in IN:
                                
                                [ file_id, associated_project ] = next_line.rstrip('\n').split('\t')

                                # Note we don't have to handle header lines separately: they'll always fail the following test.

                                if file_id in file_aliases:
                                    
                                    print( *[ file_aliases[file_id], associated_project ], sep='\t', file=OUT )

                else:
                    
                    with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'at' ) as OUT:
                        
                        if type_to_link == 'identifier':
                            
                            for next_line in IN:
                                
                                [ file_id, system, field_name, value ] = next_line.rstrip('\n').split('\t')

                                if file_id in file_aliases:
                                    
                                    print( *[ file_aliases[file_id], system, field_name, value ], sep='\t', file=OUT )

                        elif type_to_link == 'associated_project':
                            
                            for next_line in IN:
                                
                                [ file_id, associated_project ] = next_line.rstrip('\n').split('\t')

                                if file_id in file_aliases:
                                    
                                    print( *[ file_aliases[file_id], associated_project ], sep='\t', file=OUT )

                if current_cache_size < self.max_alias_cache_size:
                    
                    scan_complete = True

                lines_to_skip = lines_to_skip + current_cache_size

                first_pass = False

            with gzip.open( output_file, 'at' ) as OUT:
                
                print( r'\.', end='\n\n', file=OUT )

            print( f"   ...done writing {output_file}.", file=sys.stderr )

        for type_to_link in [ 'specimen', 'subject' ]:
            
            link_id_alias_file = path.join( self.merged_tsv_dir, f"{type_to_link}_integer_aliases.tsv.gz" )

            aliases_to_link = dict()

            with gzip.open( link_id_alias_file, 'rt' ) as IN:
                
                for next_line in IN:
                    
                    [ original_id, integer_alias ] = next_line.rstrip('\n').split('\t')

                    aliases_to_link[original_id] = integer_alias

            scan_complete = False

            first_pass = True

            lines_to_skip = 0

            input_file = full_link_table[type_to_link]

            output_file = output_link_table[type_to_link]

            print( f"   ...creating {output_file}...", file=sys.stderr )

            while not scan_complete:
                
                print( f"      ...skipping {lines_to_skip} lines; loading next <= {self.max_alias_cache_size}...", file=sys.stderr )

                # Load the next block from `file_id_alias_file`.

                file_aliases = dict()

                lines_read = 0

                current_cache_size = 0

                with gzip.open( file_id_alias_file, 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        if lines_read >= lines_to_skip:
                            
                            if current_cache_size < self.max_alias_cache_size:
                                
                                [ file_id, file_alias ] = next_line.rstrip('\n').split('\t')

                                file_aliases[file_id] = file_alias

                                current_cache_size = current_cache_size + 1
                            
                        lines_read = lines_read + 1

                # Translate all matching records to aliased versions.

                if first_pass:
                    
                    with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                        
                        print( f"COPY file_{type_to_link} ( file_alias, {type_to_link}_alias ) FROM stdin;", end='\n', file=OUT )

                        for next_line in IN:
                            
                            [ file_id, id_to_link ] = next_line.rstrip('\n').split('\t')

                            if file_id in file_aliases:
                                
                                print( *[ file_aliases[file_id], aliases_to_link[id_to_link] ], sep='\t', file=OUT )

                else:
                    
                    with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'at' ) as OUT:
                        
                        for next_line in IN:
                            
                            [ file_id, id_to_link ] = next_line.rstrip('\n').split('\t')

                            if file_id in file_aliases:
                                
                                print( *[ file_aliases[file_id], aliases_to_link[id_to_link] ], sep='\t', file=OUT )

                if current_cache_size < self.max_alias_cache_size:
                    
                    scan_complete = True

                lines_to_skip = lines_to_skip + current_cache_size

                first_pass = False

            with gzip.open( output_file, 'at' ) as OUT:
                
                print( r'\.', end='\n\n', file=OUT )

            print( f"   ...done writing {output_file}.", file=sys.stderr )

        print( '...(aliased) file association table construction complete.', file=sys.stderr )

    def create_integer_aliases_and_SQL_for_other_association_tables( self ):
        
        print( 'Creating integer aliases for association tables not involving the file table...', file=sys.stderr )

        table_file_dir = path.join( self.sql_output_dir, 'new_table_data' )

        full_link_table = {
            
            'diagnosis_identifier': path.join( self.merged_tsv_dir, 'diagnosis_identifier.tsv.gz' ),
            'diagnosis_treatment': path.join( self.merged_tsv_dir, 'diagnosis_treatment.tsv.gz' ),
            'rs_diagnosis': path.join( self.merged_tsv_dir, 'researchsubject_diagnosis.tsv.gz' ),
            'rs_identifier': path.join( self.merged_tsv_dir, 'researchsubject_identifier.tsv.gz' ),
            'rs_specimen': path.join( self.merged_tsv_dir, 'researchsubject_specimen.tsv.gz' ),
            'rs_treatment': path.join( self.merged_tsv_dir, 'researchsubject_treatment.tsv.gz' ),
            'specimen_identifier': path.join( self.merged_tsv_dir, 'specimen_identifier.tsv.gz' ),
            'subject_rs': path.join( self.merged_tsv_dir, 'subject_researchsubject.tsv.gz' ),
            'subject_associated_project': path.join( self.merged_tsv_dir, 'subject_associated_project.tsv.gz' ),
            'subject_identifier': path.join( self.merged_tsv_dir, 'subject_identifier.tsv.gz' ),
            'treatment_identifier': path.join( self.merged_tsv_dir, 'treatment_identifier.tsv.gz' )
        }

        output_link_table = {
            
            'diagnosis_identifier': path.join( table_file_dir, 'diagnosis_identifier.sql.gz' ),
            'diagnosis_treatment': path.join( table_file_dir, 'diagnosis_treatment.sql.gz' ),
            'rs_diagnosis': path.join( table_file_dir, 'researchsubject_diagnosis.sql.gz' ),
            'rs_identifier': path.join( table_file_dir, 'researchsubject_identifier.sql.gz' ),
            'rs_specimen': path.join( table_file_dir, 'researchsubject_specimen.sql.gz' ),
            'rs_treatment': path.join( table_file_dir, 'researchsubject_treatment.sql.gz' ),
            'specimen_identifier': path.join( table_file_dir, 'specimen_identifier.sql.gz' ),
            'subject_rs': path.join( table_file_dir, 'subject_researchsubject.sql.gz' ),
            'subject_associated_project': path.join( table_file_dir, 'subject_associated_project.sql.gz' ),
            'subject_identifier': path.join( table_file_dir, 'subject_identifier.sql.gz' ),
            'treatment_identifier': path.join( table_file_dir, 'treatment_identifier.sql.gz' )
        }

        id_alias_file = {
            
            'diagnosis': path.join( self.merged_tsv_dir, 'diagnosis_integer_aliases.tsv.gz' ),
            'researchsubject': path.join( self.merged_tsv_dir, 'researchsubject_integer_aliases.tsv.gz' ),
            'specimen': path.join( self.merged_tsv_dir, 'specimen_integer_aliases.tsv.gz' ),
            'subject': path.join( self.merged_tsv_dir, 'subject_integer_aliases.tsv.gz' ),
            'treatment': path.join( self.merged_tsv_dir, 'treatment_integer_aliases.tsv.gz' )
        }

        for type_to_link in sorted( full_link_table ):
            
            input_file = full_link_table[type_to_link]

            output_file = output_link_table[type_to_link]

            print( f"   ...creating {output_file}...", file=sys.stderr )

            aliases = {
                'diagnosis': dict(),
                'rs': dict(),
                'specimen': dict(),
                'subject': dict(),
                'treatment': dict()
            }

            if type_to_link in [ 'diagnosis_identifier', 'diagnosis_treatment', 'rs_diagnosis' ]:
                
                print( f"      ...pre-loading aliases from {id_alias_file['diagnosis']}...", end='', file=sys.stderr )

                with gzip.open( id_alias_file['diagnosis'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ diagnosis_id, diagnosis_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['diagnosis'][diagnosis_id] = diagnosis_alias

                print( 'done.', file=sys.stderr )

            if type_to_link in [ 'rs_diagnosis', 'rs_identifier', 'rs_specimen', 'rs_treatment', 'subject_rs' ]:
                
                print( f"      ...pre-loading aliases from {id_alias_file['researchsubject']}...", end='', file=sys.stderr )

                with gzip.open( id_alias_file['researchsubject'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ rs_id, rs_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['rs'][rs_id] = rs_alias

                print( 'done.', file=sys.stderr )

            if type_to_link in [ 'rs_specimen', 'specimen_identifier' ]:
                
                print( f"      ...pre-loading aliases from {id_alias_file['specimen']}...", end='', file=sys.stderr )

                with gzip.open( id_alias_file['specimen'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ specimen_id, specimen_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['specimen'][specimen_id] = specimen_alias

                print( 'done.', file=sys.stderr )

            if type_to_link in [ 'subject_rs', 'subject_associated_project', 'subject_identifier' ]:
                        
                print( f"      ...pre-loading aliases from {id_alias_file['subject']}...", end='', file=sys.stderr )

                with gzip.open( id_alias_file['subject'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ subject_id, subject_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['subject'][subject_id] = subject_alias

                print( 'done.', file=sys.stderr )

            if type_to_link in [ 'diagnosis_treatment', 'rs_treatment', 'treatment_identifier' ]:
                        
                print( f"      ...pre-loading aliases from {id_alias_file['treatment']}...", end='', file=sys.stderr )

                with gzip.open( id_alias_file['treatment'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ treatment_id, treatment_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['treatment'][treatment_id] = treatment_alias

                print( 'done.', file=sys.stderr )

            # Translate all matching records to aliased versions.

            with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                
                print( f"      ...transcoding {input_file} to {output_file}...", end='', file=sys.stderr )

                if re.search( r'_identifier$', type_to_link ) is not None:
                    
                    entity_name = re.sub( r'_identifier$', r'', type_to_link )

                    full_entity_name = entity_name

                    if entity_name == 'rs':
                        
                        full_entity_name = 'researchsubject'

                    table_name = type_to_link

                    if type_to_link == 'rs_identifier':
                        
                        table_name = 'researchsubject_identifier'

                    print( f"COPY {table_name} ( {full_entity_name}_alias, system, field_name, value ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ current_id, system, field_name, value ] = next_line.rstrip( '\n' ).split( '\t' )

                            if current_id in aliases[entity_name]:
                                
                                print( *[ aliases[entity_name][current_id], system, field_name, value ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: {entity_name}_id '{current_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'diagnosis_treatment':
                    
                    print( f"COPY diagnosis_treatment ( diagnosis_alias, treatment_alias ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ diagnosis_id, treatment_id ] = next_line.rstrip( '\n' ).split( '\t' )

                            if diagnosis_id in aliases['diagnosis'] and treatment_id in aliases['treatment']:
                                
                                print( *[ aliases['diagnosis'][diagnosis_id], aliases['treatment'][treatment_id] ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: one or both of diagnosis_id '{diagnosis_id}' and treatment_id '{treatment_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'rs_diagnosis':
                    
                    print( f"COPY researchsubject_diagnosis ( researchsubject_alias, diagnosis_alias ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ rs_id, diagnosis_id ] = next_line.rstrip( '\n' ).split( '\t' )

                            if rs_id in aliases['rs'] and diagnosis_id in aliases['diagnosis']:
                                
                                print( *[ aliases['rs'][rs_id], aliases['diagnosis'][diagnosis_id] ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: one or both of rs_id '{rs_id}' and diagnosis_id '{diagnosis_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'rs_specimen':
                    
                    print( f"COPY researchsubject_specimen ( researchsubject_alias, specimen_alias ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ rs_id, specimen_id ] = next_line.rstrip( '\n' ).split( '\t' )

                            if rs_id in aliases['rs'] and specimen_id in aliases['specimen']:
                                
                                print( *[ aliases['rs'][rs_id], aliases['specimen'][specimen_id] ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: one or both of researchsubject_id '{rs_id}' and specimen_id '{specimen_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'rs_treatment':
                    
                    print( f"COPY researchsubject_treatment ( researchsubject_alias, treatment_alias ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ rs_id, treatment_id ] = next_line.rstrip( '\n' ).split( '\t' )

                            if rs_id in aliases['rs'] and treatment_id in aliases['treatment']:
                                
                                print( *[ aliases['rs'][rs_id], aliases['treatment'][treatment_id] ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: one or both of rs_id '{rs_id}' and treatment_id '{treatment_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'subject_associated_project':
                    
                    print( f"COPY subject_associated_project ( subject_alias, associated_project ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ subject_id, associated_project ] = next_line.rstrip( '\n' ).split( '\t' )

                            if subject_id in aliases['subject']:
                                
                                print( *[ aliases['subject'][subject_id], associated_project ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: subject_id '{subject_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

                elif type_to_link == 'subject_rs':
                    
                    print( f"COPY subject_researchsubject ( subject_alias, researchsubject_alias ) FROM stdin;", end='\n', file=OUT )

                    header = True

                    for next_line in IN:
                        
                        if not header:
                            
                            [ subject_id, rs_id ] = next_line.rstrip( '\n' ).split( '\t' )

                            if subject_id in aliases['subject'] and rs_id in aliases['rs']:
                                
                                print( *[ aliases['subject'][subject_id], aliases['rs'][rs_id] ], sep='\t', file=OUT )

                            else:
                                
                                print( f"CRITICAL WARNING: one or both of subject_id '{subject_id}' and researchsubject_id '{rs_id}' not aliased: please investigate and fix!", file=sys.stderr )

                        header = False

            with gzip.open( output_file, 'at' ) as OUT:
                
                print( r'\.', end='\n\n', file=OUT )

                print( 'done.', file=sys.stderr )

            print( f"   ...done building {output_file}.", file=sys.stderr )

        print( '...finished creating integer aliases for association tables not involving the file table.', file=sys.stderr )



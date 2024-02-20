import gzip
import jsonlines
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

class CDA_loader:
    
    def __init__( self ):
        
        # Maximum number of file records to assemble in memory before dumping to JSONL.

        self.max_record_cache_size = 2000000

        # Maximum number of file ID/alias pairs to cache in memory before each pass building the file_subject and file_specimen alias link tables.

        self.max_alias_cache_size = 25000000

        # Maximum number of file records per JSONL output file.

        self.max_records_per_file = 20000000

        self.merged_tsv_dir = path.join( 'cda_tsvs', 'merged_cds_idc_gdc_and_pdc_tables' )

        self.jsonl_output_dir = 'BigQuery_JSONL'

        self.jsonl_log_dir = path.join( self.jsonl_output_dir, '__processing_logs' )

        self.sql_output_dir = 'SQL_data'

        # List of indexes and constraints currently applied to the RDBMS. This file
        # will need to be replaced once the cloud environment is understood, as will
        # the out-of-band script that queries the PostgreSQL instance to generate it.

        self.index_and_constraint_def_file = 'indexes_and_constraints.txt'

        for target_dir in [ self.merged_tsv_dir, self.jsonl_output_dir, self.jsonl_log_dir, self.sql_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def transform_merged_files_to_jsonl( self ):
        
        file_input_tsv = path.join( self.merged_tsv_dir, 'file.tsv.gz' )

        file_identifier_input_tsv = path.join( self.merged_tsv_dir, 'file_identifier.tsv.gz' )
        file_associated_project_input_tsv = path.join( self.merged_tsv_dir, 'file_associated_project.tsv.gz' )
        file_specimen_input_tsv = path.join( self.merged_tsv_dir, 'file_specimen.tsv.gz' )
        file_subject_input_tsv = path.join( self.merged_tsv_dir, 'file_subject.tsv.gz' )

        subject_researchsubject_input_tsv = path.join( self.merged_tsv_dir, 'subject_researchsubject.tsv.gz' )

        output_file_index = 1

        output_file = f"{self.jsonl_output_dir}/files.{output_file_index}.json.gz"

        file_id_sanity_check_list = f"{self.jsonl_log_dir}/seen_file_ids.txt.gz"

        # Load mappable metadata from the file table and its association tables, writing transformed JSON as we go for direct loading into BigQuery.

        seen_file_ids = set()

        total_file_record_count = 0

        OUT = gzip.open( output_file, 'w' )
            
        writer = jsonlines.Writer( OUT )

        first_pass = True

        while total_file_record_count == 0 or len( seen_file_ids ) < total_file_record_count:
            
            output_records = dict()

            print( 'Loading next chunk of file records for processing...', sep='', end='\n\n', file=sys.stderr )

            print( '   ...loading from file.tsv...', file=sys.stderr )

            loaded_records = 0

            display_increment = 500000

            with gzip.open( file_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for next_line in IN:
                    
                    line = next_line.rstrip('\n')

                    record = dict( zip( colnames, line.split('\t') ) )

                    file_id = record['id']

                    if first_pass:
                        
                        total_file_record_count = total_file_record_count + 1

                    elif len( output_records ) >= self.max_record_cache_size:
                        
                        break

                    if len( output_records ) < self.max_record_cache_size and file_id not in seen_file_ids:
                        
                        seen_file_ids.add( file_id )

                        # Change output files every once in a while.

                        if ( len( seen_file_ids ) > 1 ) and ( ( len( seen_file_ids ) - 1 ) % self.max_records_per_file == 0 ):
                            
                            OUT.close()

                            output_file_index = output_file_index + 1

                            output_file = f"{self.jsonl_output_dir}/files.{output_file_index}.json.gz"

                            OUT = gzip.open( output_file, 'w' )

                            writer = jsonlines.Writer( OUT )

                        if file_id in output_records:
                            
                            sys.exit(f"Unexpected failure? Duplicate file record {file_id}; aborting.")

                        else:
                            
                            output_records[file_id] = dict()

                            for colname in colnames:
                                
                                output_records[file_id][colname] = record[colname] if record[colname] != '' else None

                            # Fix the data type for this one.

                            output_records[file_id]['byte_size'] = int(record['byte_size']) if record['byte_size'] != '' else None

                            # Seed empty data structures for list elements.

                            output_records[file_id]['identifier'] = list()
                            output_records[file_id]['associated_project'] = set()
                            output_records[file_id]['Subjects'] = set()
                            output_records[file_id]['ResearchSubjects'] = set()
                            output_records[file_id]['Specimens'] = set()

                            loaded_records = loaded_records + 1

                            if loaded_records % display_increment == 0:
                                
                                print( f"      ...loaded {loaded_records} records...", file=sys.stderr )

            print( f"   ...done. Loaded {loaded_records} total for this pass.", file=sys.stderr )

            print( '   ...scanning file_identifier.tsv...', file=sys.stderr )

            with gzip.open( file_identifier_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for next_line in IN:
                    
                    line = next_line.rstrip('\n')

                    record = dict( zip( colnames, line.split('\t') ) )

                    file_id = record['file_id']

                    if file_id in output_records:
                        
                        system = record['system']

                        field_name = record['field_name']

                        value = record['value']

                        output_records[file_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

            print( '   ...scanning file_associated_project.tsv...', file=sys.stderr )

            with gzip.open( file_associated_project_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for next_line in IN:
                    
                    line = next_line.rstrip('\n')

                    record = dict( zip( colnames, line.split('\t') ) )

                    file_id = record['file_id']

                    if file_id in output_records:
                        
                        associated_project = record['associated_project']

                        output_records[file_id]['associated_project'].add( associated_project )

            print( '   ...scanning file_specimen.tsv...', file=sys.stderr )

            with gzip.open( file_specimen_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for next_line in IN:
                    
                    line = next_line.rstrip('\n')

                    record = dict( zip( colnames, line.split('\t') ) )

                    file_id = record['file_id']

                    if file_id in output_records:
                        
                        specimen_id = record['specimen_id']

                        output_records[file_id]['Specimens'].add( specimen_id )

            print( '   ...scanning subject_researchsubject.tsv...', file=sys.stderr )

            subject_researchsubject = dict()

            with gzip.open( subject_researchsubject_input_tsv, 'rt' ) as IN:
                
                header = next(IN)

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    [ subject_id, rs_id ] = line.split('\t')

                    if subject_id not in subject_researchsubject:
                        
                        subject_researchsubject[subject_id] = set()

                    subject_researchsubject[subject_id].add( rs_id )

            print( '   ...scanning file_subject.tsv...', file=sys.stderr )

            with gzip.open( file_subject_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for next_line in IN:
                    
                    line = next_line.rstrip('\n')

                    record = dict( zip( colnames, line.split('\t') ) )

                    file_id = record['file_id']

                    if file_id in output_records:
                        
                        subject_id = record['subject_id']

                        output_records[file_id]['Subjects'].add( subject_id )

                        if subject_id in subject_researchsubject:
                            
                            for rs_id in sorted( subject_researchsubject[subject_id] ):
                                
                                output_records[file_id]['ResearchSubjects'].add( rs_id )

            # Sort 'identifier' lists of dicts.

            print( '   ...sorting identifiers...', file=sys.stderr )

            for file_id in output_records:
                
                output_records[file_id]['identifier'] = sorted( output_records[file_id]['identifier'], key=lambda x: x['system'] )

            # Convert 'associated_project' sets to lists.

            print( '   ...sorting associated_projects...', file=sys.stderr )

            for file_id in output_records:
                
                output_records[file_id]['associated_project'] = sorted( output_records[file_id]['associated_project'] )

            # Convert 'Specimens' sets to lists.

            print( '   ...sorting specimens...', file=sys.stderr )

            for id in output_records:
                
                output_records[id]['Specimens'] = sorted(output_records[id]['Specimens'])

            # Convert 'Subjects' sets to lists.

            print( '   ...sorting subjects...', file=sys.stderr )

            for file_id in output_records:
                
                output_records[file_id]['Subjects'] = sorted( output_records[file_id]['Subjects'] )

            # Convert 'ResearchSubjects' sets to lists.

            print( '   ...sorting researchsubjects...', file=sys.stderr )

            for file_id in output_records:
                
                output_records[file_id]['ResearchSubjects'] = sorted( output_records[file_id]['ResearchSubjects'] )

            print( f"\n...done with current pass. Writing {len( output_records )} records to JSONL ({len( seen_file_ids )} written so far of {total_file_record_count} total)...", sep='', end='', file=sys.stderr )

            { writer.write( output_records[file_id] ) for file_id in sorted( output_records ) }

            print( 'done.', file=sys.stderr )

            first_pass = False

        OUT.close()

        with gzip.open( file_id_sanity_check_list, 'wt' ) as OUT:
            
            for file_id in seen_file_ids:
                
                print( file_id, file=OUT )

    def transform_merged_subjects_and_sub_objects_to_jsonl( self ):
        
        diagnosis_input_tsv = path.join( self.merged_tsv_dir, 'diagnosis.tsv.gz' )
        diagnosis_identifier_input_tsv = path.join( self.merged_tsv_dir, 'diagnosis_identifier.tsv.gz' )
        diagnosis_treatment_input_tsv = path.join( self.merged_tsv_dir, 'diagnosis_treatment.tsv.gz' )

        researchsubject_input_tsv = path.join( self.merged_tsv_dir, 'researchsubject.tsv.gz' )
        researchsubject_identifier_input_tsv = path.join( self.merged_tsv_dir, 'researchsubject_identifier.tsv.gz' )
        researchsubject_specimen_input_tsv = path.join( self.merged_tsv_dir, 'researchsubject_specimen.tsv.gz' )
        researchsubject_diagnosis_input_tsv = path.join( self.merged_tsv_dir, 'researchsubject_diagnosis.tsv.gz' )
        researchsubject_treatment_input_tsv = path.join( self.merged_tsv_dir, 'researchsubject_treatment.tsv.gz' )

        specimen_input_tsv = path.join( self.merged_tsv_dir, 'specimen.tsv.gz' )
        specimen_identifier_input_tsv = path.join( self.merged_tsv_dir, 'specimen_identifier.tsv.gz' )

        subject_input_tsv = path.join( self.merged_tsv_dir, 'subject.tsv.gz' )
        subject_identifier_input_tsv = path.join( self.merged_tsv_dir, 'subject_identifier.tsv.gz' )
        subject_associated_project_input_tsv = path.join( self.merged_tsv_dir, 'subject_associated_project.tsv.gz' )
        subject_researchsubject_input_tsv = path.join( self.merged_tsv_dir, 'subject_researchsubject.tsv.gz' )

        treatment_input_tsv = path.join( self.merged_tsv_dir, 'treatment.tsv.gz' )
        treatment_identifier_input_tsv = path.join( self.merged_tsv_dir, 'treatment_identifier.tsv.gz' )

        output_file = f"{self.jsonl_output_dir}/subjects.json.gz"

        subject = dict()
        researchsubject = dict()
        specimen = dict()
        diagnosis = dict()
        treatment = dict()

        subject_has_researchsubject = map_columns_one_to_many( subject_researchsubject_input_tsv, 'subject_id', 'researchsubject_id', gzipped=True )

        researchsubject_has_specimen = map_columns_one_to_many( researchsubject_specimen_input_tsv, 'researchsubject_id', 'specimen_id', gzipped=True )

        researchsubject_has_diagnosis = map_columns_one_to_many( researchsubject_diagnosis_input_tsv, 'researchsubject_id', 'diagnosis_id', gzipped=True )

        diagnosis_has_treatment = map_columns_one_to_many( diagnosis_treatment_input_tsv, 'diagnosis_id', 'treatment_id', gzipped=True )

        # Load mappable metadata from the subject table and its association tables.

        with gzip.open( subject_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                subject_id = record['id']

                if subject_id in subject:
                    
                    sys.exit(f"Unexpected failure? Duplicate subject record {subject_id}; aborting.")

                else:
                    
                    subject[subject_id] = dict()

                    for colname in colnames:
                        
                        subject[subject_id][colname] = record[colname] if record[colname] != '' else None

                    # Fix the data type for these.

                    subject[subject_id]['days_to_birth'] = int(record['days_to_birth']) if record['days_to_birth'] != '' else None
                    subject[subject_id]['days_to_death'] = int(record['days_to_death']) if record['days_to_death'] != '' else None

                    # Seed empty data structures for list elements.

                    subject[subject_id]['identifier'] = list()
                    subject[subject_id]['subject_associated_project'] = set()
                    subject[subject_id]['ResearchSubject'] = list()

        with gzip.open( subject_identifier_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                subject_id = record['subject_id']

                system = record['system']

                field_name = record['field_name']

                value = record['value']

                if subject_id not in subject:
                    
                    sys.exit(f"Unexpected failure? Subject with subject_id {subject_id} found in subject_identifier.tsv but not in subject.tsv; aborting.\n")

                subject[subject_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        with gzip.open( subject_associated_project_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                subject_id = record['subject_id']

                associated_project = record['associated_project']

                if subject_id not in subject:
                    
                    sys.exit(f"Unexpected failure? Subject with subject_id {subject_id} found in subject_associated_project.tsv but not in subject.tsv; aborting.\n")

                subject[subject_id]['subject_associated_project'].add( associated_project )

        # Sort subject.subject_associated_project data.

        for subject_id in subject:
            
            subject[subject_id]['subject_associated_project'] = sorted( subject[subject_id]['subject_associated_project'] )

        # Load mappable metadata from the researchsubject table and its association tables.

        with gzip.open( researchsubject_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                rs_id = record['id']

                if rs_id in researchsubject:
                    
                    sys.exit(f"Unexpected failure? Duplicate researchsubject record {rs_id}; aborting.")

                else:
                    
                    researchsubject[rs_id] = dict()

                    for colname in colnames:
                        
                        researchsubject[rs_id][colname] = record[colname] if record[colname] != '' else None

                    # Seed empty data structures for list elements.

                    researchsubject[rs_id]['identifier'] = list()
                    researchsubject[rs_id]['Diagnosis'] = list()
                    researchsubject[rs_id]['Specimen'] = list()

        with gzip.open( researchsubject_identifier_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                rs_id = record['researchsubject_id']

                system = record['system']

                field_name = record['field_name']

                value = record['value']

                if rs_id not in researchsubject:
                    
                    sys.exit(f"Unexpected failure? ResearchSubject with rs_id {rs_id} found in rs_identifier.tsv but not in researchsubject.tsv; aborting.\n")

                researchsubject[rs_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the diagnosis table and its association tables.

        with gzip.open( diagnosis_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                diagnosis_id = record['id']

                if diagnosis_id in diagnosis:
                    
                    sys.exit(f"Unexpected failure? Duplicate diagnosis record {diagnosis_id}; aborting.")

                else:
                    
                    diagnosis[diagnosis_id] = dict()

                    for colname in colnames:
                        
                        diagnosis[diagnosis_id][colname] = record[colname] if record[colname] != '' else None

                    # Fix the data type for this field.

                    diagnosis[diagnosis_id]['age_at_diagnosis'] = int( record['age_at_diagnosis'] ) if record['age_at_diagnosis'] != '' else None

                    # Seed empty data structures for list elements.

                    diagnosis[diagnosis_id]['identifier'] = list()
                    diagnosis[diagnosis_id]['Treatment'] = list()

        with gzip.open( diagnosis_identifier_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                diagnosis_id = record['diagnosis_id']

                system = record['system']

                field_name = record['field_name']

                value = record['value']

                if diagnosis_id not in diagnosis:
                    
                    sys.exit(f"Unexpected failure? Diagnosis record with diagnosis_id {diagnosis_id} found in diagnosis_identifier.tsv but not in diagnosis.tsv; aborting.\n")

                diagnosis[diagnosis_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the treatment table and its association tables.

        with gzip.open( treatment_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                treatment_id = record['id']

                if treatment_id in treatment:
                    
                    sys.exit(f"Unexpected failure? Duplicate treatment record {treatment_id}; aborting.")

                else:
                    
                    treatment[treatment_id] = dict()

                    for colname in colnames:
                        
                        treatment[treatment_id][colname] = record[colname] if record[colname] != '' else None

                    # Fix the data type for these fields.

                    treatment[treatment_id]['days_to_treatment_start'] = int( record['days_to_treatment_start'] ) if record['days_to_treatment_start'] != '' else None
                    treatment[treatment_id]['days_to_treatment_end'] = int( record['days_to_treatment_end'] ) if record['days_to_treatment_end'] != '' else None
                    treatment[treatment_id]['number_of_cycles'] = int( record['number_of_cycles'] ) if record['number_of_cycles'] != '' else None

                    # Seed empty data structures for list elements.

                    treatment[treatment_id]['identifier'] = list()

        with gzip.open( treatment_identifier_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                treatment_id = record['treatment_id']

                system = record['system']

                field_name = record['field_name']

                value = record['value']

                if treatment_id not in treatment:
                    
                    sys.exit(f"Unexpected failure? Treatment record with treatment_id {treatment_id} found in treatment_identifier.tsv but not in treatment.tsv; aborting.\n")

                treatment[treatment_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the specimen table and its association tables.

        with gzip.open( specimen_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                specimen_id = record['id']

                if specimen_id in specimen:
                    
                    sys.exit(f"Unexpected failure? Duplicate specimen record {specimen_id}; aborting.")

                else:
                    
                    specimen[specimen_id] = dict()

                    for colname in colnames:
                        
                        specimen[specimen_id][colname] = record[colname] if record[colname] != '' else None

                    # Fix the data type for this field.

                    specimen[specimen_id]['days_to_collection'] = int( record['days_to_collection'] ) if record['days_to_collection'] != '' else None

                    # Seed empty data structures for list elements.

                    specimen[specimen_id]['identifier'] = list()

        with gzip.open( specimen_identifier_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                specimen_id = record['specimen_id']

                system = record['system']

                field_name = record['field_name']

                value = record['value']

                if specimen_id not in specimen:
                    
                    sys.exit(f"Unexpected failure? Specimen with specimen_id {specimen_id} found in specimen_identifier.tsv but not in specimen.tsv; aborting.\n")

                specimen[specimen_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Connect treatments to diagnoses, sorting each diagnosis-attached treatment list.

        for diagnosis_id in diagnosis_has_treatment:
            
            for treatment_id in sorted( diagnosis_has_treatment[diagnosis_id] ):
                
                diagnosis[diagnosis_id]['Treatment'].append( treatment[treatment_id] )

            diagnosis[diagnosis_id]['Treatment'] = sorted( diagnosis[diagnosis_id]['Treatment'], key=lambda x: x['id'] )

        # Connect diagnoses to researchsubjects, sorting each researchsubject-attached diagnosis list.

        for rs_id in researchsubject_has_diagnosis:
            
            for diagnosis_id in sorted( researchsubject_has_diagnosis[rs_id] ):
                
                researchsubject[rs_id]['Diagnosis'].append( diagnosis[diagnosis_id] )

            researchsubject[rs_id]['Diagnosis'] = sorted( researchsubject[rs_id]['Diagnosis'], key=lambda x: x['id'] )

        # Connect specimens to researchsubjects, sorting each researchsubject-attached specimen list.

        for rs_id in researchsubject_has_specimen:
            
            for specimen_id in sorted( researchsubject_has_specimen[rs_id] ):
                
                researchsubject[rs_id]['Specimen'].append( specimen[specimen_id] )

            researchsubject[rs_id]['Specimen'] = sorted( researchsubject[rs_id]['Specimen'], key=lambda x: x['id'] )

        # Connect researchsubjects to subjects, sorting each subject-attached researchsubject list.

        for subject_id in subject_has_researchsubject:
            
            for rs_id in sorted( subject_has_researchsubject[subject_id] ):
                
                subject[subject_id]['ResearchSubject'].append( researchsubject[rs_id] )

            subject[subject_id]['ResearchSubject'] = sorted( subject[subject_id]['ResearchSubject'], key=lambda x: x['id'] )

        # Write the transformed JSON to the output file for direct loading into BigQuery.

        with gzip.open( output_file, 'w' ) as OUT:
            
            writer = jsonlines.Writer( OUT )
            
            { writer.write( subject[subject_id] ) for subject_id in sorted( subject ) }

    def create_integer_aliases_for_entity_tables( self ):
        
        print( 'Creating integer ID aliases...', file=sys.stderr )

        for input_file_basename in [ 'diagnosis.tsv.gz', 'file.tsv.gz', 'researchsubject.tsv.gz', 'specimen.tsv.gz', 'subject.tsv.gz', 'treatment.tsv.gz' ]:
            
            input_file = path.join( self.merged_tsv_dir, input_file_basename )

            print( f"   {input_file_basename}...", file=sys.stderr )

            output_file = path.join( self.merged_tsv_dir, re.sub( r'^(.*).tsv.gz', r'\1' + '_integer_aliases.tsv.gz', input_file_basename ) )

            with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                
                header = next(IN)

                current_alias = 0

                for next_line in IN:
                    
                    values = next_line.rstrip('\n').split('\t')

                    print( *[ values[0], current_alias ], sep='\t', file=OUT )

                    current_alias = current_alias + 1

        print( '...done.', file=sys.stderr )

    def transform_files_to_SQL( self ):
        
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

        # Data in these tables need to be refactored to use integer aliases (TSV versions in self.merged_tsv_dir contain full ID strings) before pushing to postgres.

        tables_with_aliases = {
            
            'diagnosis_identifier',
            'diagnosis_treatment',
            'file_associated_project',
            'file_identifier',
            'file_specimen',
            'file_subject',
            'researchsubject_diagnosis',
            'researchsubject_identifier',
            'researchsubject_specimen',
            'researchsubject_treatment',
            'specimen_identifier',
            'subject_associated_project',
            'subject_identifier',
            'subject_researchsubject',
            'treatment_identifier'
        }

        for input_file_basename in sorted( listdir( self.merged_tsv_dir ) ):
            
            if re.search( r'_integer_aliases\.tsv\.gz$', input_file_basename ) is None and re.search( r'\.tsv\.gz$', input_file_basename ) is not None:
                
                input_file = path.join( self.merged_tsv_dir, input_file_basename )

                print( input_file_basename )

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
                        # table, at the time this comment was written) or an association table that doesn't use integer aliases (like "diagnosis_identifier"
                        # at time of writing). Either way, transcode the TSV data directly into a COPY block.

                        with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                            
                            colnames = next(IN).rstrip('\n').split('\t')

                            # COPY public.diagnosis (id, primary_diagnosis, age_at_diagnosis, morphology, stage, grade, method_of_diagnosis) FROM stdin;

                            print( f"COPY {target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )

                            for next_line in IN:
                                
                                line = next_line.rstrip('\n')
    
                                record = dict( zip( colnames, [ value for value in line.split('\t') ] ) )
    
                                print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), end='\n', file=OUT )
    
                            print( r'\.', end='\n\n', file=OUT )
    
                    else:
                        
                        # Integer aliases have been created for this table: note that this will be a main entity table (like file or specimen) and not
                        # an association table (like diagnosis_identifier): we take care of the latter in the section just above, and also below in
                        # the create_integer_aliases_for_file_association_tables() and create_integer_aliases_for_other_association_tables() functions.
                        # 
                        # Transcode the TSV data into a COPY block, patching in the integer ID alias assigned to each entity record (adding
                        # a new "integer_id_alias" column for that purpose).

                        with gzip.open( input_file, 'rt' ) as IN, gzip.open( integer_alias_file, 'rt' ) as ALIAS, gzip.open( output_file, 'wt' ) as OUT:
                            
                            colnames = next(IN).rstrip('\n').split('\t')
    
                            colnames.append( 'integer_id_alias' )
    
                            print( f"COPY {target_table} (" + ', '.join( colnames ) + ') FROM stdin;', end='\n', file=OUT )
    
                            for next_line in IN:
                                
                                line = next_line.rstrip('\n')
    
                                record = dict( zip( colnames[:-1], [ value for value in line.split('\t') ] ) )
    
                                id_alias = next(ALIAS).rstrip('\n').split('\t')[1]
    
                                record['integer_id_alias'] = id_alias
    
                                print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), end='\n', file=OUT )
    
                            print( r'\.', end='\n\n', file=OUT )

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

        # Then delete all table rows and replace them with the new data
        # (via the .sql files in table_file_dir/ ).

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

    def create_integer_aliases_for_file_association_tables( self ):
        
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

            while not scan_complete:
                
                print( f"file<->{type_to_link}: skipping {lines_to_skip} lines, loading next <= {self.max_alias_cache_size}...", file=sys.stderr )

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

            print( f"file<->{type_to_link}: done", file=sys.stderr )

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

            while not scan_complete:
                
                print( f"file<->{type_to_link}: skipping {lines_to_skip} lines, loading next <= {self.max_alias_cache_size}...", file=sys.stderr )

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

            print( f"file<->{type_to_link}: done", file=sys.stderr )

    def create_integer_aliases_for_other_association_tables( self ):
        
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

            aliases = {
                'diagnosis': dict(),
                'rs': dict(),
                'specimen': dict(),
                'subject': dict(),
                'treatment': dict()
            }

            if type_to_link in [ 'diagnosis_identifier', 'diagnosis_treatment', 'rs_diagnosis' ]:
                
                with gzip.open( id_alias_file['diagnosis'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ diagnosis_id, diagnosis_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['diagnosis'][diagnosis_id] = diagnosis_alias

            if type_to_link in [ 'rs_diagnosis', 'rs_identifier', 'rs_specimen', 'rs_treatment', 'subject_rs' ]:
                
                with gzip.open( id_alias_file['researchsubject'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ rs_id, rs_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['rs'][rs_id] = rs_alias

            if type_to_link in [ 'rs_specimen', 'specimen_identifier' ]:
                
                with gzip.open( id_alias_file['specimen'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ specimen_id, specimen_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['specimen'][specimen_id] = specimen_alias

            if type_to_link in [ 'subject_rs', 'subject_associated_project', 'subject_identifier' ]:
                        
                with gzip.open( id_alias_file['subject'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ subject_id, subject_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['subject'][subject_id] = subject_alias

            if type_to_link in [ 'diagnosis_treatment', 'rs_treatment', 'treatment_identifier' ]:
                        
                with gzip.open( id_alias_file['treatment'], 'rt' ) as IN:
                    
                    for next_line in IN:
                        
                        [ treatment_id, treatment_alias ] = next_line.rstrip( '\n' ).split( '\t' )

                        aliases['treatment'][treatment_id] = treatment_alias

            # Translate all matching records to aliased versions.

            with gzip.open( input_file, 'rt' ) as IN, gzip.open( output_file, 'wt' ) as OUT:
                
                if re.search( r'_identifier$', type_to_link ) is not None:
                    
                    entity_name = re.sub( r'_identifier$', r'', type_to_link )

                    full_entity_name = entity_name

                    if entity_name == 'rs':
                        
                        full_entity_name = 'researchsubject'
                    
                    print( f"COPY {type_to_link} ( {full_entity_name}_alias, system, field_name, value ) FROM stdin;", end='\n', file=OUT )

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

            print( f"{type_to_link}: done", file=sys.stderr )



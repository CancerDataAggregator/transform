import gzip
import jsonlines
import re
import sys

from os import listdir, makedirs, path

from etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

class CDA_loader:
    
    def __init__( self ):
        
        # Maximum number of file records to assemble in memory before dumping to JSONL.

        self.max_record_cache_size = 2000000

        # Maximum number of file records per JSONL output file.

        self.max_records_per_file = 20000000

        self.merged_tsv_dir = path.join( 'cda_tsvs', 'merged_idc_gdc_and_pdc_tables' )

        self.jsonl_output_dir = 'BigQuery_JSONL'

        self.jsonl_log_dir = path.join( self.jsonl_output_dir, '__processing_logs' )

        self.sql_output_dir = 'SQL_data'

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
                            
                            sys.exit(f"WTF? Duplicate file record {file_id}; aborting.")

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
                    
                    sys.exit(f"WTF? Duplicate subject record {subject_id}; aborting.")

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
                    
                    sys.exit(f"WTF? Subject with subject_id {subject_id} found in subject_identifier.tsv but not in subject.tsv; aborting.\n")

                subject[subject_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        with gzip.open( subject_associated_project_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                subject_id = record['subject_id']

                associated_project = record['associated_project']

                if subject_id not in subject:
                    
                    sys.exit(f"WTF? Subject with subject_id {subject_id} found in subject_associated_project.tsv but not in subject.tsv; aborting.\n")

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
                    
                    sys.exit(f"WTF? Duplicate researchsubject record {rs_id}; aborting.")

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
                    
                    sys.exit(f"WTF? ResearchSubject with rs_id {rs_id} found in rs_identifier.tsv but not in researchsubject.tsv; aborting.\n")

                researchsubject[rs_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the diagnosis table and its association tables.

        with gzip.open( diagnosis_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                diagnosis_id = record['id']

                if diagnosis_id in diagnosis:
                    
                    sys.exit(f"WTF? Duplicate diagnosis record {diagnosis_id}; aborting.")

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
                    
                    sys.exit(f"WTF? Diagnosis record with diagnosis_id {diagnosis_id} found in diagnosis_identifier.tsv but not in diagnosis.tsv; aborting.\n")

                diagnosis[diagnosis_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the treatment table and its association tables.

        with gzip.open( treatment_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                treatment_id = record['id']

                if treatment_id in treatment:
                    
                    sys.exit(f"WTF? Duplicate treatment record {treatment_id}; aborting.")

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
                    
                    sys.exit(f"WTF? Treatment record with treatment_id {treatment_id} found in treatment_identifier.tsv but not in treatment.tsv; aborting.\n")

                treatment[treatment_id]['identifier'].append( { 'system': system, 'field_name' : field_name, 'value' : value } )

        # Load mappable metadata from the specimen table and its association tables.

        with gzip.open( specimen_input_tsv, 'rt' ) as IN:

            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                specimen_id = record['id']

                if specimen_id in specimen:
                    
                    sys.exit(f"WTF? Duplicate specimen record {specimen_id}; aborting.")

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
                    
                    sys.exit(f"WTF? Specimen with specimen_id {specimen_id} found in specimen_identifier.tsv but not in specimen.tsv; aborting.\n")

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

    def transform_files_to_SQL( self ):
        
        initial_command_file = path.join( self.sql_output_dir, 'clear_tables_and_drop_indices.sql' )

        run_first_dir = path.join( self.sql_output_dir, 'run_first' )

        run_second_dir = path.join( self.sql_output_dir, 'run_second' )

        for output_dir in [ run_first_dir, run_second_dir ]:
            
            if not path.exists( output_dir ):
                
                makedirs( output_dir )

        # Save second-pass SQL delete commands to be executed prior to DB repopulation.
        # (Dump the first-pass commands inline into `initial_command_file` as we go
        # through the following loop.)

        second_round_delete_commands = []

        with open( initial_command_file, 'w' ) as CMD:
            
            for input_file_basename in sorted( listdir( self.merged_tsv_dir ) ):
                
                if re.search( r'\.tsv\.gz$', input_file_basename ) is not None:
                    
                    input_file = path.join( self.merged_tsv_dir, input_file_basename )

                    print( input_file_basename )

                    # Figure out which fields are integers.

                    integer_colnames = set()

                    with gzip.open( input_file, 'rt' ) as IN:
                        
                        colnames = next(IN).rstrip('\n').split('\t')

                        column_all_integers = dict( zip( colnames, [ True ] * len( colnames ) ) )

                        column_null_count = dict( zip( colnames, [ 0 ] * len( colnames ) ) )

                        row_count = 0

                        for next_line in IN:
                            
                            line = next_line.rstrip('\n')
                            
                            row_count = row_count + 1

                            values = line.split('\t')

                            for i in range( 0, len( values ) ):
                                
                                if values[i] == '':
                                    
                                    column_null_count[colnames[i]] = column_null_count[colnames[i]] + 1

                                elif re.search( r'^-?\d+$', values[i] ) is None:
                                    
                                    # We don't have any of these right now, but if they crop up, they'll need
                                    # their own handling for SQL translation, so vomit loudly if any are encountered.

                                    if re.search( r'^-?\d+(\.\d+)?$', values[i] ) is not None:
                                        
                                        sys.exit( f"Floating-point value detected in column {colnames[i]}, row {row_count}; aborting.\n" )
                                    
                                    column_all_integers[colnames[i]] = False

                        for colname in colnames:
                            
                            if column_all_integers[colname] and row_count > 0 and column_null_count[colname] < row_count:
                                
                                integer_colnames.add( colname )

                    # Translate rows to SQL INSERT statements and create a prepared statement to
                    # (clear previous table and) populate the table corresponding to the TSV being scanned.

                    target_table = re.sub( r'\.tsv\.gz$', '', input_file_basename )

                    output_file_basename = re.sub( r'\.tsv\.gz$', '.sql.gz', input_file_basename )

                    output_file = path.join( run_first_dir, output_file_basename )

                    if re.search( r'_', output_file_basename ) is not None:
                        
                        output_file = path.join( run_second_dir, output_file_basename )
                        
                        print( f"TRUNCATE {target_table};", end='\n\n', file=CMD )

                    else:
                        
                        second_round_delete_commands.append( f"TRUNCATE {target_table} CASCADE;" )

                    with gzip.open( input_file, 'rt' ) as IN:
                        
                        with gzip.open( output_file, 'wt' ) as OUT:
                            
                            colnames = next(IN).rstrip('\n').split('\t')

                            for next_line in IN:
                                
                                line = next_line.rstrip('\n')

                                # Escape quote marks for SQL compliance.

                                record = dict( zip( colnames, [ re.sub( r"'", "''", value ) for value in line.split('\t') ] ) )

                                print( f"INSERT INTO {target_table} ( " + ', '.join( colnames ) + ' ) VALUES ( ' + ', '.join( [ 'NULL' if len( record[colname] ) == 0 else f"'{record[colname]}'" if colname not in integer_colnames else f"{record[colname]}" for colname in colnames ] ) + ' );', end='\n', file=OUT )

            for delete_command in second_round_delete_commands:
                
                print( delete_command, end='\n\n', file=CMD )



import gzip
import jsonlines
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import map_columns_one_to_one, sort_file_with_header

class IDC_aggregator:
    
    def __init__( self, source_version ):
        
        self.source_version = source_version

        self.extract_dir = path.join( 'extracted_data', 'idc', self.source_version )
        self.idc_cda_dir = path.join( 'cda_tsvs', 'idc', self.source_version )

        self.merged_input_dir = path.join( 'cda_tsvs', 'merged_gdc_and_pdc_tables' )
        self.merged_output_dir = path.join( 'cda_tsvs', 'merged_idc_gdc_and_pdc_tables' )

        self.aux_root = 'auxiliary_metadata'
        self.merge_log_dir = path.join( self.aux_root, '__merge_logs' )
        self.project_crossref_dir = path.join( self.aux_root, '__project_crossrefs' )

        self.input_files = {
            
            'gdc_project_affiliations' : path.join( self.project_crossref_dir, 'GDC_entity_submitter_id_to_program_name_and_project_id.tsv' ),
            'gdc_to_idc_project_map' : path.join( self.project_crossref_dir, 'naive_GDC-IDC_project_id_map.hand_edited_to_remove_false_positives.tsv' ),
            'pdc_project_affiliations' : path.join( self.project_crossref_dir, 'PDC_entity_submitter_id_to_program_project_and_study.tsv' ),
            'idc_to_pdc_project_map' : path.join( self.project_crossref_dir, 'naive_IDC-PDC_project_id_map.hand_edited_to_remove_false_positives.tsv' ),
            'gdc_pdc_merged_subject_ids' : path.join( self.merge_log_dir, 'PDC_subject_IDs_absorbed_into_corresponding_GDC_subject_IDs.tsv' ),
            'subject_identifier' : path.join( self.idc_cda_dir, 'subject_identifier.tsv.gz' ),
            'subject_associated_project' : path.join( self.idc_cda_dir, 'subject_associated_project.tsv.gz' )
        }

        self.aux_files = {
            
            'idc_subject_id_to_gdc_subject_id' : path.join( self.merge_log_dir, 'idc_subject_id_to_gdc_subject_id.tsv' ),
            'idc_subject_id_to_pdc_subject_id' : path.join( self.merge_log_dir, 'idc_subject_id_to_pdc_subject_id.tsv' ),
            'all_merged_subject_ids_so_far' : path.join( self.merge_log_dir, 'merged_subject_ids.IDC_GDC_PDC.tsv' ),
            'idc_subject_id_to_cda_subject_id' : path.join( self.merge_log_dir, 'idc_subject_id_to_cda_subject_id.tsv' )
        }

        for target_dir in [ self.merged_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def match_idc_subject_ids_with_other_dcs( self ):
        
        self.__match_idc_subject_ids_with_gdc_subject_ids()

        self.__match_idc_subject_ids_with_pdc_subject_ids()

        self.__merge_match_lists()

        self.__validate_GDC_PDC_merge_pairs()

        self.__map_idc_subject_ids_to_cda_targets()

    def __match_idc_subject_ids_with_gdc_subject_ids ( self ):
        
        output_file = self.aux_files['idc_subject_id_to_gdc_subject_id']

        idc_subject_id_to_collection_id = dict()

        with gzip.open( self.input_files['subject_associated_project'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                idc_subject_id_to_collection_id[subject_id] = associated_project

        idc_submitter_case_id_to_subject_id = dict()

        with gzip.open( self.input_files['subject_identifier'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split('\t')

                if field_name == 'submitter_case_id':
                    
                    idc_submitter_case_id_to_subject_id[id_value] = subject_id

        valid_gdc_project_targets = dict()

        with open( self.input_files['gdc_to_idc_project_map'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # match_count	GDC_program_name	GDC_project_id	IDC_collection_id
                
                collection_id = record['IDC_collection_id']

                project_id = record['GDC_project_id']

                if collection_id not in valid_gdc_project_targets:
                    
                    valid_gdc_project_targets[collection_id] = set()

                valid_gdc_project_targets[collection_id].add( project_id )

        idc_subject_id_to_gdc_subject_id = dict()

        with open( self.input_files['gdc_project_affiliations'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # entity_type	entity_submitter_id	GDC_project_id	GDC_program_name

                entity_type = record['entity_type']

                if entity_type == 'case':
                    
                    submitter_id = record['entity_submitter_id']

                    current_gdc_project = record['GDC_project_id']

                    if submitter_id in idc_submitter_case_id_to_subject_id:
                        
                        subject_id = idc_submitter_case_id_to_subject_id[submitter_id]

                        collection_id = idc_subject_id_to_collection_id[subject_id]

                        if collection_id in valid_gdc_project_targets:
                            
                            for gdc_project in valid_gdc_project_targets[collection_id]:
                                
                                if gdc_project == current_gdc_project:
                                    
                                    # GDC CDA subject ID: program_name.case_submitter_id

                                    idc_subject_id_to_gdc_subject_id[subject_id] = record['GDC_program_name'] + '.' + submitter_id

        with open( output_file, 'w' ) as OUT:
            
            print( *[ 'IDC_subject_id', 'GDC_subject_id' ], sep='\t', end='\n', file=OUT )

            for subject_id in sorted( idc_subject_id_to_gdc_subject_id ):
                
                print( *[ subject_id, idc_subject_id_to_gdc_subject_id[subject_id] ], sep='\t', end='\n', file=OUT )

    def __match_idc_subject_ids_with_pdc_subject_ids ( self ):
        
        output_file = self.aux_files['idc_subject_id_to_pdc_subject_id']

        idc_subject_id_to_collection_id = dict()

        with gzip.open( self.input_files['subject_associated_project'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                idc_subject_id_to_collection_id[subject_id] = associated_project

        idc_submitter_case_id_to_subject_id = dict()

        with gzip.open( self.input_files['subject_identifier'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split('\t')

                if field_name == 'submitter_case_id':
                    
                    idc_submitter_case_id_to_subject_id[id_value] = subject_id

        valid_pdc_study_targets = dict()

        with open( self.input_files['idc_to_pdc_project_map'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # match_count	IDC_collection_id	PDC_program_name	PDC_project_submitter_id	PDC_study_submitter_id	PDC_pdc_study_id
                
                collection_id = record['IDC_collection_id']

                study_id = record['PDC_pdc_study_id']

                if collection_id not in valid_pdc_study_targets:
                    
                    valid_pdc_study_targets[collection_id] = set()

                valid_pdc_study_targets[collection_id].add( study_id )

        idc_subject_id_to_pdc_subject_id = dict()

        with open( self.input_files['pdc_project_affiliations'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

                entity_type = record['entity_type']

                if entity_type == 'case':
                    
                    submitter_id = record['entity_submitter_id']

                    current_pdc_study = record['pdc_study_id']

                    if submitter_id in idc_submitter_case_id_to_subject_id:
                        
                        subject_id = idc_submitter_case_id_to_subject_id[submitter_id]

                        collection_id = idc_subject_id_to_collection_id[subject_id]

                        if collection_id in valid_pdc_study_targets:
                            
                            for pdc_study in valid_pdc_study_targets[collection_id]:
                                
                                if pdc_study == current_pdc_study:
                                    
                                    # PDC CDA subject ID: project_submitter_id.case_submitter_id

                                    idc_subject_id_to_pdc_subject_id[subject_id] = record['project_submitter_id'] + '.' + submitter_id

        with open( output_file, 'w' ) as OUT:
            
            print( *[ 'IDC_subject_id', 'PDC_subject_id' ], sep='\t', end='\n', file=OUT )

            for subject_id in sorted( idc_subject_id_to_pdc_subject_id ):
                
                print( *[ subject_id, idc_subject_id_to_pdc_subject_id[subject_id] ], sep='\t', end='\n', file=OUT )

    def __merge_match_lists( self ):
        
        gdc_subject_to_pdc_subject_full = map_columns_one_to_one( self.input_files['gdc_pdc_merged_subject_ids'], 'GDC_subject_id', 'PDC_subject_id' )

        idc_subject_to_gdc_subject = map_columns_one_to_one( self.aux_files['idc_subject_id_to_gdc_subject_id'], 'IDC_subject_id', 'GDC_subject_id' )

        idc_subject_to_pdc_subject = map_columns_one_to_one( self.aux_files['idc_subject_id_to_pdc_subject_id'], 'IDC_subject_id', 'PDC_subject_id' )

        gdc_subject_to_pdc_subject_observed = dict()

        all_matched_idc_subject_ids = set()

        for idc_subject_id in sorted( idc_subject_to_gdc_subject ):
            
            all_matched_idc_subject_ids.add( idc_subject_id )

        for idc_subject_id in sorted( idc_subject_to_pdc_subject ):
            
            all_matched_idc_subject_ids.add( idc_subject_id )

        with open( self.aux_files['all_merged_subject_ids_so_far'], 'w' ) as OUT:
            
            print( *[ 'IDC_subject_id', 'GDC_subject_id', 'PDC_subject_id' ], sep='\t', end='\n', file=OUT )

            for idc_subject_id in sorted( all_matched_idc_subject_ids ):
                
                gdc_subject_id = ''

                if idc_subject_id in idc_subject_to_gdc_subject:
                    
                    gdc_subject_id = idc_subject_to_gdc_subject[idc_subject_id]

                pdc_subject_id = ''

                if idc_subject_id in idc_subject_to_pdc_subject:
                    
                    pdc_subject_id = idc_subject_to_pdc_subject[idc_subject_id]

                print( *[ idc_subject_id, gdc_subject_id, pdc_subject_id ], sep='\t', end='\n', file=OUT )

                if gdc_subject_id != '' and pdc_subject_id != '':
                    
                    gdc_subject_to_pdc_subject_observed[gdc_subject_id] = pdc_subject_id

            idc_subject_id = ''

            for gdc_subject_id in sorted( gdc_subject_to_pdc_subject_full ):
                
                if gdc_subject_id not in gdc_subject_to_pdc_subject_observed:
                    
                    print( *[ idc_subject_id, gdc_subject_id, gdc_subject_to_pdc_subject_full[gdc_subject_id] ], sep='\t', end='\n', file=OUT )

    def __validate_GDC_PDC_merge_pairs( self ):
        
        gdc_pdc_merges = dict()

        with open( self.input_files['gdc_pdc_merged_subject_ids'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # PDC_subject_id	GDC_subject_id

                gdc_id = record['GDC_subject_id']

                pdc_id = record['PDC_subject_id']

                if gdc_id not in gdc_pdc_merges:
                    
                    gdc_pdc_merges[gdc_id] = set()

                gdc_pdc_merges[gdc_id].add( pdc_id )

        with open( self.aux_files['all_merged_subject_ids_so_far'] ) as IN:
            
            header = next(IN).rstrip('\n')

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                # IDC_subject_id	GDC_subject_id	PDC_subject_id

                gdc_id = record['GDC_subject_id']

                pdc_id = record['PDC_subject_id']

                if gdc_id != '' and pdc_id != '' and ( gdc_id not in gdc_pdc_merges or pdc_id not in gdc_pdc_merges[gdc_id] ):
                    
                    print( f"WARNING: putatively linked records (GDC) {gdc_id} and (PDC) {pdc_id} not merged during PDC matching phase. Investigate.", sep='', end='\n', file=sys.stderr )

    def __map_idc_subject_ids_to_cda_targets( self ):
        
        with open( self.aux_files['all_merged_subject_ids_so_far'] ) as IN, open( self.aux_files['idc_subject_id_to_cda_subject_id'], 'w' ) as OUT:
            
            colnames = next(IN).rstrip('\n').split('\t')

            print( *[ 'IDC_subject_id', 'CDA_subject_id' ], sep='\t', end='\n', file=OUT )

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                idc_id = record['IDC_subject_id']

                if idc_id != '':
                    
                    cda_id = record['GDC_subject_id']

                    if cda_id == '':
                        
                        cda_id = record['PDC_subject_id']

                        if cda_id == '':
                            
                            sys.exit( "SOMETHING TECHNICALLY IMPOSSIBLE WENT HORRIBLY WRONG!" )

                    print( *[ idc_id, cda_id ], sep='\t', end='\n', file=OUT )

    def merge_into_existing_cda_data( self ):
        
        cda_file_subject_input_tsv = path.join( self.merged_input_dir, 'file_subject.tsv' )

        cda_subject_input_tsv = path.join( self.merged_input_dir, 'subject.tsv' )

        cda_subject_associated_project_input_tsv = path.join( self.merged_input_dir, 'subject_associated_project.tsv' )

        cda_subject_identifier_input_tsv = path.join( self.merged_input_dir, 'subject_identifier.tsv' )

        cda_subject_researchsubject_input_tsv = path.join( self.merged_input_dir, 'subject_researchsubject.tsv' )

        cross_dc_subject_id_map = self.aux_files['idc_subject_id_to_cda_subject_id']

        idc_file_subject_input_tsv = path.join( self.idc_cda_dir, 'file_subject.tsv.gz' )

        idc_subject_input_tsv = path.join( self.idc_cda_dir, 'subject.tsv.gz' )

        idc_subject_associated_project_input_tsv = self.input_files['subject_associated_project']

        idc_subject_identifier_input_tsv = self.input_files['subject_identifier']

        idc_subject_researchsubject_input_tsv = path.join( self.idc_cda_dir, 'subject_researchsubject.tsv.gz' )

        file_subject_output_tsv = path.join( self.merged_output_dir, 'file_subject.tsv.gz' )

        subject_output_tsv = path.join( self.merged_output_dir, 'subject.tsv.gz' )

        subject_associated_project_output_tsv = path.join( self.merged_output_dir, 'subject_associated_project.tsv.gz' )

        subject_identifier_output_tsv = path.join( self.merged_output_dir, 'subject_identifier.tsv.gz' )

        subject_researchsubject_output_tsv = path.join( self.merged_output_dir, 'subject_researchsubject.tsv.gz' )

        subject_merged_log = path.join( self.merge_log_dir, 'IDC_subject_IDs_absorbed_into_corresponding_GDC_PDC_merged_subject_IDs.tsv' )

        null_sub_log = path.join( self.merge_log_dir, 'IDC_values_replacing_GDC_PDC_merged_nulls_by_subject_column_name.tsv' )

        clashes_ignored_log = path.join( self.merge_log_dir, 'IDC_values_unused_but_different_from_GDC_PDC_merged_values_by_subject_column_name_and_dataset.tsv' )

        # First, identify all pairs of IDC & GDC+PDC subject records to be merged.

        cda_subject_id_to_idc_subject_id = map_columns_one_to_one( cross_dc_subject_id_map, 'CDA_subject_id', 'IDC_subject_id' )

        idc_subject_id_to_cda_subject_id = map_columns_one_to_one( cross_dc_subject_id_map, 'IDC_subject_id', 'CDA_subject_id' )

        # Next, merge the subject_* association tables.

        with gzip.open( subject_associated_project_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with open( cda_subject_associated_project_input_tsv ) as IN:
                
                header = next(IN).rstrip('\n')

                print( header, end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    print( line, end='\n', file=OUT )

            # Port over records from IDC, translating subject_id where appropriate.

            with gzip.open( idc_subject_associated_project_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    if record['subject_id'] in idc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = idc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

        sort_file_with_header( subject_associated_project_output_tsv, gzipped=True )

        with gzip.open( subject_identifier_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with open( cda_subject_identifier_input_tsv ) as IN:
                
                header = next(IN).rstrip('\n')

                print( header, end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    print( line, end='\n', file=OUT )

            # Port over records from IDC, translating subject_id where appropriate.

            with gzip.open( idc_subject_identifier_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    if record['subject_id'] in idc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = idc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

        sort_file_with_header( subject_identifier_output_tsv, gzipped=True )

        with gzip.open( subject_researchsubject_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with open( cda_subject_researchsubject_input_tsv ) as IN:
                
                header = next(IN).rstrip('\n')

                print( header, end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    print( line, end='\n', file=OUT )

            # Port over records from IDC, translating subject_id where appropriate.

            with gzip.open( idc_subject_researchsubject_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    if record['subject_id'] in idc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = idc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

        sort_file_with_header( subject_researchsubject_output_tsv, gzipped=True )

        # Next merge file_subject.tsv, whose Subject IDs need updating for any IDC Subjects whose
        # CDA IDs have been superseded by those of their corresponding GDC+PDC-merged Subjects.

        with gzip.open( file_subject_output_tsv, 'wt' ) as OUT:
            
            # Copy over the records from GDC+PDC.

            with open( cda_file_subject_input_tsv ) as IN:
                
                header = next(IN).rstrip('\n')

                print( header, end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    print( line, end='\n', file=OUT )

            # Port over records from IDC, translating subject_id where appropriate.

            with gzip.open( idc_file_subject_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    if record['subject_id'] in idc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = idc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

        # Now merge subject.tsv, log merges, and keep track of any data conflicts encountered.

        # Preload records from IDC.

        idc_subject_preload = dict()

        with gzip.open( idc_subject_input_tsv, 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                if record['id'] in idc_subject_id_to_cda_subject_id:
                    
                    record['id'] = idc_subject_id_to_cda_subject_id[record['id']]

                idc_subject_preload[record['id']] = record

        # Translate/upgrade GDC+PDC-merged records and add in any unmapped IDC records as well.

        merged_from_idc = set()

        clashes_ignored = dict()

        null_subs_made = dict()

        with gzip.open( subject_output_tsv, 'wt' ) as OUT:
            
            # Use the GDC+PDC-merged file as the first source for output records, updating with IDC subject data where possible.

            with open( cda_subject_input_tsv ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                print( *colnames, sep='\t', end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    current_id = record['id']

                    if current_id in idc_subject_preload:
                        
                        merged_from_idc.add( cda_subject_id_to_idc_subject_id[current_id] )

                        for colname in colnames:
                            
                            # Replace any values that are null in the existing GDC+PDC-merged data with corresponding non-null values from IDC.

                            if record[colname] is None or record[colname] == '':
                                
                                if idc_subject_preload[current_id][colname] is not None and idc_subject_preload[current_id][colname] != '':
                                    
                                    record[colname] = idc_subject_preload[current_id][colname]

                                    if colname not in null_subs_made:
                                        
                                        null_subs_made[colname] = dict()

                                    if idc_subject_preload[current_id][colname] not in null_subs_made[colname]:
                                        
                                        null_subs_made[colname][idc_subject_preload[current_id][colname]] = 1

                                    else:
                                        
                                        null_subs_made[colname][idc_subject_preload[current_id][colname]] = null_subs_made[colname][idc_subject_preload[current_id][colname]] + 1

                            elif record[colname] is not None and record[colname] != '':
                                
                                cda_value = record[colname]

                                if idc_subject_preload[current_id][colname] is None or idc_subject_preload[current_id][colname] == '':
                                    
                                    idc_value = ''

                                else:
                                    
                                    idc_value = idc_subject_preload[current_id][colname]

                                if idc_value != cda_value:
                                    
                                    if colname not in clashes_ignored:
                                        
                                        clashes_ignored[colname] = dict()

                                    if idc_value not in clashes_ignored[colname]:
                                        
                                        clashes_ignored[colname][idc_value] = dict()

                                    if cda_value not in clashes_ignored[colname][idc_value]:
                                        
                                        clashes_ignored[colname][idc_value][cda_value] = 1

                                    else:
                                        
                                        clashes_ignored[colname][idc_value][cda_value] = clashes_ignored[colname][idc_value][cda_value] + 1

                    print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

            # Use the IDC file as the next source for output records, ignoring any IDC subject data already scanned.

            with gzip.open( idc_subject_input_tsv, 'rt' ) as IN:
                
                colnames = next(IN).rstrip('\n').split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    current_id = record['id']

                    if current_id not in merged_from_idc:
                        
                        print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

        sort_file_with_header( subject_output_tsv, gzipped=True )

        # Log GDC+PDC-merged null values that were replaced with non-null data from IDC.

        with open( null_sub_log, 'w' ) as LOG:
            
            print( *[ 'subject_column', 'IDC_value', 'substitution_count' ], sep='\t', end='\n', file=LOG )

            for colname in sorted( null_subs_made ):
                
                for value in sorted( null_subs_made[colname] ):
                    
                    print( *[ colname, value, null_subs_made[colname][value] ], sep='\t', end='\n', file=LOG )

        # Log non-null IDC values that didn't match existing GDC+PDC-merged CDA metadata values (these were NOT used to update CDA metadata).

        with open( clashes_ignored_log, 'w' ) as LOG:
            
            print( *[ 'subject_column', 'IDC_value', 'GDC_PDC_merged_value', 'match_count' ], sep='\t', end='\n', file=LOG )

            for colname in sorted( clashes_ignored ):
                
                for idc_value in sorted( clashes_ignored[colname] ):
                    
                    for cda_value in sorted( clashes_ignored[colname][idc_value] ):
                        
                        print( *[ colname, idc_value, cda_value, clashes_ignored[colname][idc_value][cda_value] ], sep='\t', end='\n', file=LOG )

        # Log before- and after-IDs for IDC subject records that got merged with (subsumed by) GDC+PDC-merged subject records.

        with open( subject_merged_log, 'w' ) as LOG:
            
            print( *[ 'IDC_subject_id', 'GDC_PDC_merged_subject_id' ], sep='\t', end='\n', file=LOG )

            for idc_subject_id in sorted( merged_from_idc ):
                
                cda_subject_id = idc_subject_id_to_cda_subject_id[idc_subject_id]

                print( *[ idc_subject_id, cda_subject_id ], sep='\t', end='\n', file=LOG )

        # Concatenate the rest of everything. We're only merging records at the subject level at this time.

        for file_basename in sorted( listdir( self.merged_input_dir ) ):
            
            # We already covered subject_* and file_subject; don't parse anything not ending in '.tsv'.

            if re.search( r'^subject', file_basename ) is None and file_basename != 'file_subject.tsv' and re.search( r'\.tsv$', file_basename ) is not None:
                
                cda_file_full_path = path.join( self.merged_input_dir, file_basename )

                idc_file_full_path = path.join( self.idc_cda_dir, file_basename + '.gz' )

                dest_file_full_path = path.join( self.merged_output_dir, file_basename + '.gz' )

                with gzip.open( dest_file_full_path, 'wt' ) as OUT:
                    
                    with open( cda_file_full_path ) as IN:
                        
                        output_colnames = next(IN).rstrip('\n').split('\t')

                        print( *output_colnames, sep='\t', end='\n', file=OUT )

                        for line in [ next_line.rstrip('\n') for next_line in IN ]:
                            
                            print( line, end='\n', file=OUT )

                    if path.isfile( idc_file_full_path ):
                        
                        with gzip.open( idc_file_full_path, 'rt' ) as IN:
                            
                            current_colnames = next(IN).rstrip('\n').split('\t')

                            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                                
                                record = dict( zip( current_colnames, line.split('\t') ) )

                                print( *[ record[colname] for colname in output_colnames ], sep='\t', end='\n', file=OUT )

                # File metadata is now too big to sort.

                if re.search( r'^file', file_basename ) is None:
                    
                    sort_file_with_header( dest_file_full_path, gzipped=True )



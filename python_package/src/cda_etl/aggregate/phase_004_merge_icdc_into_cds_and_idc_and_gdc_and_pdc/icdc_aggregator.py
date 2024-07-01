import gzip
import jsonlines
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import map_columns_one_to_one, sort_file_with_header

class ICDC_aggregator:
    
    def __init__( self ):
        
        self.icdc_cda_dir = path.join( 'cda_tsvs', 'icdc' )

        self.merged_input_dir = path.join( 'cda_tsvs', 'merged_cds_idc_gdc_and_pdc_tables' )
        self.merged_output_dir = path.join( 'cda_tsvs', 'merged_icdc_cds_idc_gdc_and_pdc_tables' )

        self.aux_root = 'auxiliary_metadata'
        self.gdc_metadata_dir = path.join( self.aux_root, '__GDC_supplemental_metadata' )
        self.pdc_metadata_dir = path.join( self.aux_root, '__PDC_supplemental_metadata' )
        self.idc_metadata_dir = path.join( self.aux_root, '__IDC_supplemental_metadata' )
        self.cds_metadata_dir = path.join( self.aux_root, '__CDS_supplemental_metadata' )
        self.merge_log_dir = path.join( self.aux_root, '__merge_logs' )
        self.project_crossref_dir = path.join( self.aux_root, '__project_crossrefs' )

        self.input_files = {
            
            'cds_project_affiliations' : path.join( self.cds_metadata_dir, 'CDS_entity_submitter_id_to_program_and_study.tsv' ),
            'icdc_to_cds_project_map' : path.join( self.project_crossref_dir, 'naive_ICDC-CDS_project_id_map.hand_edited_to_remove_false_positives.tsv' ),

            'gdc_project_affiliations' : path.join( self.gdc_metadata_dir, 'GDC_entity_submitter_id_to_program_name_and_project_id.tsv' ),
            'icdc_to_gdc_project_map' : path.join( self.project_crossref_dir, 'naive_ICDC-GDC_project_id_map.hand_edited_to_remove_false_positives.tsv' ),

            'idc_project_affiliations' : path.join( self.idc_metadata_dir, 'IDC_entity_submitter_id_to_collection_id.tsv' ),
            'icdc_to_idc_project_map' : path.join( self.project_crossref_dir, 'naive_ICDC-IDC_project_id_map.hand_edited_to_remove_false_positives.tsv' ),

            'pdc_project_affiliations' : path.join( self.pdc_metadata_dir, 'PDC_entity_submitter_id_to_program_project_and_study.tsv' ),
            'icdc_to_pdc_project_map' : path.join( self.project_crossref_dir, 'naive_ICDC-PDC_project_id_map.hand_edited_to_remove_false_positives.tsv' ),
            'pdc_merged_subject_ids' : path.join( self.merge_log_dir, 'PDC_initial_default_subject_IDs_aggregated_across_projects.tsv' ),

            'subject_identifier' : path.join( self.icdc_cda_dir, 'subject_identifier.tsv' ),
            'subject_associated_project' : path.join( self.icdc_cda_dir, 'subject_associated_project.tsv' )
        }

        self.aux_files = {
            
            'icdc_subject_id_to_gdc_subject_id' : path.join( self.merge_log_dir, 'icdc_subject_id_to_gdc_subject_id.tsv' ),
            'icdc_subject_id_to_pdc_subject_id' : path.join( self.merge_log_dir, 'icdc_subject_id_to_pdc_subject_id.tsv' ),
            'icdc_subject_id_to_idc_subject_id' : path.join( self.merge_log_dir, 'icdc_subject_id_to_idc_subject_id.tsv' ),
            'icdc_subject_id_to_cds_subject_id' : path.join( self.merge_log_dir, 'icdc_subject_id_to_cds_subject_id.tsv' ),
            'cds_idc_gdc_pdc_subject_map' : path.join( self.merge_log_dir, 'merged_subject_ids.CDS_IDC_GDC_PDC.tsv' ),
            'all_merged_subject_ids_so_far' : path.join( self.merge_log_dir, 'merged_subject_ids.ICDC_CDS_IDC_GDC_PDC.tsv' ),
            'icdc_subject_id_to_cda_subject_id' : path.join( self.merge_log_dir, 'icdc_subject_id_to_cda_subject_id.tsv' )
        }

        for target_dir in [ self.merged_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def match_icdc_subject_ids_with_other_dcs( self ):
        
        self.__match_icdc_subject_ids_with_gdc_subject_ids()

        self.__match_icdc_subject_ids_with_pdc_subject_ids()

        self.__match_icdc_subject_ids_with_idc_subject_ids()

        self.__match_icdc_subject_ids_with_cds_subject_ids()

        self.__merge_match_lists()

        self.__map_icdc_subject_ids_to_cda_targets()

    def __match_icdc_subject_ids_with_gdc_subject_ids ( self ):
        
        icdc_subject_id_to_study_id = dict()

        with open( self.input_files['subject_associated_project'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split( '\t' )

                icdc_subject_id_to_study_id[subject_id] = associated_project

        icdc_case_id_to_subject_id = dict()

        with open( self.input_files['subject_identifier'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split( '\t' )

                if field_name == 'case.case_id':
                    
                    icdc_case_id_to_subject_id[id_value] = subject_id

        valid_gdc_project_targets = dict()

        with open( self.input_files['icdc_to_gdc_project_map'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # match_count	ICDC_program_acronym	ICDC_study_name	ICDC_study_id	GDC_program_name	GDC_project_id
                
                study_id = record['ICDC_study_id']

                project_id = record['GDC_project_id']

                if study_id not in valid_gdc_project_targets:
                    
                    valid_gdc_project_targets[study_id] = set()

                valid_gdc_project_targets[study_id].add( project_id )

        icdc_subject_id_to_gdc_subject_id = dict()

        with open( self.input_files['gdc_project_affiliations'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # entity_type	entity_submitter_id	GDC_project_id	GDC_program_name

                entity_type = record['entity_type']

                if entity_type == 'case':
                    
                    submitter_id = record['entity_submitter_id']

                    current_gdc_project = record['GDC_project_id']

                    if submitter_id in icdc_case_id_to_subject_id:
                        
                        subject_id = icdc_case_id_to_subject_id[submitter_id]

                        study_id = icdc_subject_id_to_study_id[subject_id]

                        if study_id in valid_gdc_project_targets and current_gdc_project in valid_gdc_project_targets[study_id]:
                            
                            # GDC CDA subject ID: program_name.case_submitter_id

                            icdc_subject_id_to_gdc_subject_id[subject_id] = record['GDC_program_name'] + '.' + submitter_id

        with open( self.aux_files['icdc_subject_id_to_gdc_subject_id'], 'w' ) as OUT:
            
            print( *[ 'ICDC_subject_id', 'GDC_subject_id' ], sep='\t', file=OUT )

            for subject_id in sorted( icdc_subject_id_to_gdc_subject_id ):
                
                print( *[ subject_id, icdc_subject_id_to_gdc_subject_id[subject_id] ], sep='\t', file=OUT )

    def __match_icdc_subject_ids_with_pdc_subject_ids ( self ):
        
        # Track which PDC subject IDs got updated due to internal PDC merges.

        merged_pdc_subject_id = dict()

        with open( self.input_files['pdc_merged_subject_ids'] ) as IN:
            
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( column_names, line.split( '\t' ) ) )

                merged_pdc_subject_id[record['default_id']] = record['merged_id']

        icdc_subject_id_to_study_id = dict()

        with open( self.input_files['subject_associated_project'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split( '\t' )

                icdc_subject_id_to_study_id[subject_id] = associated_project

        icdc_case_id_to_subject_id = dict()

        with open( self.input_files['subject_identifier'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split( '\t' )

                if field_name == 'case.case_id':
                    
                    icdc_case_id_to_subject_id[id_value] = subject_id

        valid_pdc_study_targets = dict()

        with open( self.input_files['icdc_to_pdc_project_map'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # match_count	ICDC_program_acronym	ICDC_study_name	ICDC_study_id	PDC_program_name	PDC_project_submitter_id	PDC_study_submitter_id	PDC_pdc_study_id
                
                study_id = record['ICDC_study_id']

                pdc_study_id = record['PDC_pdc_study_id']

                if study_id not in valid_pdc_study_targets:
                    
                    valid_pdc_study_targets[study_id] = set()

                valid_pdc_study_targets[study_id].add( pdc_study_id )

        icdc_subject_id_to_pdc_subject_id = dict()

        with open( self.input_files['pdc_project_affiliations'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # program_name	project_submitter_id	study_submitter_id	pdc_study_id	entity_submitter_id	entity_id	entity_type

                entity_type = record['entity_type']

                if entity_type == 'case':
                    
                    submitter_id = record['entity_submitter_id']

                    current_pdc_study = record['pdc_study_id']

                    if submitter_id in icdc_case_id_to_subject_id:
                        
                        subject_id = icdc_case_id_to_subject_id[submitter_id]

                        study_id = icdc_subject_id_to_study_id[subject_id]

                        if study_id in valid_pdc_study_targets and current_pdc_study in valid_pdc_study_targets[study_id]:
                            
                            # PDC CDA subject ID: project_submitter_id.case_submitter_id

                            pdc_subject_id = record['project_submitter_id'] + '.' + submitter_id

                            # If we overrode the default by an internal merge within PDC, swap in the merged ID instead of the default.

                            if pdc_subject_id in merged_pdc_subject_id:
                                
                                pdc_subject_id = merged_pdc_subject_id[pdc_subject_id]

                            icdc_subject_id_to_pdc_subject_id[subject_id] = pdc_subject_id

        with open( self.aux_files['icdc_subject_id_to_pdc_subject_id'], 'w' ) as OUT:
            
            print( *[ 'ICDC_subject_id', 'PDC_subject_id' ], sep='\t', file=OUT )

            for subject_id in sorted( icdc_subject_id_to_pdc_subject_id ):
                
                print( *[ subject_id, icdc_subject_id_to_pdc_subject_id[subject_id] ], sep='\t', file=OUT )

    def __match_icdc_subject_ids_with_idc_subject_ids ( self ):
        
        icdc_subject_id_to_study_id = dict()

        with open( self.input_files['subject_associated_project'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split( '\t' )

                icdc_subject_id_to_study_id[subject_id] = associated_project

        icdc_case_id_to_subject_id = dict()

        with open( self.input_files['subject_identifier'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split( '\t' )

                if field_name == 'case.case_id':
                    
                    icdc_case_id_to_subject_id[id_value] = subject_id

        valid_idc_study_targets = dict()

        with open( self.input_files['icdc_to_idc_project_map'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # match_count	ICDC_program_acronym	ICDC_study_name	ICDC_study_id	IDC_collection_id

                study_id = record['ICDC_study_id']

                idc_collection_id = record['IDC_collection_id']

                if study_id not in valid_idc_study_targets:
                    
                    valid_idc_study_targets[study_id] = set()

                valid_idc_study_targets[study_id].add( idc_collection_id )

        icdc_subject_id_to_idc_subject_id = dict()

        with open( self.input_files['idc_project_affiliations'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # collection_id	submitter_case_id	idc_case_id	entity_type

                entity_type = record['entity_type']

                if entity_type == 'case':
                    
                    submitter_id = record['submitter_case_id']

                    current_idc_collection = record['collection_id']

                    if submitter_id in icdc_case_id_to_subject_id:
                        
                        subject_id = icdc_case_id_to_subject_id[submitter_id]

                        study_id = icdc_subject_id_to_study_id[subject_id]

                        if study_id in valid_idc_study_targets and current_idc_collection in valid_idc_study_targets[study_id]:
                            
                            # IDC CDA subject ID: collection_id.submitter_case_id

                            icdc_subject_id_to_idc_subject_id[subject_id] = f"{current_idc_collection}.{submitter_id}"

        with open( self.aux_files['icdc_subject_id_to_idc_subject_id'], 'w' ) as OUT:
            
            print( *[ 'ICDC_subject_id', 'IDC_subject_id' ], sep='\t', file=OUT )

            for subject_id in sorted( icdc_subject_id_to_idc_subject_id ):
                
                print( *[ subject_id, icdc_subject_id_to_idc_subject_id[subject_id] ], sep='\t', file=OUT )

    def __match_icdc_subject_ids_with_cds_subject_ids ( self ):
        
        icdc_subject_id_to_study_id = dict()

        with open( self.input_files['subject_associated_project'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split( '\t' )

                icdc_subject_id_to_study_id[subject_id] = associated_project

        icdc_case_id_to_subject_id = dict()

        with open( self.input_files['subject_identifier'] ) as IN:
            
            header = next( IN )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split( '\t' )

                if field_name == 'case.case_id':
                    
                    icdc_case_id_to_subject_id[id_value] = subject_id

        valid_cds_study_targets = dict()

        with open( self.input_files['icdc_to_cds_project_map'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # match_count	ICDC_program_acronym	ICDC_study_name	ICDC_study_id	CDS_program_acronym	CDS_study_name	CDS_study_id
                
                icdc_study_id = record['ICDC_study_id']

                cds_study_id = record['CDS_study_id']

                if icdc_study_id not in valid_cds_study_targets:
                    
                    valid_cds_study_targets[icdc_study_id] = set()

                valid_cds_study_targets[icdc_study_id].add( cds_study_id )

        icdc_subject_id_to_cds_subject_id = dict()

        with open( self.input_files['cds_project_affiliations'] ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            colnames = header.split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                # entity_type	entity_id	program_acronym study_name	study_id

                entity_type = record['entity_type']

                if entity_type == 'participant':
                    
                    submitter_id = record['entity_id']

                    current_cds_study = record['study_id']

                    if submitter_id in icdc_case_id_to_subject_id:
                        
                        subject_id = icdc_case_id_to_subject_id[submitter_id]

                        study_id = icdc_subject_id_to_study_id[subject_id]

                        if study_id in valid_cds_study_targets and current_cds_study in valid_cds_study_targets[study_id]:
                            
                            # CDS CDA subject ID: program_acronym.participant_id

                            icdc_subject_id_to_cds_subject_id[subject_id] = record['program_acronym'] + '.' + submitter_id

        with open( self.aux_files['icdc_subject_id_to_cds_subject_id'], 'w' ) as OUT:
            
            print( *[ 'ICDC_subject_id', 'CDS_subject_id' ], sep='\t', file=OUT )

            for subject_id in sorted( icdc_subject_id_to_cds_subject_id ):
                
                print( *[ subject_id, icdc_subject_id_to_cds_subject_id[subject_id] ], sep='\t', file=OUT )

    def __merge_match_lists( self ):
        
        icdc_subject_to_gdc_subject = map_columns_one_to_one( self.aux_files['icdc_subject_id_to_gdc_subject_id'], 'ICDC_subject_id', 'GDC_subject_id' )

        icdc_subject_to_pdc_subject = map_columns_one_to_one( self.aux_files['icdc_subject_id_to_pdc_subject_id'], 'ICDC_subject_id', 'PDC_subject_id' )

        icdc_subject_to_idc_subject = map_columns_one_to_one( self.aux_files['icdc_subject_id_to_idc_subject_id'], 'ICDC_subject_id', 'IDC_subject_id' )

        icdc_subject_to_cds_subject = map_columns_one_to_one( self.aux_files['icdc_subject_id_to_cds_subject_id'], 'ICDC_subject_id', 'CDS_subject_id' )

        observed_gdc_subjects = set()

        observed_pdc_subjects = set()

        observed_idc_subjects = set()

        observed_cds_subjects = set()

        all_matched_icdc_subject_ids = set()

        for icdc_subject_id in sorted( icdc_subject_to_gdc_subject ):
            
            all_matched_icdc_subject_ids.add( icdc_subject_id )

        for icdc_subject_id in sorted( icdc_subject_to_pdc_subject ):
            
            all_matched_icdc_subject_ids.add( icdc_subject_id )

        for icdc_subject_id in sorted( icdc_subject_to_idc_subject ):
            
            all_matched_icdc_subject_ids.add( icdc_subject_id )

        for icdc_subject_id in sorted( icdc_subject_to_cds_subject ):
            
            all_matched_icdc_subject_ids.add( icdc_subject_id )

        with open( self.aux_files['all_merged_subject_ids_so_far'], 'w' ) as OUT:
            
            print( *[ 'ICDC_subject_id', 'CDS_subject_id', 'IDC_subject_id', 'GDC_subject_id', 'PDC_subject_id' ], sep='\t', file=OUT )

            for icdc_subject_id in sorted( all_matched_icdc_subject_ids ):
                
                gdc_subject_id = ''

                if icdc_subject_id in icdc_subject_to_gdc_subject:
                    
                    gdc_subject_id = icdc_subject_to_gdc_subject[icdc_subject_id]

                    observed_gdc_subjects.add( gdc_subject_id )

                pdc_subject_id = ''

                if icdc_subject_id in icdc_subject_to_pdc_subject:
                    
                    pdc_subject_id = icdc_subject_to_pdc_subject[icdc_subject_id]

                    observed_pdc_subjects.add( pdc_subject_id )

                idc_subject_id = ''

                if icdc_subject_id in icdc_subject_to_idc_subject:
                    
                    idc_subject_id = icdc_subject_to_idc_subject[icdc_subject_id]

                    observed_idc_subjects.add( idc_subject_id )

                cds_subject_id = ''

                if icdc_subject_id in icdc_subject_to_cds_subject:
                    
                    cds_subject_id = icdc_subject_to_cds_subject[icdc_subject_id]

                    observed_cds_subjects.add( cds_subject_id )

                print( *[ icdc_subject_id, cds_subject_id, idc_subject_id, gdc_subject_id, pdc_subject_id ], sep='\t', file=OUT )

            with open( self.aux_files['cds_idc_gdc_pdc_subject_map'] ) as IN:
                
                header = next( IN )

                # The only remaining records we'll print should not include any ICDC subject IDs.

                icdc_subject_id = ''

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    # CDS_subject_id	IDC_subject_id	GDC_subject_id	PDC_subject_id

                    [ cds_subject_id, idc_subject_id, gdc_subject_id, pdc_subject_id ] = line.split( '\t' )

                    if cds_subject_id in observed_cds_subjects:
                        
                        if idc_subject_id != '' and idc_subject_id not in observed_idc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: CDS subject ID '{cds_subject_id}' matched to an ICDC subject ID, but corresponding IDC subject ID '{idc_subject_id}' did not. Aborting." )

                        if gdc_subject_id != '' and gdc_subject_id not in observed_gdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: CDS subject ID '{cds_subject_id}' matched to an ICDC subject ID, but corresponding GDC subject ID '{gdc_subject_id}' did not. Aborting." )

                        if pdc_subject_id != '' and pdc_subject_id not in observed_pdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: CDS subject ID '{cds_subject_id}' matched to an ICDC subject ID, but corresponding PDC subject ID '{pdc_subject_id}' did not. Aborting." )

                    elif idc_subject_id in observed_idc_subjects:
                        
                        if cds_subject_id != '' and cds_subject_id not in observed_cds_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: IDC subject ID '{idc_subject_id}' matched to an ICDC subject ID, but corresponding CDS subject ID '{cds_subject_id}' did not. Aborting." )

                        if gdc_subject_id != '' and gdc_subject_id not in observed_gdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: IDC subject ID '{idc_subject_id}' matched to an ICDC subject ID, but corresponding GDC subject ID '{gdc_subject_id}' did not. Aborting." )

                        if pdc_subject_id != '' and pdc_subject_id not in observed_pdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: IDC subject ID '{idc_subject_id}' matched to an ICDC subject ID, but corresponding PDC subject ID '{pdc_subject_id}' did not. Aborting." )

                    elif gdc_subject_id in observed_gdc_subjects:
                        
                        if cds_subject_id != '' and cds_subject_id not in observed_cds_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: GDC subject ID '{gdc_subject_id}' matched to an ICDC subject ID, but corresponding CDS subject ID '{cds_subject_id}' did not. Aborting." )

                        if idc_subject_id != '' and idc_subject_id not in observed_idc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: GDC subject ID '{gdc_subject_id}' matched to an ICDC subject ID, but corresponding IDC subject ID '{idc_subject_id}' did not. Aborting." )

                        if pdc_subject_id != '' and pdc_subject_id not in observed_pdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: GDC subject ID '{gdc_subject_id}' matched to an ICDC subject ID, but corresponding PDC subject ID '{pdc_subject_id}' did not. Aborting." )

                    elif pdc_subject_id in observed_pdc_subjects:
                        
                        if cds_subject_id != '' and cds_subject_id not in observed_cds_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: PDC subject ID '{pdc_subject_id}' matched to an ICDC subject ID, but corresponding CDS subject ID '{cds_subject_id}' did not. Aborting." )

                        if idc_subject_id != '' and idc_subject_id not in observed_idc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: PDC subject ID '{pdc_subject_id}' matched to an ICDC subject ID, but corresponding IDC subject ID '{idc_subject_id}' did not. Aborting." )

                        if gdc_subject_id != '' and gdc_subject_id not in observed_gdc_subjects:
                            
                            sys.exit( f"FATAL: Merge integrity error: PDC subject ID '{pdc_subject_id}' matched to an ICDC subject ID, but corresponding GDC subject ID '{gdc_subject_id}' did not. Aborting." )

                    else:
                        
                        print( *[ icdc_subject_id, idc_subject_id, gdc_subject_id, pdc_subject_id ], sep='\t', file=OUT )

    def __map_icdc_subject_ids_to_cda_targets( self ):
        
        with open( self.aux_files['all_merged_subject_ids_so_far'] ) as IN, open( self.aux_files['icdc_subject_id_to_cda_subject_id'], 'w' ) as OUT:
            
            colnames = next( IN ).rstrip( '\n' ).split( '\t' )

            print( *[ 'ICDC_subject_id', 'CDA_subject_id' ], sep='\t', file=OUT )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                icdc_id = record['ICDC_subject_id']

                if icdc_id != '':
                    
                    cda_id = record['GDC_subject_id']

                    if cda_id == '':
                        
                        cda_id = record['PDC_subject_id']

                        if cda_id == '':
                            
                            cda_id = record['IDC_subject_id']

                            if cda_id == '':
                                
                                sys.exit( "SOMETHING TECHNICALLY IMPOSSIBLE WENT HORRIBLY WRONG!" )

                    print( *[ icdc_id, cda_id ], sep='\t', file=OUT )

    def merge_into_existing_cda_data( self ):
        
        cda_file_subject_input_tsv = path.join( self.merged_input_dir, 'file_subject.tsv.gz' )

        cda_subject_input_tsv = path.join( self.merged_input_dir, 'subject.tsv.gz' )

        cda_subject_associated_project_input_tsv = path.join( self.merged_input_dir, 'subject_associated_project.tsv.gz' )

        cda_subject_identifier_input_tsv = path.join( self.merged_input_dir, 'subject_identifier.tsv.gz' )

        cda_subject_researchsubject_input_tsv = path.join( self.merged_input_dir, 'subject_researchsubject.tsv.gz' )


        cross_dc_subject_id_map = self.aux_files['icdc_subject_id_to_cda_subject_id']


        icdc_file_subject_input_tsv = path.join( self.icdc_cda_dir, 'file_subject.tsv' )

        icdc_subject_input_tsv = path.join( self.icdc_cda_dir, 'subject.tsv' )

        icdc_subject_associated_project_input_tsv = self.input_files['subject_associated_project']

        icdc_subject_identifier_input_tsv = self.input_files['subject_identifier']

        icdc_subject_researchsubject_input_tsv = path.join( self.icdc_cda_dir, 'subject_researchsubject.tsv' )


        file_subject_output_tsv = path.join( self.merged_output_dir, 'file_subject.tsv.gz' )

        subject_output_tsv = path.join( self.merged_output_dir, 'subject.tsv.gz' )

        subject_associated_project_output_tsv = path.join( self.merged_output_dir, 'subject_associated_project.tsv.gz' )

        subject_identifier_output_tsv = path.join( self.merged_output_dir, 'subject_identifier.tsv.gz' )

        subject_researchsubject_output_tsv = path.join( self.merged_output_dir, 'subject_researchsubject.tsv.gz' )


        subject_merged_log = path.join( self.merge_log_dir, 'ICDC_subject_IDs_absorbed_into_corresponding_CDS_IDC_GDC_PDC_merged_subject_IDs.tsv' )

        null_sub_log = path.join( self.merge_log_dir, 'ICDC_values_replacing_CDS_IDC_GDC_PDC_merged_nulls_by_subject_column_name.tsv' )

        clashes_ignored_log = path.join( self.merge_log_dir, 'ICDC_values_unused_but_different_from_CDS_IDC_GDC_PDC_merged_values_by_subject_column_name_and_dataset.tsv' )


        # First, identify all pairs of ICDC & CDS+IDC+GDC+PDC subject records to be merged.

        cda_subject_id_to_icdc_subject_id = map_columns_one_to_one( cross_dc_subject_id_map, 'CDA_subject_id', 'ICDC_subject_id' )

        icdc_subject_id_to_cda_subject_id = map_columns_one_to_one( cross_dc_subject_id_map, 'ICDC_subject_id', 'CDA_subject_id' )

        # Next, merge the subject_* association tables.

        print( 'Merging subject_associated_project ...', end='', file=sys.stderr )

        input_record_count = 0
        added_record_count = 0

        with gzip.open( subject_associated_project_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with gzip.open( cda_subject_associated_project_input_tsv, 'rt' ) as IN:
                
                header = next( IN ).rstrip( '\n' )

                print( header, file=OUT )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    print( line, file=OUT )

                    input_record_count = input_record_count + 1

            # Port over records from ICDC, translating subject_id where appropriate.

            with open( icdc_subject_associated_project_input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    if record['subject_id'] in icdc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = icdc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                    added_record_count = added_record_count + 1

        sort_file_with_header( subject_associated_project_output_tsv, gzipped=True )

        print( f"done. Copied {input_record_count} existing records to new TSV and added {added_record_count} new records.", file=sys.stderr )

        print( 'Merging subject_identifier ...', end='', file=sys.stderr )

        input_record_count = 0
        added_record_count = 0

        with gzip.open( subject_identifier_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with gzip.open( cda_subject_identifier_input_tsv, 'rt' ) as IN:
                
                header = next( IN ).rstrip( '\n' )

                print( header, file=OUT )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    print( line, file=OUT )

                    input_record_count = input_record_count + 1

            # Port over records from ICDC, translating subject_id where appropriate.

            with open( icdc_subject_identifier_input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    if record['subject_id'] in icdc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = icdc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                    added_record_count = added_record_count + 1

        sort_file_with_header( subject_identifier_output_tsv, gzipped=True )

        print( f"done. Copied {input_record_count} existing records to new TSV and added {added_record_count} new records.", file=sys.stderr )

        print( 'Merging subject_researchsubject ...', end='', file=sys.stderr )

        input_record_count = 0
        added_record_count = 0

        with gzip.open( subject_researchsubject_output_tsv, 'wt' ) as OUT:
            
            # Copy over the existing records.

            with gzip.open( cda_subject_researchsubject_input_tsv, 'rt' ) as IN:
                
                header = next( IN ).rstrip( '\n' )

                print( header, file=OUT )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    print( line, file=OUT )

                    input_record_count = input_record_count + 1

            # Port over records from ICDC, translating subject_id where appropriate.

            with open( icdc_subject_researchsubject_input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    if record['subject_id'] in icdc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = icdc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                    added_record_count = added_record_count + 1

        sort_file_with_header( subject_researchsubject_output_tsv, gzipped=True )

        print( f"done. Copied {input_record_count} existing records to new TSV and added {added_record_count} new records.", file=sys.stderr )

        # Next merge file_subject.tsv, whose Subject IDs need updating for any ICDC Subjects whose
        # CDA IDs have been superseded by those of their corresponding CDS+IDC+GDC+PDC-merged Subjects.

        print( 'Merging file_subject ...', end='', file=sys.stderr )

        input_record_count = 0
        added_record_count = 0

        with gzip.open( file_subject_output_tsv, 'wt' ) as OUT:
            
            # Copy over the records from CDS+IDC+GDC+PDC.

            with gzip.open( cda_file_subject_input_tsv, 'rt' ) as IN:
                
                header = next( IN ).rstrip( '\n' )

                print( header, file=OUT )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    print( line, file=OUT )

                    input_record_count = input_record_count + 1

            # Port over records from ICDC, translating subject_id where appropriate.

            with open( icdc_file_subject_input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    if record['subject_id'] in icdc_subject_id_to_cda_subject_id:
                        
                        record['subject_id'] = icdc_subject_id_to_cda_subject_id[record['subject_id']]

                    print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                    added_record_count = added_record_count + 1

        print( f"done. Copied {input_record_count} existing records to new TSV and added {added_record_count} new records.", file=sys.stderr )

        # Now merge subject.tsv, log merges, and keep track of any data conflicts encountered.

        print( 'Merging subject ...', end='', file=sys.stderr )

        input_record_count = 0
        merged_record_count = 0
        added_record_count = 0

        # Preload records from ICDC.

        icdc_subject_preload = dict()

        with open( icdc_subject_input_tsv ) as IN:
            
            colnames = next( IN ).rstrip( '\n' ).split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                record = dict( zip( colnames, line.split( '\t' ) ) )

                if record['id'] in icdc_subject_id_to_cda_subject_id:
                    
                    record['id'] = icdc_subject_id_to_cda_subject_id[record['id']]

                icdc_subject_preload[record['id']] = record

        # Translate/upgrade CDS+IDC+GDC+PDC-merged records and add in any unmapped ICDC records as well.

        merged_from_icdc = set()

        clashes_ignored = dict()

        null_subs_made = dict()

        with gzip.open( subject_output_tsv, 'wt' ) as OUT:
            
            # Use the CDS+IDC+GDC+PDC-merged file as the first source for output records, updating with ICDC subject data where possible.

            with gzip.open( cda_subject_input_tsv, 'rt' ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                print( *colnames, sep='\t', file=OUT )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    current_id = record['id']

                    if current_id in icdc_subject_preload:
                        
                        merged_from_icdc.add( cda_subject_id_to_icdc_subject_id[current_id] )

                        for colname in colnames:
                            
                            # Replace any values that are null in the existing CDS+IDC+GDC+PDC-merged data with corresponding non-null values from ICDC.

                            if record[colname] is None or record[colname] == '':
                                
                                if icdc_subject_preload[current_id][colname] is not None and icdc_subject_preload[current_id][colname] != '':
                                    
                                    record[colname] = icdc_subject_preload[current_id][colname]

                                    if colname not in null_subs_made:
                                        
                                        null_subs_made[colname] = dict()

                                    if icdc_subject_preload[current_id][colname] not in null_subs_made[colname]:
                                        
                                        null_subs_made[colname][icdc_subject_preload[current_id][colname]] = 1

                                    else:
                                        
                                        null_subs_made[colname][icdc_subject_preload[current_id][colname]] = null_subs_made[colname][icdc_subject_preload[current_id][colname]] + 1

                            elif record[colname] is not None and record[colname] != '':
                                
                                cda_value = record[colname]

                                if icdc_subject_preload[current_id][colname] is None or icdc_subject_preload[current_id][colname] == '':
                                    
                                    icdc_value = ''

                                else:
                                    
                                    icdc_value = icdc_subject_preload[current_id][colname]

                                if icdc_value != cda_value:
                                    
                                    if colname not in clashes_ignored:
                                        
                                        clashes_ignored[colname] = dict()

                                    if icdc_value not in clashes_ignored[colname]:
                                        
                                        clashes_ignored[colname][icdc_value] = dict()

                                    if cda_value not in clashes_ignored[colname][icdc_value]:
                                        
                                        clashes_ignored[colname][icdc_value][cda_value] = 1

                                    else:
                                        
                                        clashes_ignored[colname][icdc_value][cda_value] = clashes_ignored[colname][icdc_value][cda_value] + 1

                    print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                    input_record_count = input_record_count + 1

            merged_record_count = len( merged_from_icdc )

            # Use the ICDC file as the next source for output records, ignoring any ICDC subject data already scanned.

            with open( icdc_subject_input_tsv ) as IN:
                
                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split( '\t' ) ) )

                    current_id = record['id']

                    if current_id not in merged_from_icdc:
                        
                        print( *[ record[colname] for colname in colnames ], sep='\t', file=OUT )

                        added_record_count = added_record_count + 1

        sort_file_with_header( subject_output_tsv, gzipped=True )

        print( f"done. Copied {input_record_count} existing records to new TSV, of which {merged_record_count} were updated with new data merged in from ICDC, and added {added_record_count} entirely new records.", file=sys.stderr )

        # Log CDS+IDC+GDC+PDC-merged null values that were replaced with non-null data from ICDC.

        with open( null_sub_log, 'w' ) as LOG:
            
            print( *[ 'subject_column', 'ICDC_value', 'substitution_count' ], sep='\t', file=LOG )

            for colname in sorted( null_subs_made ):
                
                for value in sorted( null_subs_made[colname] ):
                    
                    print( *[ colname, value, null_subs_made[colname][value] ], sep='\t', file=LOG )

        # Log non-null ICDC values that didn't match existing CDS+IDC+GDC+PDC-merged CDA metadata values (these were NOT used to update CDA metadata).

        with open( clashes_ignored_log, 'w' ) as LOG:
            
            print( *[ 'subject_column', 'ICDC_value', 'CDS_IDC_GDC_PDC_merged_value', 'match_count' ], sep='\t', file=LOG )

            for colname in sorted( clashes_ignored ):
                
                for icdc_value in sorted( clashes_ignored[colname] ):
                    
                    for cda_value in sorted( clashes_ignored[colname][icdc_value] ):
                        
                        print( *[ colname, icdc_value, cda_value, clashes_ignored[colname][icdc_value][cda_value] ], sep='\t', file=LOG )

        # Log before- and after-IDs for ICDC subject records that got merged with (subsumed by) CDS+IDC+GDC+PDC-merged subject records.

        with open( subject_merged_log, 'w' ) as LOG:
            
            print( *[ 'ICDC_subject_id', 'CDS_IDC_GDC_PDC_merged_subject_id' ], sep='\t', file=LOG )

            for icdc_subject_id in sorted( merged_from_icdc ):
                
                cda_subject_id = icdc_subject_id_to_cda_subject_id[icdc_subject_id]

                print( *[ icdc_subject_id, cda_subject_id ], sep='\t', file=LOG )

        # Concatenate the rest of everything. We're only merging records at the subject level at this time.

        for file_basename in sorted( listdir( self.merged_input_dir ) ):
            
            # We already covered subject_* and file_subject. Also don't parse anything not ending in '.tsv' and ignore any downstream-generated files like integer-alias maps and data_source tables.

            if re.search( r'^subject', file_basename ) is None and re.search( r'^file_subject\.', file_basename ) is None and \
                re.search( r'\.tsv\.gz$', file_basename ) is not None and \
                re.search( r'data_source', file_basename ) is None and re.search( r'integer_aliases', file_basename ) is None:
                
                print( f"Concatenating {file_basename} data ...", end='', file=sys.stderr )

                cda_file_full_path = path.join( self.merged_input_dir, file_basename )

                icdc_file_full_path = path.join( self.icdc_cda_dir, re.sub( r'\.gz$', r'', file_basename ) )

                dest_file_full_path = path.join( self.merged_output_dir, file_basename )

                input_record_count = 0
                added_record_count = 0

                with gzip.open( dest_file_full_path, 'wt' ) as OUT:
                    
                    with gzip.open( cda_file_full_path, 'rt' ) as IN:
                        
                        output_colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                        print( *output_colnames, sep='\t', file=OUT )

                        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                            
                            print( line, file=OUT )

                            input_record_count = input_record_count + 1

                    if path.isfile( icdc_file_full_path ):
                        
                        with open( icdc_file_full_path ) as IN:
                            
                            current_colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                                
                                record = dict( zip( current_colnames, line.split( '\t' ) ) )

                                print( *[ record[colname] for colname in output_colnames ], sep='\t', file=OUT )

                                added_record_count = added_record_count + 1

                # File metadata is now too big to sort.

                if re.search( r'^file', file_basename ) is None:
                    
                    sort_file_with_header( dest_file_full_path, gzipped=True )

                print( f"done. Copied {input_record_count} existing records to new TSV and added {added_record_count} new records.", file=sys.stderr )



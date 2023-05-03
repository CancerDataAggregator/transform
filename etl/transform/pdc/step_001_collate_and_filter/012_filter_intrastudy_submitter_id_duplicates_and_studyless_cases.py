#!/usr/bin/env python -u

from os import makedirs, path, rename

class PDC_data_cleaner:
    
    def __init__( self ):
        
        # Main data root.

        self.processing_root = 'merged_data/pdc'

        if not path.exists( self.processing_root ):
            
            makedirs( self.processing_root )

        # Storage area for removal logs.

        self.log_dir = path.join( self.processing_root, '__filtration_logs' )

        if not path.exists( self.log_dir ):
            
            makedirs( self.log_dir )

        # Temp file to use for TSV filtration.

        self.temp_file = path.join( self.processing_root, 'temp' )

        # Name of primary ID field for each entity type.

        self.id_colname = {
            
            'Aliquot': 'aliquot_id',
            'Case': 'case_id',
            'Demographic': 'demographic_id',
            'Diagnosis': 'diagnosis_id',
            'File': 'file_id',
            'Sample': 'sample_id'
        }

        # Name of submitter_id field for each top-level entity type.

        self.submitter_id_colname = {
            
            'Aliquot': 'aliquot_submitter_id',
            'Case': 'case_submitter_id',
            'Demographic': 'demographic_submitter_id',
            'Diagnosis': 'diagnosis_submitter_id',
            'Sample': 'sample_submitter_id'
        }

        # Association map between each entity type and the entity type capable of orphaning it.

        self.orphaned_if_missing = {
            
            'Aliquot': path.join( self.processing_root, 'Sample', 'Sample.aliquot_id.tsv' ),
            'Case': path.join( self.processing_root, 'Case', 'Case.demographic_id.tsv' ),
            'Demographic': path.join( self.processing_root, 'Case', 'Case.demographic_id.tsv' ),
            'Diagnosis': path.join( self.processing_root, 'Case', 'Case.diagnosis_id.tsv' ),
            'File': path.join( self.processing_root, 'File', 'File.case_id.tsv' ),
            'Sample': path.join( self.processing_root, 'Sample', 'Sample.case_id.tsv' )
        }

        # Relationships between each entity type and other dependent entity types (types the main type can orphan if removed).

        self.dependent_types = {
            
            'Aliquot': [],
            'Case': [ 'Demographic', 'Diagnosis', 'File', 'Sample' ],
            'Demographic': [ 'Case' ],
            'Diagnosis': [],
            'File': [],
            'Sample': [ 'Aliquot' ]
        }

        # Lists of dependent entities to recursively remove following the top-level
        # duplication-removal pass.

        self.ids_to_remove = {
            
            'Aliquot': dict(),
            'Case': dict(),
            'Demographic': dict(),
            'Diagnosis': dict(),
            'Sample': dict()
        }

        # Association maps between each entity type and all other non-dependent entity types (types the main type cannot orphan if removed).

        self.independent_type_association_tsvs = {
           
            'Aliquot': [
                
                path.join( self.processing_root, 'Aliquot', 'Aliquot.case_id.tsv' ),
                path.join( self.processing_root, 'Aliquot', 'Aliquot.project_id.tsv' ),
                path.join( self.processing_root, 'Aliquot', 'Aliquot.study_id.tsv' ),
                path.join( self.processing_root, 'File', 'File.aliquot_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.aliquot_id.tsv' )
            ],
            'Case': [
                
                path.join( self.processing_root, 'Aliquot', 'Aliquot.case_id.tsv' ),
                path.join( self.processing_root, 'Case', 'Case.gdc_id.tsv' ),
                path.join( self.processing_root, 'Case', 'Case.project_id.tsv' ),
                # This next one has a redundant copy in Sample/Sample.case_id.tsv, which is
                # the one we use for filtration dependency checks. So we can leave
                # this one here where it won't trigger any cascade checking, because
                # that's taken care of elsewhere.
                path.join( self.processing_root, 'Case', 'Case.sample_id.tsv' ),
                path.join( self.processing_root, 'Case', 'Case.study_id.tsv' ),
                path.join( self.processing_root, 'File', 'File.case_id.tsv' )
            ],
            'Demographic': [
                
                path.join( self.processing_root, 'Demographic', 'Demographic.project_id.tsv' ),
                path.join( self.processing_root, 'Demographic', 'Demographic.study_id.tsv' )
            ],
            'Diagnosis': [
                
                path.join( self.processing_root, 'Case', 'Case.diagnosis_id.tsv' ),
                path.join( self.processing_root, 'Diagnosis', 'Diagnosis.project_id.tsv' ),
                path.join( self.processing_root, 'Diagnosis', 'Diagnosis.study_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.diagnosis_id.tsv' )
            ],
            'File': [
                
                path.join( self.processing_root, 'File', 'File.aliquot_id.tsv' ),
                path.join( self.processing_root, 'File', 'File.instrument.tsv' ),
                path.join( self.processing_root, 'File', 'File.study_id.tsv' )
            ],
            'Sample': [
                
                path.join( self.processing_root, 'Case', 'Case.sample_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.case_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.diagnosis_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.project_id.tsv' ),
                path.join( self.processing_root, 'Sample', 'Sample.study_id.tsv' )
            ]
        }

    def __map_columns_one_to_one( self, input_file, from_field, to_field ):
        
        return_map = dict()

        with open( input_file ) as IN:
            
            column_names = next(IN).rstrip('\n').split('\t')

            if from_field not in column_names or to_field not in column_names:
                
                sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( column_names, line.split('\t') ) )

                return_map[record[from_field]] = record[to_field]

        return return_map

    def __map_columns_one_to_many( self, input_file, from_field, to_field ):
        
        return_map = dict()

        with open( input_file ) as IN:
            
            column_names = next(IN).rstrip('\n').split('\t')

            if from_field not in column_names or to_field not in column_names:
                
                sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( column_names, line.split('\t') ) )

                if record[from_field] not in return_map:
                    
                    return_map[record[from_field]] = set()

                return_map[record[from_field]].add( record[to_field] )

        return return_map

    def __remove_list( self, entity_type, removal_list, provenance_list ):
        
        # Update the provenance chain.

        provenance_list.append( entity_type )

        # First, remove the target records from their main entity table and log deletions.

        input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.tsv" )

        id_list_filtration_log = path.join( self.log_dir, '_'.join( provenance_list ) + f".orphans.removed_ids.txt" )

        with open( input_tsv ) as IN, open( self.temp_file, 'w' ) as OUT, open( id_list_filtration_log, 'a' ) as LOG:
            
            header = next(IN).rstrip('\n')

            print( header, file=OUT )

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                check_id = record[self.id_colname[entity_type]]

                if check_id in removal_list:
                    
                    print( check_id, file=LOG )

                else:
                    
                    print( line, file=OUT )

        rename( self.temp_file, input_tsv )

        # Update non-orphanable map files to remove filtered IDs.

        for map_file in self.independent_type_association_tsvs[entity_type]:
            
            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    record_id = record[self.id_colname[entity_type]]

                    if record_id not in removal_list:
                        
                        print( line, file=OUT )

            rename( self.temp_file, map_file )

    def __recursive_remove( self, entity_type, entity_id, provenance_list ):
        
        # Update the provenance chain.

        provenance_list.append( entity_type )

        # First, remove the target record from its main entity table and log deletions.

        input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.tsv" )

        recursive_id_filtration_log = path.join( self.log_dir, '_'.join( provenance_list ) + f".orphans.removed_ids.txt" )

        with open( input_tsv ) as IN, open( self.temp_file, 'w' ) as OUT, open( recursive_id_filtration_log, 'a' ) as LOG:
            
            header = next(IN).rstrip('\n')

            print( header, file=OUT )

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                check_id = record[self.id_colname[entity_type]]

                if check_id == entity_id:
                    
                    print( entity_id, file=LOG )

                else:
                    
                    print( line, file=OUT )

        rename( self.temp_file, input_tsv )

        # For each type orphanable by {entity_type},

        for dependent_type in self.dependent_types[entity_type]:
            
            # Delete records for the target {entity_type} ID from the map file between
            # {entity_type} and {dependent_type}. Cache IDs of possible {dependent_type}
            # orphans as you go, and track all {dependent_type} IDs still present in records
            # that don't refer to the target {entity_type} ID.

            map_file = self.orphaned_if_missing[dependent_type]

            possibly_orphaned_ids = set()

            remaining_dependent_type_ids = set()

            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    check_id = record[self.id_colname[entity_type]]

                    if check_id == entity_id:
                        
                        possibly_orphaned_ids.add(record[self.id_colname[dependent_type]])

                    else:
                        
                        remaining_dependent_type_ids.add(record[self.id_colname[dependent_type]])

                        print( line, file=OUT )

            rename( self.temp_file, map_file )

            # For each possibly orphaned {dependent_type} ID, see if it was in fact orphaned.

            for dependent_entity_id in sorted( possibly_orphaned_ids ):
                
                if dependent_entity_id not in remaining_dependent_type_ids:
                    
                    # If so, remove the orphaned record and recursively
                    # remove all other records orphaned by the deletion.

                    self.__recursive_remove( dependent_type, dependent_entity_id, list( provenance_list ) )

        # Update non-orphanable map files to remove filtered IDs.

        for map_file in self.independent_type_association_tsvs[entity_type]:
            
            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    record_id = record[self.id_colname[entity_type]]

                    if record_id != entity_id:
                        
                        print( line, file=OUT )

            rename( self.temp_file, map_file )

    def remove_studyless_cases( self ):
        
        entity_type = 'Case'

        # Load the Case->Study map.

        case_in_study_input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.study_id.tsv" )

        case_in_study = self.__map_columns_one_to_one( case_in_study_input_tsv, self.id_colname[entity_type], 'study_id' )

        # Scan Case.tsv and flag IDs that aren't in the loaded map.

        input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.tsv" )

        case_id_filtration_log = path.join( self.log_dir, f"{entity_type}.not_in_Study.removed_ids.txt" )

        # Strip out offending records and log removals.

        removed_ids = set()

        with open( input_tsv ) as IN, open( self.temp_file, 'w' ) as OUT, open( case_id_filtration_log, 'w' ) as LOG:
            
            header = next(IN).rstrip('\n')

            print( header, file=OUT )

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                record_id = record[self.id_colname[entity_type]]

                if record_id not in case_in_study:
                    
                    print( record_id, file=LOG )

                    removed_ids.add( record_id )

                else:
                    
                    print( line, file=OUT )

        rename( self.temp_file, input_tsv )

        # For each type orphanable by Case,

        for dependent_type in self.dependent_types[entity_type]:
            
            # Delete records for removed Case IDs from the map file between
            # Case and {dependent_type}. Cache IDs of possible {dependent_type}
            # orphans as you go, and track all {dependent_type} IDs still present in records
            # that don't refer to the target Case ID.

            map_file = self.orphaned_if_missing[dependent_type]

            possibly_orphaned_ids = set()

            remaining_dependent_type_ids = set()

            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    check_id = record[self.id_colname[entity_type]]

                    if check_id in removed_ids:
                        
                        possibly_orphaned_ids.add(record[self.id_colname[dependent_type]])

                    else:
                        
                        remaining_dependent_type_ids.add(record[self.id_colname[dependent_type]])

                        print( line, file=OUT )

            rename( self.temp_file, map_file )

            # For each possibly orphaned {dependent_type} ID, see if it was in fact orphaned.

            for dependent_entity_id in sorted( possibly_orphaned_ids ):
                
                if dependent_entity_id not in remaining_dependent_type_ids:
                    
                    # If so, flag the orphan for recursive removal after we're done with the
                    # top-level cleaning pass.

                    if dependent_type not in self.ids_to_remove[entity_type]:
                        
                        self.ids_to_remove[entity_type][dependent_type] = set()

                    self.ids_to_remove[entity_type][dependent_type].add( dependent_entity_id )

        # Update non-orphanable map files to remove filtered IDs.

        for map_file in self.independent_type_association_tsvs[entity_type]:
            
            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    record_id = record[self.id_colname[entity_type]]

                    if record_id not in removed_ids:
                        
                        print( line, file=OUT )

            rename( self.temp_file, map_file )

    def remove_study_and_submitter_id_pair_duplicates( self, entity_type ):
        
        # Load the {entity_type}<->study_id map.

        entity_in_study_input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.study_id.tsv" )

        entity_in_study = self.__map_columns_one_to_many( entity_in_study_input_tsv, self.id_colname[entity_type], 'study_id' )

        # Scan {entity_type}.tsv and count occurrences of study_id+submitter_id pairs.

        input_tsv = path.join( self.processing_root, entity_type, f"{entity_type}.tsv" )

        multiplicity = dict()

        with open( input_tsv ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                record_id = record[self.id_colname[entity_type]]

                submitter_id = record[self.submitter_id_colname[entity_type]]

                if record_id in entity_in_study:
                    
                    for study_id in sorted( entity_in_study[record_id] ):

                        if study_id not in multiplicity:
                            
                            multiplicity[study_id] = dict()

                        if submitter_id not in multiplicity[study_id]:
                            
                            multiplicity[study_id][submitter_id] = 1

                        else:
                            
                            multiplicity[study_id][submitter_id] = multiplicity[study_id][submitter_id] + 1

        duplicate_id_filtration_log = path.join( self.log_dir, f"{entity_type}.same_study_{self.submitter_id_colname[entity_type]}_duplicates.removed_ids.txt" )

        # Strip out offending records and log removals.

        removed_ids = set()

        with open( input_tsv ) as IN, open( self.temp_file, 'w' ) as OUT, open( duplicate_id_filtration_log, 'w' ) as LOG:
            
            header = next(IN).rstrip('\n')

            print( header, file=OUT )

            colnames = header.split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                record_id = record[self.id_colname[entity_type]]

                submitter_id = record[self.submitter_id_colname[entity_type]]

                printed = False

                if record_id in entity_in_study:
                    
                    for study_id in sorted( entity_in_study[record_id] ):
                        
                        if multiplicity[study_id][submitter_id] > 1:
                            
                            if not printed:
                                
                                print( record_id, file=LOG )

                                removed_ids.add( record_id )

                                printed = True

                if not printed:
                    
                    print( line, file=OUT )

        rename( self.temp_file, input_tsv )

        # For each type orphanable by {entity_type},

        for dependent_type in self.dependent_types[entity_type]:
            
            # Delete records for removed {entity_type} IDs from the map file between
            # {entity_type} and {dependent_type}. Cache IDs of possible {dependent_type}
            # orphans as you go, and track all {dependent_type} IDs still present in records
            # that don't refer to the target {entity_type} ID.

            map_file = self.orphaned_if_missing[dependent_type]

            possibly_orphaned_ids = set()

            remaining_dependent_type_ids = set()

            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    check_id = record[self.id_colname[entity_type]]

                    if check_id in removed_ids:
                        
                        possibly_orphaned_ids.add(record[self.id_colname[dependent_type]])

                    else:
                        
                        remaining_dependent_type_ids.add(record[self.id_colname[dependent_type]])

                        print( line, file=OUT )

            rename( self.temp_file, map_file )

            # For each possibly orphaned {dependent_type} ID, see if it was in fact orphaned.

            for dependent_entity_id in sorted( possibly_orphaned_ids ):
                
                if dependent_entity_id not in remaining_dependent_type_ids:
                    
                    # If so, flag the orphan for recursive removal after we're done with the
                    # top-level cleaning pass.

                    if dependent_type not in self.ids_to_remove[entity_type]:
                        
                        self.ids_to_remove[entity_type][dependent_type] = set()

                    self.ids_to_remove[entity_type][dependent_type].add( dependent_entity_id )

        # Update non-orphanable map files to remove filtered IDs.

        for map_file in self.independent_type_association_tsvs[entity_type]:
            
            with open( map_file ) as IN, open( self.temp_file, 'w' ) as OUT:
                
                header = next(IN).rstrip('\n')

                print( header, file=OUT )

                colnames = header.split('\t')

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    record = dict( zip( colnames, line.split('\t') ) )

                    record_id = record[self.id_colname[entity_type]]

                    if record_id not in removed_ids:
                        
                        print( line, file=OUT )

            rename( self.temp_file, map_file )

    def remove_orphans( self, entity_type ):
        
        for dependent_type in self.ids_to_remove[entity_type]:
            
            # If there are no other types that can be orphaned by the removal of
            # {dependent_type} records, we don't need to recurse.

            if len( self.dependent_types[dependent_type] ) == 0:
                
                self.__remove_list( dependent_type, self.ids_to_remove[entity_type][dependent_type], [entity_type] )

            else:
                
                for entity_id in self.ids_to_remove[entity_type][dependent_type]:
                    
                    self.__recursive_remove( dependent_type, entity_id, [entity_type] )

        self.ids_to_remove[entity_type] = dict()

def main():
    
    cleaner = PDC_data_cleaner()

    cleaner.remove_studyless_cases()

    cleaner.remove_orphans( 'Case' )

    for entity_type in [ 'Diagnosis', 'Aliquot', 'Sample', 'Demographic', 'Case' ]:
        
        cleaner.remove_study_and_submitter_id_pair_duplicates( entity_type )

    for entity_type in [ 'Diagnosis', 'Aliquot', 'Sample', 'Demographic', 'Case' ]:
        
        cleaner.remove_orphans( entity_type )

if __name__ == "__main__":
    
    main()



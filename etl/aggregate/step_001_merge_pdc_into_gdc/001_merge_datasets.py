#!/usr/bin/env python -u

import re
import sys

from os import listdir, makedirs, path, rename

# SUBROUTINES

def map_columns_one_to_one( input_file, from_field, to_field ):
    
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

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            return_map[current_from] = current_to

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field ):
    
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

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add(current_to)

    return return_map

def load_tsv_as_dict( input_file ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            # Assumes first column is a unique ID column; if this
            # doesn't end up being true, only the last record will
            # be stored for any repeated ID.

            result[values[0]] = dict( zip( colnames, values ) )

    return result

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

gdc_input_dir = 'gdc_tables'

gdc_file_subject_input_tsv = path.join( gdc_input_dir, 'file_subject.tsv' )

gdc_subject_input_tsv = path.join( gdc_input_dir, 'subject.tsv' )

gdc_subject_associated_project_input_tsv = path.join( gdc_input_dir, 'subject_associated_project.tsv' )

gdc_subject_identifier_input_tsv = path.join( gdc_input_dir, 'subject_identifier.tsv' )

gdc_subject_researchsubject_input_tsv = path.join( gdc_input_dir, 'subject_researchsubject.tsv' )

pdc_input_dir = 'pdc_tables'

cross_dc_subject_id_map = path.join( pdc_input_dir, 'subject_to_GDC_subject_if_latter_exists.tsv' )

pdc_file_subject_input_tsv = path.join( pdc_input_dir, 'file_subject.tsv' )

pdc_subject_input_tsv = path.join( pdc_input_dir, 'subject.tsv' )

pdc_subject_associated_project_input_tsv = path.join( pdc_input_dir, 'subject_associated_project.tsv' )

pdc_subject_identifier_input_tsv = path.join( pdc_input_dir, 'subject_identifier.tsv' )

pdc_subject_researchsubject_input_tsv = path.join( pdc_input_dir, 'subject_researchsubject.tsv' )

output_dir = 'merged_gdc_and_pdc_tables'

file_subject_output_tsv = path.join( output_dir, 'file_subject.tsv' )

subject_output_tsv = path.join( output_dir, 'subject.tsv' )

subject_associated_project_output_tsv = path.join( output_dir, 'subject_associated_project.tsv' )

subject_identifier_output_tsv = path.join( output_dir, 'subject_identifier.tsv' )

subject_researchsubject_output_tsv = path.join( output_dir, 'subject_researchsubject.tsv' )

log_dir = path.join( output_dir, '__merge_logs' )

subject_merged_log = path.join( log_dir, 'PDC_subject_IDs_absorbed_into_corresponding_GDC_subject_IDs.tsv' )

null_sub_log = path.join( log_dir, 'PDC_values_replacing_GDC_nulls_by_subject_column_name.tsv' )

clashes_ignored_log = path.join( log_dir, 'PDC_values_unused_but_different_from_GDC_values_by_subject_column_name_and_DC.tsv' )

# EXECUTION

for target_dir in [ output_dir, log_dir ]:
    
    if not path.exists( target_dir ):
        
        makedirs( target_dir )

# First, identify all pairs of GDC & PDC subject records that are valid merge candidates.

potential_gdc_subject_id_to_pdc_subject_id = map_columns_one_to_one( cross_dc_subject_id_map, 'gdc_subject_id', 'pdc_subject_id' )

pdc_subject_id_to_gdc_subject_id = dict()

with open( gdc_subject_input_tsv ) as IN:
    
    colnames = next(IN).rstrip('\n').split('\t')

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        record = dict( zip( colnames, line.split('\t') ) )

        if record['id'] in potential_gdc_subject_id_to_pdc_subject_id:
            
            pdc_subject_id_to_gdc_subject_id[potential_gdc_subject_id_to_pdc_subject_id[record['id']]] = record['id']

# Next, merge the subject_* association tables.

with open( subject_associated_project_output_tsv, 'w' ) as OUT:
    
    # Copy over the records from GDC.

    with open( gdc_subject_associated_project_input_tsv ) as IN:
        
        header = next(IN).rstrip('\n')

        print( header, end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            print( line, end='\n', file=OUT )

    # Port over records from PDC, translating subject_id where appropriate.

    with open( pdc_subject_associated_project_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            if record['subject_id'] in pdc_subject_id_to_gdc_subject_id:
                
                record['subject_id'] = pdc_subject_id_to_gdc_subject_id[record['subject_id']]

            print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

sort_file_with_header( subject_associated_project_output_tsv )

with open( subject_identifier_output_tsv, 'w' ) as OUT:
    
    # Copy over the records from GDC.

    with open( gdc_subject_identifier_input_tsv ) as IN:
        
        header = next(IN).rstrip('\n')

        print( header, end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            print( line, end='\n', file=OUT )

    # Port over records from PDC, translating subject_id where appropriate.

    with open( pdc_subject_identifier_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            if record['subject_id'] in pdc_subject_id_to_gdc_subject_id:
                
                record['subject_id'] = pdc_subject_id_to_gdc_subject_id[record['subject_id']]

            print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

sort_file_with_header( subject_identifier_output_tsv )

with open( subject_researchsubject_output_tsv, 'w' ) as OUT:
    
    # Copy over the records from GDC.

    with open( gdc_subject_researchsubject_input_tsv ) as IN:
        
        header = next(IN).rstrip('\n')

        print( header, end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            print( line, end='\n', file=OUT )

    # Port over records from PDC, translating subject_id where appropriate.

    with open( pdc_subject_researchsubject_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            if record['subject_id'] in pdc_subject_id_to_gdc_subject_id:
                
                record['subject_id'] = pdc_subject_id_to_gdc_subject_id[record['subject_id']]

            print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

sort_file_with_header( subject_researchsubject_output_tsv )

# Next merge file_subject.tsv, whose Subject IDs need updating for any PDC Subjects whose
# CDA IDs have been superseded by those of their corresponding GDC Subjects.

with open( file_subject_output_tsv, 'w' ) as OUT:
    
    # Copy over the records from GDC.

    with open( gdc_file_subject_input_tsv ) as IN:
        
        header = next(IN).rstrip('\n')

        print( header, end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            print( line, end='\n', file=OUT )

    # Port over records from PDC, translating subject_id where appropriate.

    with open( pdc_file_subject_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            if record['subject_id'] in pdc_subject_id_to_gdc_subject_id:
                
                record['subject_id'] = pdc_subject_id_to_gdc_subject_id[record['subject_id']]

            print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

sort_file_with_header( file_subject_output_tsv )

# Now merge subject.tsv, log merges, and keep track of any data conflicts encountered.

# Preload records from PDC.

pdc_subject_preload = dict()

with open( pdc_subject_input_tsv ) as IN:
    
    colnames = next(IN).rstrip('\n').split('\t')

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        record = dict( zip( colnames, line.split('\t') ) )

        if record['id'] in pdc_subject_id_to_gdc_subject_id:
            
            record['id'] = pdc_subject_id_to_gdc_subject_id[record['id']]

        pdc_subject_preload[record['id']] = record

# Translate/upgrade GDC records and add in any unmapped PDC records as well.

merged_from_pdc = set()

clashes_ignored = dict()

null_subs_made = dict()

with open( subject_output_tsv, 'w' ) as OUT:
    
    # Use the GDC file as the first source for output records, updating with PDC subject data where possible.

    with open( gdc_subject_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        print( *colnames, sep='\t', end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            current_id = record['id']

            if current_id in pdc_subject_preload:
                
                merged_from_pdc.add( potential_gdc_subject_id_to_pdc_subject_id[current_id] )

                for colname in colnames:
                    
                    # Replace any values that are null in GDC with corresponding non-null values from PDC.

                    if record[colname] is None or record[colname] == '':
                        
                        if pdc_subject_preload[current_id][colname] is not None and pdc_subject_preload[current_id][colname] != '':
                            
                            record[colname] = pdc_subject_preload[current_id][colname]

                            if colname not in null_subs_made:
                                
                                null_subs_made[colname] = dict()

                            if pdc_subject_preload[current_id][colname] not in null_subs_made[colname]:
                                
                                null_subs_made[colname][pdc_subject_preload[current_id][colname]] = 1

                            else:
                                
                                null_subs_made[colname][pdc_subject_preload[current_id][colname]] = null_subs_made[colname][pdc_subject_preload[current_id][colname]] + 1

                    elif record[colname] is not None and record[colname] != '':
                        
                        gdc_value = record[colname]

                        if pdc_subject_preload[current_id][colname] is None or pdc_subject_preload[current_id][colname] == '':
                            
                            pdc_value = ''

                        else:
                            
                            pdc_value = pdc_subject_preload[current_id][colname]

                        if pdc_value != gdc_value:
                            
                            if colname not in clashes_ignored:
                                
                                clashes_ignored[colname] = dict()

                            if pdc_value not in clashes_ignored[colname]:
                                
                                clashes_ignored[colname][pdc_value] = dict()

                            if gdc_value not in clashes_ignored[colname][pdc_value]:
                                
                                clashes_ignored[colname][pdc_value][gdc_value] = 1

                            else:
                                
                                clashes_ignored[colname][pdc_value][gdc_value] = clashes_ignored[colname][pdc_value][gdc_value] + 1

            print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

    # Use the PDC file as the next source for output records, ignoring any PDC subject data already scanned.

    with open( pdc_subject_input_tsv ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            record = dict( zip( colnames, line.split('\t') ) )

            current_id = record['id']

            if current_id not in merged_from_pdc:
                
                print( *[ record[colname] for colname in colnames ], sep='\t', end='\n', file=OUT )

sort_file_with_header( subject_output_tsv )

# Log GDC null values that were replaced with non-null data from PDC.

with open( null_sub_log, 'w' ) as LOG:
    
    print( *[ 'subject_column', 'PDC_value', 'substitution_count' ], sep='\t', end='\n', file=LOG )

    for colname in sorted( null_subs_made ):
        
        for value in sorted( null_subs_made[colname] ):
            
            print( *[ colname, value, null_subs_made[colname][value] ], sep='\t', end='\n', file=LOG )

# Log non-null values in both DCs that didn't match (these were NOT used to update GDC metadata).

with open( clashes_ignored_log, 'w' ) as LOG:
    
    print( *[ 'subject_column', 'PDC_value', 'GDC_value', 'match_count' ], sep='\t', end='\n', file=LOG )

    for colname in sorted( clashes_ignored ):
        
        for pdc_value in sorted( clashes_ignored[colname] ):
            
            for gdc_value in sorted( clashes_ignored[colname][pdc_value] ):
                
                print( *[ colname, pdc_value, gdc_value, clashes_ignored[colname][pdc_value][gdc_value] ], sep='\t', end='\n', file=LOG )

# Log before- and after-IDs for PDC subject records that got merged with (subsumed by) GDC subject records.

with open( subject_merged_log, 'w' ) as LOG:
    
    print( *[ 'PDC_subject_id', 'GDC_subject_id' ], sep='\t', end='\n', file=LOG )

    for pdc_subject_id in sorted( merged_from_pdc ):
        
        gdc_subject_id = pdc_subject_id_to_gdc_subject_id[pdc_subject_id]

        print( *[ pdc_subject_id, gdc_subject_id ], sep='\t', end='\n', file=LOG )

# Concatenate the rest of everything. We're only merging records at the subject level at this time,
# because of ongoing challenges with PDC Study<->GDC Project mappability and ambiguous definitions
# of what might/should constitute "distinct records" vs "the same record" (e.g. one Diagnosis record
# with its own submitter_id but whose human is in multiple intra-DC studies/projects: current
# ID rubrique == "CDA_researchsubject_ID.diagnosis_submitter_id". alternative naming conventions, e.g.
# "researchsubject_submitter_ID.diagnosis_submitter_id", lead to unresolvable ambiguities across projects,
# but including disambiguating project/study qualifiers (as we currently do) splits one record into
# multiple copies, one per researchsubject (i.e. not one per human, in the case of multiple
# researchsubjects mapping to a single subject).

# ...anyway, process the rest of the tables via concatenation without merging.

for file_basename in listdir( gdc_input_dir ):
    
    # We already covered subject_* and file_subject; don't parse anything not ending in '.tsv'.

    if re.search( r'^subject', file_basename ) is None and file_basename != 'file_subject.tsv' and re.search( r'\.tsv$', file_basename ) is not None:
        
        gdc_file_full_path = path.join( gdc_input_dir, file_basename )

        pdc_file_full_path = path.join( pdc_input_dir, file_basename )

        dest_file_full_path = path.join( output_dir, file_basename )

        with open( dest_file_full_path, 'w' ) as OUT:
            
            with open( gdc_file_full_path ) as IN:
                
                header = next(IN).rstrip('\n')

                print( header, end='\n', file=OUT )

                for line in [ next_line.rstrip('\n') for next_line in IN ]:
                    
                    print( line, end='\n', file=OUT )

            if path.isfile( pdc_file_full_path ):
                
                with open( pdc_file_full_path ) as IN:
                    
                    header = next(IN).rstrip('\n')

                    for line in [ next_line.rstrip('\n') for next_line in IN ]:
                        
                        print( line, end='\n', file=OUT )

        sort_file_with_header( dest_file_full_path )



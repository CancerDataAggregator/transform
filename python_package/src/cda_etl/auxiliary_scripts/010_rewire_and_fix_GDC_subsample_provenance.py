#!/usr/bin/env python -u

import re
import shutil
import sys

from os import listdir, makedirs, path, rename

from cda_etl.lib import load_tsv_as_dict, sort_file_with_header

# SUBROUTINES

def count_descendant_leaves_with_submitter_ids( sample_id, children, object_type, submitter_id, target_type ):
    
    final_count = 0

    if sample_id in children:
        
        for child_id in children[sample_id]:
            
            if object_type[child_id] == target_type and child_id not in children:
                
                if child_id in submitter_id and submitter_id[child_id] is not None and submitter_id[child_id] != '':
                    
                    final_count = final_count + 1

            elif child_id in children:
                
                final_count = final_count + count_descendant_leaves_with_submitter_ids( child_id, children, object_type, submitter_id, target_type )

    return final_count

def count_descendant_nonleaves_missing_submitter_ids( sample_id, children, object_type, submitter_id, target_type ):
    
    final_count = 0

    if sample_id in children:
        
        for child_id in children[sample_id]:
            
            if object_type[child_id] == target_type and child_id in children:
                
                if child_id not in submitter_id or submitter_id[child_id] is None or submitter_id[child_id] == '':
                    
                    final_count = final_count + 1

            elif child_id in children:
                
                final_count = final_count + count_descendant_nonleaves_missing_submitter_ids( child_id, children, object_type, submitter_id, target_type )

    return final_count

# PARAMETERS

input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

aux_root = 'auxiliary_metadata'

# The postprocessed output of this script does not go into CDA public releases; we prepare
# it upstream of ISB-CGC's analytic consumption of the data as a courtesy, but responsibility
# for this level of curation rests firmly with the CRDC data centers and not with CDA.

output_root = path.join( 'extracted_data', 'gdc_isb_cgc' )

subsample_provenance_dir = path.join( aux_root, '__GDC_supplemental_metadata', 'subsample_provenance' )

non_aliquot_or_slide_leaves_by_type_file = path.join( subsample_provenance_dir, 'non-aliquot-or-slide_leaf_specimens_by_type.tsv' )

non_sample_roots_by_type_file = path.join( subsample_provenance_dir, 'non-sample_root_specimens_by_type.tsv' )

non_sample_specimens_whose_parents_lack_submitter_ids_file = path.join( subsample_provenance_dir, 'non-sample_specimens_whose_parents_lack_submitter_ids_by_type.tsv' )

cases_with_specimens_missing_submitter_ids_and_weird_leaf_specimens_file = path.join( subsample_provenance_dir, 'cases_with_non-aliquot-or-slide_leaf_specimens_AND_specimens_lacking_submitter_IDs.tsv' )

samples_with_suspicious_groups_of_portion_or_analyte_subsamples_file = path.join( subsample_provenance_dir, 'samples_with_suspicious_groups_of_portion_or_analyte_subsamples.tsv' )

rewire_log = path.join( subsample_provenance_dir, 'analytes_rewired_to_correct_parent_portions.tsv' )

table_names = [
    
    'aliquot',
    'aliquot_from_case',
    'aliquot_of_analyte',
    'analyte',
    'analyte_from_case',
    'analyte_from_portion',
    'case',
    'case_in_project',
    'portion',
    'portion_from_case',
    'portion_from_sample',
    'sample',
    'sample_from_case',
    'slide',
    'slide_from_case',
    'slide_from_portion'
]

table_data = dict()

for table_name in table_names:
    
    table_file = path.join( input_root, f"{table_name}.tsv" )

    table_data[table_name] = load_tsv_as_dict( table_file )

# EXECUTION

for output_dir in [ output_root, subsample_provenance_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Save object types and submitter IDs, and load one data structure to track case affiliations.
# 
# IMPORTANT: GDCs UUIDs really are UUIDs, i.e. there's no case whose case_id is the same as some sample's sample_id; IDs are
# unique both within AND across entities. If this weren't true, the following would not work.

object_type = dict()

submitter_id = dict()

case = dict()

project = dict()

specimen_count = dict()

number_of_specimens_lacking_submitter_ids = dict()

for entity in [ 'aliquot', 'analyte', 'portion', 'sample', 'slide' ]:
    
    for record_id in table_data[entity]:
        
        object_type[record_id] = entity

        submitter_id[record_id] = table_data[entity][record_id]['submitter_id']

    for record_id in table_data[f"{entity}_from_case"]:
        
        case_id = table_data[f"{entity}_from_case"][record_id]['case_id']

        case[record_id] = case_id

        project[record_id] = table_data['case_in_project'][case_id]['project_id']

        object_type[case_id] = 'case'

        submitter_id[case_id] = table_data['case'][case_id]['submitter_id']

        project[case_id] = project[record_id]

        if case_id not in specimen_count:
            
            specimen_count[case_id] = 1

        else:
            
            specimen_count[case_id] = specimen_count[case_id] + 1

        if submitter_id[record_id] is None or submitter_id[record_id] == '':
            
            if case_id not in number_of_specimens_lacking_submitter_ids:
                
                number_of_specimens_lacking_submitter_ids[case_id] = 1

            else:
                
                number_of_specimens_lacking_submitter_ids[case_id] = number_of_specimens_lacking_submitter_ids[case_id] + 1

# Build provenance/subsampling tree(s) using what the API asserted.
# 
# IMPORTANT: GDCs UUIDs really are UUIDs, i.e. there's no case whose case_id is the same as some sample's sample_id; IDs are
# unique both within AND across entities. If this weren't true, the following would not work.

parents = dict()

children = dict()

for link_table in [ 'aliquot_of_analyte', 'analyte_from_portion', 'portion_from_sample', 'slide_from_portion' ]:
    
    if re.search( r'_of_', link_table ) is not None:
        
        ( type_one, type_two ) = link_table.split( '_of_' )

    else:
        
        ( type_one, type_two ) = link_table.split( '_from_' )

    for thing_one_id in table_data[link_table]:
        
        thing_two_id = table_data[link_table][thing_one_id][f"{type_two}_id"]

        if thing_one_id not in parents:
            
            parents[thing_one_id] = set()

        parents[thing_one_id].add( thing_two_id )

        if thing_two_id not in children:
            
            children[thing_two_id] = set()

        children[thing_two_id].add( thing_one_id )

# Count leaf records that are supposed to be for interstitial entities by case.

leaf_count = {
    
    'analyte_leaves': dict(),
    'portion_leaves': dict()
}

with open( non_aliquot_or_slide_leaves_by_type_file, 'w' ) as OUT:
    
    print( *[ 'id', 'type', 'submitter_id', 'case_submitter_id' ], sep='\t', end='\n', file=OUT )

    for current_type in [ 'analyte', 'portion' ]:
        
        for specimen_id in sorted( table_data[current_type] ):
            
            if specimen_id not in children:
                
                print( *[ specimen_id, current_type, submitter_id[specimen_id], submitter_id[case[specimen_id]] ], sep='\t', end='\n', file=OUT )

                leaf_count_keyword = f"{current_type}_leaves"

                if case[specimen_id] not in leaf_count[leaf_count_keyword]:
                    
                    leaf_count[leaf_count_keyword][case[specimen_id]] = 1

                else:
                    
                    leaf_count[leaf_count_keyword][case[specimen_id]] = leaf_count[leaf_count_keyword][case[specimen_id]] + 1

# Roll up some key counts by top-level sample.

analyte_leaves_with_submitter_ids = dict()

analyte_nonleaves_missing_submitter_ids = dict()

portion_leaves_with_submitter_ids = dict()

portion_nonleaves_missing_submitter_ids = dict()

for sample_id in sorted( table_data['sample'] ):
    
    if sample_id in children:
        
        analyte_leaves_with_submitter_ids[sample_id] = count_descendant_leaves_with_submitter_ids( sample_id, children, object_type, submitter_id, 'analyte' )

        analyte_nonleaves_missing_submitter_ids[sample_id] = count_descendant_nonleaves_missing_submitter_ids( sample_id, children, object_type, submitter_id, 'analyte' )

        portion_leaves_with_submitter_ids[sample_id] = count_descendant_leaves_with_submitter_ids( sample_id, children, object_type, submitter_id, 'portion' )

        portion_nonleaves_missing_submitter_ids[sample_id] = count_descendant_nonleaves_missing_submitter_ids( sample_id, children, object_type, submitter_id, 'portion' )

with open( non_sample_roots_by_type_file, 'w' ) as OUT:
    
    print( *[ 'id', 'type', 'submitter_id', 'case_submitter_id' ], sep='\t', end='\n', file=OUT )

    for current_type in [ 'aliquot', 'analyte', 'portion', 'slide' ]:
        
        for specimen_id in sorted( children ):
            
            if object_type[specimen_id] == current_type and specimen_id not in parents:
                
                print( *[ specimen_id, current_type, submitter_id[specimen_id], submitter_id[case[specimen_id]] ], sep='\t', end='\n', file=OUT )

with open( non_sample_specimens_whose_parents_lack_submitter_ids_file, 'w' ) as OUT:
    
    print( *[ 'id', 'type', 'submitter_id', 'case_submitter_id', 'number_of_parents' ], sep='\t', end='\n', file=OUT )

    for current_type in [ 'aliquot', 'analyte', 'portion', 'slide' ]:
        
        for specimen_id in sorted( parents ):
            
            if object_type[specimen_id] == current_type:
                
                found_null = False

                parent_count = 0

                for parent_id in parents[specimen_id]:
                    
                    parent_count = parent_count + 1

                    if submitter_id[parent_id] is None or submitter_id[parent_id] == '':
                        
                        found_null = True

                if found_null:
                    
                    print( *[ specimen_id, current_type, submitter_id[specimen_id], submitter_id[case[specimen_id]], parent_count ], sep='\t', end='\n', file=OUT )

with open( cases_with_specimens_missing_submitter_ids_and_weird_leaf_specimens_file, 'w' ) as OUT:
    
    print( *[ 'case_id', 'total_specimens', 'specimens_missing_submitter_ids', 'analyte_leaves', 'portion_leaves' ], sep='\t', end='\n', file=OUT )

    for case_id in sorted( table_data['case'] ):
        
        if case_id in number_of_specimens_lacking_submitter_ids:
            
            analyte_leaves = 0

            if case_id in leaf_count['analyte_leaves']:
                
                analyte_leaves = leaf_count['analyte_leaves'][case_id]

            portion_leaves = 0

            if case_id in leaf_count['portion_leaves']:
                
                portion_leaves = leaf_count['portion_leaves'][case_id]

            if analyte_leaves != 0 or portion_leaves != 0:
                
                print( *[ case_id, specimen_count[case_id], number_of_specimens_lacking_submitter_ids[case_id], analyte_leaves, portion_leaves ], sep='\t', end='\n', file=OUT )

samples_to_rewire = set()

with open( samples_with_suspicious_groups_of_portion_or_analyte_subsamples_file, 'w' ) as OUT:
    
    print( *[ 'sample_id', 'sample_submitter_id', 'project_id', 'analyte_leaves_with_submitter_ids', 'analyte_nonleaves_missing_submitter_ids', 'portion_leaves_with_submitter_ids', 'portion_nonleaves_missing_submitter_ids' ], sep='\t', end='\n', file=OUT )

    for sample_id in sorted( table_data['sample'] ):
        
        matching_analyte_leaf_count = 0

        if sample_id in analyte_leaves_with_submitter_ids:
            
            matching_analyte_leaf_count = analyte_leaves_with_submitter_ids[sample_id]

        matching_analyte_nonleaf_count = 0

        if sample_id in analyte_nonleaves_missing_submitter_ids:
            
            matching_analyte_nonleaf_count = analyte_nonleaves_missing_submitter_ids[sample_id]
        
        matching_portion_leaf_count = 0

        if sample_id in portion_leaves_with_submitter_ids:
            
            matching_portion_leaf_count = portion_leaves_with_submitter_ids[sample_id]

        matching_portion_nonleaf_count = 0

        if sample_id in portion_nonleaves_missing_submitter_ids:
            
            matching_portion_nonleaf_count = portion_nonleaves_missing_submitter_ids[sample_id]

        if ( matching_analyte_leaf_count > 0 and matching_analyte_nonleaf_count > 0 ) or ( matching_portion_leaf_count > 0 and matching_portion_nonleaf_count > 0 ):
            
            print( *[ sample_id, submitter_id[sample_id], project[sample_id], matching_analyte_leaf_count, matching_analyte_nonleaf_count, matching_portion_leaf_count, matching_portion_nonleaf_count ], sep='\t', end='\n', file=OUT )
            
            # At time of writing, this filter results in a set that exactly matches the 188 defective ORGANOID-PANCREATIC samples we need to correct.

            if matching_portion_leaf_count == 1 and matching_portion_nonleaf_count == 1:
                
                samples_to_rewire.add( sample_id )

# Load data structures needed to rewire target samples. For each sample, take all children of
# that sample's (unique) non-leaf portion that lacks a submitter_id, and reattach those children
# to the (also unique) (initially leaf) portion of the sample that has a submitter_id.

delete_portions = set()

disconnect_children_from_old_parent = dict()

connect_children_to_new_parent = dict()

for sample_id in samples_to_rewire:
    
    portion_to_remove = ''

    new_parent_portion = ''

    for child_id in children[sample_id]:
        
        if object_type[child_id] == 'portion':
            
            if child_id in children and ( child_id not in submitter_id or submitter_id[child_id] is None or submitter_id[child_id] == '' ):
                
                portion_to_remove = child_id

                for rewire_child_id in children[portion_to_remove]:
                    
                    disconnect_children_from_old_parent[rewire_child_id] = portion_to_remove

            elif child_id not in children and child_id in submitter_id and submitter_id[child_id] is not None and submitter_id[child_id] != '':
                
                new_parent_portion = child_id

    if portion_to_remove == '' or new_parent_portion == '':
        
        sys.exit("Rut roh.")

    delete_portions.add( portion_to_remove )

    for rewire_child_id in children[portion_to_remove]:
        
        connect_children_to_new_parent[rewire_child_id] = new_parent_portion

for input_file_basename in sorted( listdir( input_root ) ):

    if re.search( r'\.tsv$', input_file_basename ) is not None:
        
        shutil.copy2( path.join( input_root, input_file_basename ), path.join( output_root, input_file_basename ) )

for input_file_basename in [ 'analyte_from_portion.tsv', 'slide_from_portion.tsv' ]:
    
    input_file = path.join( input_root, input_file_basename )

    output_file = path.join( output_root, input_file_basename )

    current_object_type = 'analyte'

    if re.search( r'slide_', input_file_basename ) is not None:
        
        current_object_type = 'slide'

    with open( input_file ) as IN, open( output_file, 'w' ) as OUT:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        print( *colnames, sep='\t', end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split( '\t' )

            child_id = values[0]

            portion_id = values[1]

            if child_id in disconnect_children_from_old_parent:
                
                # Skip it, but make sure it's sane.

                if disconnect_children_from_old_parent[child_id] != portion_id:
                    
                    sys.exit(f"Unexpected parent linkage for child specimen {child_id}, expected parent {disconnect_children_from_old_parent[child_id]}, encountered {portion_id} instead; aborting.")

            else:
                
                print( *values, sep='\t', end='\n', file=OUT )

        for child_id in connect_children_to_new_parent:
            
            if object_type[child_id] == current_object_type:
                
                print( *[ child_id, connect_children_to_new_parent[child_id] ], sep='\t', end='\n', file=OUT )

    sort_file_with_header( output_file )

for input_file_basename in [ 'portion.tsv', 'portion_from_case.tsv', 'portion_from_sample.tsv' ]:
    
    input_file = path.join( input_root, input_file_basename )

    output_file = path.join( output_root, input_file_basename )

    with open( input_file ) as IN, open( output_file, 'w' ) as OUT:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        print( *colnames, sep='\t', end='\n', file=OUT )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split( '\t' )

            portion_id = values[0]

            if portion_id not in delete_portions:
                
                print( *values, sep='\t', end='\n', file=OUT )

with open( rewire_log, 'w' ) as OUT:
    
    print( *[ 'project', 'child_specimen_id', 'child_specimen_submitter_id', 'child_specimen_type', 'old_parent_specimen_id', 'old_parent_specimen_type', 'new_parent_specimen_id', 'new_parent_specimen_submitter_id', 'new_parent_specimen_type' ], sep='\t', end='\n', file=OUT )

    for child_id in sorted( disconnect_children_from_old_parent ):
        
        print( *[ project[child_id], child_id, submitter_id[child_id], object_type[child_id], \
            disconnect_children_from_old_parent[child_id], object_type[disconnect_children_from_old_parent[child_id]], \
            connect_children_to_new_parent[child_id], submitter_id[connect_children_to_new_parent[child_id]], object_type[connect_children_to_new_parent[child_id]] ], \
            sep='\t', end='\n', file=OUT )



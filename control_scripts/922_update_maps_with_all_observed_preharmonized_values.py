#!/usr/bin/env python3 -u

import re
import sys

from os import path, listdir, makedirs

from cda_etl.lib import load_obo_file, get_universal_value_deletion_patterns, load_tsv_as_dict

# PARAMETERS

cda_tsv_root = path.join( 'cda_tsvs' )

harmonization_root = path.join( 'harmonization_maps' )

column_concept_map_file = path.join( harmonization_root, '000_cda_column_targets.tsv' )

output_dir = path.join( harmonization_root, 'zz01_maps_updated_with_all_observed_values' )

ontology_reference_root = path.join( 'auxiliary_metadata', '__ontology_reference' )

uberon_reference_dir = path.join( ontology_reference_root, 'UBERON' )

uberon_obo_file = path.join( uberon_reference_dir, 'uberon_ext.obo' )

icd_o_3_reference_dir = path.join( ontology_reference_root, 'ICD-O-3' )

icd_o_3_name_file = path.join( icd_o_3_reference_dir, 'icd_o_3.codes_and_preferred_names.tsv' )

do_reference_dir = path.join( ontology_reference_root, 'DO' )

do_obo_file = path.join( do_reference_dir, 'doid-merged.obo' )

null_values = [ '__CDA_UNASSIGNED__', 'null' ]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load ontology reference data.

uberon_terms = load_obo_file( uberon_obo_file )

uberon_id_to_name = dict()

uberon_name_to_id = dict()

for uberon_id in uberon_terms:
    
    uberon_names = list( uberon_terms[uberon_id]['name'] )

    if len( uberon_names ) != 1:
        
        sys.exit( f"FATAL: UBERON term '{uberon_id}' has {len( uberon_names )} distinct values for 'name' -- please handle." )

    else:
        
        uberon_name = uberon_names[0]

        uberon_id_to_name[uberon_id] = uberon_name

        uberon_name_to_id[uberon_name] = uberon_id

icd_o_3_name = dict()

with open( icd_o_3_name_file ) as IN:
    
    header = next( IN )

    seen = set()

    for next_line in IN:
        
        ( code, name ) = next_line.rstrip( '\n' ).split( '\t' )

        if code in seen:
            
            sys.exit( f"FATAL: Duplicate code listed in {icd_o_3_name_file}: '{code}'; something has gone wrong with upstream processing, please investigate. Aborting." )

        icd_o_3_name[code] = name

        seen.add( code )

do_terms = load_obo_file( do_obo_file )

icd_code_to_do_id = dict()

do_id_to_name = dict()

do_id_to_ncit_code = dict()

skip_do_map = set()

for do_id in do_terms:
    
    do_names = list( do_terms[do_id]['name'] )

    if len( do_names ) != 1:
        
        sys.exit( f"FATAL: DO term '{do_id}' has {len( do_names )} distinct values for 'name' -- please handle." )

    else:
        
        do_name = do_names[0]

        do_id_to_name[do_id] = do_name

        if 'xref' in do_terms[do_id]:
            
            for xref_code in sorted( do_terms[do_id]['xref'] ):
                
                # Check for NCIt xrefs.

                match_result = re.search( r'^NCI:(.+)$', xref_code )

                if match_result is not None:
                    
                    ncit_code = match_result.group(1)

                    if do_id not in do_id_to_ncit_code:
                        
                        do_id_to_ncit_code[do_id] = set()

                    do_id_to_ncit_code[do_id].add( ncit_code )

                # Check for unambiguous ICD-O-3 xrefs.

                match_result = re.search( r'^ICDO:(.+)$', xref_code )

                if match_result is not None:
                    
                    icd_code = match_result.group(1)

                    if icd_code in icd_code_to_do_id:
                        
                        if icd_code not in skip_do_map:
                            
                            print( f"WARNING: ICD-O-3 xref '{icd_code}' is associated with multiple DOIDs; will not auto-populate DO IDs for this code." )

                            skip_do_map.add( icd_code )

                    else:
                        
                        icd_code_to_do_id[icd_code] = do_id

delete_everywhere = get_universal_value_deletion_patterns()

columns_to_concepts = load_tsv_as_dict( column_concept_map_file, id_column_count=2 )

observed_values = dict()

for cda_tsv_sub in sorted( listdir( cda_tsv_root ) ):
    
    if re.search( r'_000_unharmonized$', cda_tsv_sub ) is not None:
        
        input_dir = path.join( cda_tsv_root, cda_tsv_sub )

        for input_sub in sorted( listdir( input_dir ) ):
            
            match_result = re.search( r'^(.*)\.tsv$', input_sub )

            if match_result is not None:
                
                table_name = match_result.group( 1 )

                if table_name in columns_to_concepts:
                    
                    '''
                    # Sanity check:

                    for column_name in sorted( columns_to_concepts[table_name] ):
                        
                        concept_name = columns_to_concepts[table_name][column_name]['concept_map_name']

                        print( f"{input_dir}: {table_name}.{column_name} <- {concept_name}" )
                    '''

                    with open( path.join( input_dir, input_sub ) ) as IN:
                        
                        header = next( IN ).rstrip( '\n' )

                        column_names = header.split( '\t' )

                        for next_line in IN:
                            
                            record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                            for column_name in record:
                                
                                if column_name in columns_to_concepts[table_name]:
                                    
                                    concept_name = columns_to_concepts[table_name][column_name]['concept_map_name']

                                    new_value = record[column_name]

                                    # Skip null values and universally-deleted values like 'Unknown' and 'Not reported'.

                                    if new_value is not None and new_value.strip() != '' and re.sub( r'\s', r'', new_value.strip().lower() ) not in delete_everywhere:
                                        
                                        # Ignore case and surrounding whitespace.

                                        new_value = new_value.strip().lower()

                                        if concept_name not in observed_values:
                                            
                                            observed_values[concept_name] = set()

                                        observed_values[concept_name].add( new_value )

# Mutation data is handled separately, downstream of DC processing.

if 'mutation' in columns_to_concepts:
    
    table_name = 'mutation'

    for column_name in sorted( columns_to_concepts[table_name] ):
        
        concept_name = columns_to_concepts[table_name][column_name]['concept_map_name']

        harmonization_log = path.join( 'auxiliary_metadata', '__harmonization_logs', table_name, f"{table_name}.{column_name}.substitution_log.tsv" )

        '''
        # Sanity check:

        print( f"{harmonization_log}: {table_name}.{column_name} <- {concept_name}" )
        '''

        with open( harmonization_log ) as IN:
            
            sub_log = load_tsv_as_dict( harmonization_log )

            for new_value in sub_log:
                
                # Skip null values and universally-deleted values like 'Unknown' and 'Not reported'.

                if new_value is not None and new_value.strip() != '' and re.sub( r'\s', r'', new_value.strip().lower() ) not in delete_everywhere:
                    
                    # Ignore case and surrounding whitespace.

                    new_value = new_value.strip().lower()

                    if concept_name not in observed_values:
                        
                        observed_values[concept_name] = set()

                    observed_values[concept_name].add( new_value )

for concept_name in sorted( observed_values ):
    
    old_map_file = path.join( harmonization_root, f"{concept_name}.tsv" )

    old_map = dict()

    with open( old_map_file ) as IN:
        
        header = next( IN ).rstrip( '\n' )

        for next_line in IN:
            
            if concept_name == 'species':
                
                ( value, ncbi_tax_id, scientific_name, cda_common_name ) = next_line.rstrip( '\n' ).split( '\t' )

                lc_value = value.lower()

                target = dict()

                target['ncbi_tax_id'] = ncbi_tax_id
                target['scientific_name'] = scientific_name
                target['cda_common_name'] = cda_common_name

                if lc_value in old_map:
                    
                    existing_dict = old_map[lc_value]

                    for field_name in sorted( target ):
                        
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )

                old_map[lc_value] = target

            elif concept_name == 'disease':
                
                ( value, icd_o_3_code, icd_o_3_preferred_name, do_id, do_name, ncit_codes ) = next_line.rstrip( '\n' ).split( '\t' )

                lc_value = value.lower()

                target = dict()

                target['icd_o_3_code'] = icd_o_3_code
                target['icd_o_3_preferred_name'] = icd_o_3_preferred_name
                target['do_id'] = do_id
                target['do_name'] = do_name

                if lc_value in old_map:
                    
                    existing_dict = old_map[lc_value]

                    for field_name in sorted( target ):
                        
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )

                # Once a match is confirmed or deemed irrelevant, update term names from the current ontology data.

                if icd_o_3_code not in null_values:
                    
                    if icd_o_3_code not in icd_o_3_name:
                        
                        print( f"WARNING: ICD-O-3 code '{icd_o_3_code}' not found in ICD-O-3 reference data. Leaving name as '{icd_o_3_preferred_name}' -- if this seems wrong, investigate.", file=sys.stderr )

                    else:
                        
                        target['icd_o_3_preferred_name'] = icd_o_3_name[icd_o_3_code]

                    # If we didn't have a DO annotation before, try to add one.

                    if do_id in null_values and icd_o_3_code in icd_code_to_do_id and icd_o_3_code not in skip_do_map:
                        
                        do_id = icd_code_to_do_id[icd_o_3_code]

                        target['do_id'] = do_id

                        if do_id not in do_id_to_name:
                            
                            sys.exit( f"FATAL: DO term ID '{do_id}' not found in DO reference data. Please investigate." )

                        target['do_name'] = do_id_to_name[do_id]

                    # Ensure correct names.

                    if do_id not in null_values:
                        
                        if do_id not in do_id_to_name:
                            
                            sys.exit( f"FATAL: DO term ID '{do_id}' not found in DO reference data. Please investigate." )

                        target['do_name'] = do_id_to_name[do_id]

                    else:
                        
                        target['do_name'] = do_id

                else:
                    
                    # Everything should flow first from the ICD-O-3 code. If it isn't there, we have nothing.

                    target['icd_o_3_code'] = icd_o_3_code
                    target['icd_o_3_preferred_name'] = icd_o_3_code
                    target['do_id'] = icd_o_3_code
                    target['do_name'] = icd_o_3_code

                old_map[lc_value] = target

            elif concept_name == 'anatomic_site':
                
                ( value, uberon_id, uberon_name ) = next_line.rstrip( '\n' ).split( '\t' )

                lc_value = value.lower()

                target = dict()

                target['uberon_id'] = uberon_id
                target['uberon_name'] = uberon_name

                if lc_value in old_map:
                    
                    existing_dict = old_map[lc_value]

                    for field_name in sorted( target ):
                        
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )

                # Once a match is confirmed or deemed irrelevant, update term names from the current ontology data.

                if uberon_id not in null_values:
                    
                    if uberon_id not in uberon_id_to_name:
                        
                        sys.exit( f"FATAL: UBERON term ID '{uberon_id}' not found in UBERON reference data. Please investigate." )

                    target['uberon_name'] = uberon_id_to_name[uberon_id]

                old_map[lc_value] = target

            else:
                
                ( value, target ) = next_line.rstrip( '\n' ).split( '\t' )

                lc_value = value.lower()

                if lc_value in old_map and old_map[lc_value] != target:
                    
                    print( f"YARRRGH! ({value}:{target}) does not match loaded map data ({lc_value}:{old_map[lc_value]})!!!", file=sys.stderr )

                old_map[lc_value] = target

    output_file = path.join( output_dir, f"{concept_name}.tsv" )

    with open( output_file, 'w' ) as OUT:
        
        if concept_name == 'anatomic_site':
            
            print( *[ 'unharmonized value', 'UBERON id', 'UBERON name' ], sep='\t', file=OUT )

        elif concept_name == 'species':
            
            print( *[ 'unharmonized value', 'NCBI Taxonomy ID', 'scientific name', 'CDA curated common name' ], sep='\t', file=OUT )

        elif concept_name == 'disease':
            
            print( *[ 'unharmonized value', 'icd_o_3_code', 'icd_o_3_preferred_name', 'do_id', 'do_name', 'ncit_concept_codes' ], sep='\t', file=OUT )

        else:
            
            print( *[ 'unharmonized value', 'harmonized value' ], sep='\t', file=OUT )

        for observed_value in sorted( set( sorted( old_map ) + sorted( observed_values[concept_name] ) ) ):
            
            # Weed legacy delete-everywhere values from existing maps.

            if observed_value is not None and observed_value.strip() != '' and re.sub( r'\s', r'', observed_value.strip().lower() ) not in delete_everywhere:
                
                target_value = '__CDA_UNASSIGNED__'

                printed = False

                if observed_value in old_map:
                    
                    if concept_name == 'species':
                        
                        target_dict = old_map[observed_value]

                        print( *[ observed_value, target_dict['ncbi_tax_id'], target_dict['scientific_name'], target_dict['cda_common_name'] ], sep='\t', file=OUT )

                        printed = True

                    elif concept_name == 'disease':
                        
                        target_dict = old_map[observed_value]

                        ncit_codes = '__CDA_UNASSIGNED__'

                        if target_dict['icd_o_3_code'] == '__CDA_UNASSIGNED__':
                            
                            if observed_value in icd_o_3_name:
                                
                                icd_o_3_code = observed_value

                                icd_o_3_preferred_name = icd_o_3_name[icd_o_3_code]

                                do_id = '__CDA_UNASSIGNED__'

                                do_name = '__CDA_UNASSIGNED__'

                                if icd_o_3_code in icd_code_to_do_id and icd_o_3_code not in skip_do_map:
                                    
                                    do_id = icd_code_to_do_id[icd_o_3_code]

                                    if do_id not in do_id_to_name:
                                        
                                        sys.exit( f"FATAL: DO term ID '{do_id}' not found in DO reference data. Please investigate." )

                                    do_name = do_id_to_name[do_id]

                                    if do_id in do_id_to_ncit_code:
                                        
                                        ncit_codes = ';'.join( sorted( do_id_to_ncit_code[do_id] ) )

                                target_dict['icd_o_3_code'] = icd_o_3_code

                                target_dict['icd_o_3_preferred_name'] = icd_o_3_preferred_name

                                target_dict['do_id'] = do_id

                                target_dict['do_name'] = do_name

                        elif target_dict['icd_o_3_code'] == 'null':
                            
                            ncit_codes = 'null'

                        elif target_dict['do_id'] not in null_values and target_dict['do_id'] in do_id_to_ncit_code:
                            
                            ncit_codes = ';'.join( sorted( do_id_to_ncit_code[target_dict['do_id']] ) )

                        print( *[ observed_value, target_dict['icd_o_3_code'], target_dict['icd_o_3_preferred_name'], target_dict['do_id'], target_dict['do_name'], ncit_codes ], sep='\t', file=OUT )

                        printed = True

                    elif concept_name == 'anatomic_site':
                        
                        target_dict = old_map[observed_value]

                        print( *[ observed_value, target_dict['uberon_id'], target_dict['uberon_name'] ], sep='\t', file=OUT )

                        printed = True

                    else:
                        
                        target_value = old_map[observed_value]

                elif concept_name == 'anatomic_site':
                    
                    print( *[ observed_value, target_value, target_value ], sep='\t', file=OUT )

                    printed = True

                elif concept_name == 'species':
                    
                    print( *[ observed_value, target_value, target_value, target_value ], sep='\t', file=OUT )

                    printed = True

                elif concept_name == 'disease':
                    
                    if observed_value in icd_o_3_name:
                        
                        icd_o_3_code = observed_value

                        icd_o_3_preferred_name = icd_o_3_name[icd_o_3_code]

                        do_id = '__CDA_UNASSIGNED__'

                        do_name = '__CDA_UNASSIGNED__'

                        ncit_codes = '__CDA_UNASSIGNED__'

                        if icd_o_3_code in icd_code_to_do_id and icd_o_3_code not in skip_do_map:
                            
                            do_id = icd_code_to_do_id[icd_o_3_code]

                            if do_id not in do_id_to_name:
                                
                                sys.exit( f"FATAL: DO term ID '{do_id}' not found in DO reference data. Please investigate." )

                            do_name = do_id_to_name[do_id]

                            if do_id in do_id_to_ncit_code:
                                
                                ncit_codes = ';'.join( sorted( do_id_to_ncit_code[do_id] ) )

                        print( *[ observed_value, icd_o_3_code, icd_o_3_preferred_name, do_id, do_name, ncit_codes ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ observed_value, target_value, target_value, target_value, target_value, target_value ], sep='\t', file=OUT )

                    printed = True

                if not printed:
                    
                    print( *[ observed_value, target_value], sep='\t', file=OUT )



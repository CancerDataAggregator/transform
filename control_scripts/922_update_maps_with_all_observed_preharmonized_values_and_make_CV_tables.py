#!/usr/bin/env python3 -u

import json
import re
import sys

from os import path, listdir, makedirs

from cda_etl.lib import load_obo_file, get_universal_value_deletion_patterns, load_tsv_as_dict

# PARAMETERS

cda_tsv_root = path.join( 'cda_tsvs' )

harmonization_root = path.join( 'harmonization_maps' )
column_concept_map_file = path.join( harmonization_root, '000_cda_column_targets.tsv' )
output_dir = path.join( harmonization_root, 'zz01_maps_updated_with_all_observed_values' )
slim_dir = path.join( harmonization_root, '001_slims' )
new_slim_dir = path.join( harmonization_root, 'zz02_slims_updated_with_all_observed_values' )
term_table_dir = path.join( harmonization_root, 'zz03_term_tables' )
controlled_term_tsv = path.join( term_table_dir, 'controlled_term.tsv' )
synonym_term_tsv = path.join( term_table_dir, 'synonym_term.tsv' )
slim_term_tsv = path.join( term_table_dir, 'slim_term.tsv' )
related_term_tsv = path.join( term_table_dir, 'related_term.tsv' )
containing_term_tsv = path.join( term_table_dir, 'containing_term.tsv' )

ontology_reference_root = path.join( 'auxiliary_metadata', '__ontology_reference' )
uberon_reference_dir = path.join( ontology_reference_root, 'UBERON' )
uberon_obo_file = path.join( uberon_reference_dir, 'uberon_ext.obo' )
icd_o_3_reference_dir = path.join( ontology_reference_root, 'ICD-O-3' )
icd_o_3_name_file = path.join( icd_o_3_reference_dir, 'icd_o_3.codes_and_preferred_names.tsv' )
do_reference_dir = path.join( ontology_reference_root, 'DO' )
do_obo_file = path.join( do_reference_dir, 'doid-merged.obo' )

null_values = [ '__CDA_UNASSIGNED__', 'null' ]

custom_concept_id_field_names = {
    '__DEFAULT': 'harmonized value',
    'anatomic_site': 'UBERON id',
    'disease': 'icd_o_3_code',
    'species': 'NCBI Taxonomy ID'
}

# EXECUTION

for target_subdir in [ output_dir, slim_dir, new_slim_dir, term_table_dir ]:
    if not path.exists( target_subdir ):
        makedirs( target_subdir )

# Load ontology reference data.

synonym_terms = dict()
related_terms = dict()
containing_terms = dict()

for term_set in { 'UBERON' }:
    synonym_terms[term_set] = dict()
    related_terms[term_set] = dict()
    containing_terms[term_set] = dict()

# UBERON.
uberon_terms = load_obo_file( uberon_obo_file )

uberon_id_to_name = dict()
uberon_name_to_id = dict()

foreign_id_to_name = dict()

# First, scan to match display names with IDs for later reverse lookup.
for uberon_id in uberon_terms:
    uberon_names = list( uberon_terms[uberon_id]['name'] )

    if len( uberon_names ) != 1:
        sys.exit( f"FATAL: UBERON term '{uberon_id}' has {len( uberon_names )} distinct values for 'name' -- please handle." )
    else:
        uberon_name = uberon_names[0]
        uberon_id_to_name[uberon_id] = uberon_name
        uberon_name_to_id[uberon_name] = uberon_id
        uberon_terms[uberon_id]['url'] = r'http://purl.obolibrary.org/obo/' + re.sub( r':', r'_', uberon_id )

# Now that we have name maps for the canonical UBERON terms, document all relationships and metadata.
for uberon_id in uberon_terms:
    uberon_name = uberon_id_to_name[uberon_id]

    # Don't load associations to non-Uberon things with identical names to Uberon terms. This
    # happens sometimes and clutters up our references: worse, available cross-references are
    # inconsistently coded and sometimes null, so it's not like we can reliably make new terms
    # for these things that are explicitly flagged as coming from non-UBERON data sources.
    # At least not without another layer of custom parsing/QC which will not effectively or evenly
    # cover the data space.
    # 
    # Ex.: See palatine tonsil [UBERON:0002373]: it has both
    #    synonym: "tonsil" BROAD [] # no UBERON xref
    # and
    #    is_a: UBERON:0002372 ! tonsil
    # ...leading to two distinct 'containing' entries where there should be just one.

    if 'is_a' in uberon_terms[uberon_id]:
        current_term_containers = sorted( uberon_terms[uberon_id]['is_a'] )

        for container_record in current_term_containers:
            # NB: Trailing whitespace on all values is removed in load_obo_file().
            match_result = re.search( r'^\s*(\S[^\!]*)\s+\!\s+(\S[^\!]*)$', container_record )

            if match_result is None:
                print( f"NON-FATAL WARNING: Malformed is_a record encountered in UBERON OBO metadata (doesn't match /<thing> ! <thing/): skipping. Offending record: \"{container_record}\"", file=sys.stderr )
            else:
                containing_id = match_result.group(1)
                # Remove unstructured xref comments attached to this ID by UBERON.
                containing_id = re.sub( r'\s+\{.*\}\s*$', r'', containing_id )
                containing_name = match_result.group(2)

                if uberon_id not in containing_terms['UBERON']:
                    containing_terms['UBERON'][uberon_id] = set()

                if containing_id in uberon_terms:
                    if containing_id != uberon_id:
                        containing_terms['UBERON'][uberon_id].add( containing_id )
                else:
                    # Bare names are not safe.
                    containing_name = json.dumps( containing_name ).strip( '"' )
                    # Don't load associations to non-Uberon things with identical names to Uberon terms.
                    if containing_name not in uberon_name_to_id:
                        containing_terms['UBERON'][uberon_id].add( containing_id )
                        if containing_id in foreign_id_to_name and foreign_id_to_name[containing_id] != containing_name:
                            sys.exit( f"FATAL: UBERON metadata: Clash on name for foreign ID '{containing_id}': {foreign_id_to_name[containing_id]} (old) vs {containing_name} (new): please resolve and handle." )
                        foreign_id_to_name[containing_id] = containing_name

    if 'synonym' in uberon_terms[uberon_id]:
        current_term_synonyms = sorted( uberon_terms[uberon_id]['synonym'] )

        for synonym_record in current_term_synonyms:
            match_result = re.search( r'^[^"]*"([^"]*)"\s+(\S+)\s+(\S.*)\s*$', synonym_record )

            if match_result is not None:
                synonym_name = match_result.group(1)
                synonym_type = match_result.group(2)
                synonym_record_suffix = match_result.group(3)
                suffix_match_result = re.search( r'^\[(UBERON:\d+)\]$', synonym_record_suffix )

                if suffix_match_result is not None:
                    # This synonym is another UBERON term.
                    synonym_uberon_id = suffix_match_result.group(1)
                    # print( f"{uberon_id} ({uberon_name}) --> {synonym_type} --> {synonym_uberon_id} ({synonym_name})", file=sys.stderr )
                    if synonym_type == 'EXACT':
                        if uberon_id not in synonym_terms['UBERON']:
                            synonym_terms['UBERON'][uberon_id] = set()
                        if synonym_uberon_id != uberon_id:
                            synonym_terms['UBERON'][uberon_id].add( synonym_uberon_id )
                        elif synonym_name != uberon_name:
                            # Bare non-canonical names are not safe.
                            synonym_name = json.dumps( synonym_name ).strip( '"' )
                            synonym_terms['UBERON'][uberon_id].add( synonym_name )
                    elif synonym_type == 'BROAD':
                        if uberon_id not in containing_terms['UBERON']:
                            containing_terms['UBERON'][uberon_id] = set()
                        if synonym_uberon_id != uberon_id:
                            containing_terms['UBERON'][uberon_id].add( synonym_uberon_id )
                        elif synonym_name != uberon_name:
                            # Bare non-canonical names are not safe.
                            synonym_name = json.dumps( synonym_name ).strip( '"' )
                            containing_terms['UBERON'][uberon_id].add( synonym_name )
                    elif synonym_type == 'RELATED':
                        if uberon_id not in related_terms['UBERON']:
                            related_terms['UBERON'][uberon_id] = set()
                        if synonym_uberon_id != uberon_id:
                            related_terms['UBERON'][uberon_id].add( synonym_uberon_id )
                        elif synonym_name != uberon_name:
                            # Bare non-canonical names are not safe.
                            synonym_name = json.dumps( synonym_name ).strip( '"' )
                            related_terms['UBERON'][uberon_id].add( synonym_name )
                    elif synonym_type not in { 'NARROW' }:
                        sys.exit( f"WARNING: Unexpected UBERON synonym type encountered: {synonym_type}; please investigate & handle." )
                else:
                    # Bare names are not safe.
                    synonym_name = json.dumps( synonym_name ).strip( '"' )
                    #print( f"{uberon_id} ({uberon_name}) --> {synonym_type} --> {synonym_name}", file=sys.stderr )
                    # Don't load associations to non-Uberon things with identical names to Uberon terms.
                    if synonym_name not in uberon_name_to_id:
                        if synonym_type == 'EXACT':
                            if uberon_id not in synonym_terms['UBERON']:
                                synonym_terms['UBERON'][uberon_id] = set()
                            synonym_terms['UBERON'][uberon_id].add( synonym_name )
                        elif synonym_type == 'BROAD':
                            if uberon_id not in containing_terms['UBERON']:
                                containing_terms['UBERON'][uberon_id] = set()
                            containing_terms['UBERON'][uberon_id].add( synonym_name )
                        elif synonym_type == 'RELATED':
                            if uberon_id not in related_terms['UBERON']:
                                related_terms['UBERON'][uberon_id] = set()
                            related_terms['UBERON'][uberon_id].add( synonym_name )
                        elif synonym_type not in { 'NARROW' }:
                            sys.exit( f"WARNING: Unexpected UBERON synonym type encountered: {synonym_type}; please investigate & handle." )

# ICD-O-3.
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

# Disease Ontology (DO).
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

# Harmonize (in memory) all non-null unharmonized concept-assigned column values and track what we encounter.
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
                        concept = columns_to_concepts[table_name][column_name]['concept_map_name']
                        print( f"{input_dir}: {table_name}.{column_name} <- {concept}" )
                    '''

                    with open( path.join( input_dir, input_sub ) ) as IN:
                        header = next( IN ).rstrip( '\n' )
                        column_names = header.split( '\t' )

                        for next_line in IN:
                            record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                            for column_name in record:
                                
                                if column_name in columns_to_concepts[table_name]:
                                    concept = columns_to_concepts[table_name][column_name]['concept_map_name']
                                    new_value = record[column_name]
                                    # Skip null values and universally-deleted values like 'Unknown' and 'Not reported'.

                                    if new_value is not None and new_value.strip() != '' and re.sub( r'\s', r'', new_value.strip().lower() ) not in delete_everywhere:
                                        # Ignore case and surrounding whitespace.
                                        new_value = new_value.strip().lower()

                                        if new_value == 'primary_diagnosis':
                                            print( 'FOR THE LOVE OF ALL THAT IS BEAUTIFUL AND GOOD, NOT AGAIN!', file=sys.stderr )

                                        if concept not in observed_values:
                                            observed_values[concept] = { new_value: 1 }
                                        elif new_value not in observed_values[concept]:
                                            observed_values[concept][new_value] = 1
                                        else:
                                            observed_values[concept][new_value] = observed_values[concept][new_value] + 1

# Mutation data is handled separately, downstream of DC processing.
if 'mutation' in columns_to_concepts:
    table_name = 'mutation'
    for column_name in sorted( columns_to_concepts[table_name] ):
        concept = columns_to_concepts[table_name][column_name]['concept_map_name']
        harmonization_log = path.join( 'auxiliary_metadata', '__harmonization_logs', table_name, f"{table_name}.{column_name}.substitution_log.tsv" )
        '''
        # Sanity check:
        print( f"{harmonization_log}: {table_name}.{column_name} <- {concept}" )
        '''
        with open( harmonization_log ) as IN:
            sub_log = load_tsv_as_dict( harmonization_log )
            for new_value in sub_log:
                # Skip null values and universally-deleted values like 'Unknown' and 'Not reported'.
                if new_value is not None and new_value.strip() != '' and re.sub( r'\s', r'', new_value.strip().lower() ) not in delete_everywhere:
                    new_value_count = int(sub_log[new_value]['number_of_substitutions'])
                    # Ignore case and surrounding whitespace.
                    new_value = new_value.strip().lower()

                    if concept not in observed_values:
                        observed_values[concept] = { new_value: new_value_count }
                    elif new_value not in observed_values[concept]:
                        observed_values[concept][new_value] = new_value_count
                    else:
                        observed_values[concept][new_value] = observed_values[concept][new_value] + new_value_count

observed_harmonized_terms = dict()
old_map = dict()
slim_map = dict()

for concept in sorted( observed_values ):
    slim_map_file = path.join( slim_dir, f"{concept}_slim.tsv" )
    target_concept_field = ''
    harmonized_term_header_to_load = ''
    slim_header_to_load = ''

    if path.exists( slim_map_file ):
        if concept not in slim_map:
            slim_map[concept] = dict()
        with open( slim_map_file ) as IN:
            header_list = next( IN ).rstrip( '\n' ).split( '\t' )
            # harmonized_disease_term_id	harmonized_disease_term_name	slim_disease_term_id	slim_disease_term_name	most_recent_count_in_CDA_data
            if concept in custom_concept_id_field_names:
                target_concept_field = custom_concept_id_field_names[concept]
            else:
                target_concept_field = custom_concept_id_field_names['__DEFAULT']
            harmonized_term_id_header = f"harmonized_{concept}_term_id"
            harmonized_term_name_header = f"harmonized_{concept}_term_name"
            slim_id_header = f"slim_{concept}_term_id"
            slim_name_header = f"slim_{concept}_term_name"
            for next_line in IN:
                current_record = dict( zip ( header_list, next_line.rstrip( '\n' ).split( '\t' ) ) )
                # Pick a thing to rely on: using the first record, if there's a non-null
                # harmonized_{concept}_term_id, use that; otherwise use harmonized_{concept}_term_name.
                if harmonized_term_header_to_load == '':
                    if current_record[harmonized_term_id_header] is not None and current_record[harmonized_term_id_header] != '':
                        harmonized_term_header_to_load = harmonized_term_id_header
                    elif current_record[harmonized_term_name_header] is not None and current_record[harmonized_term_name_header] != '':
                        harmonized_term_header_to_load = harmonized_term_name_header
                    else:
                        sys.exit( f"FATAL: Slim map {slim_map_file} contains null input columns; please fix." )
                harmonized_value = current_record[harmonized_term_header_to_load]

                # Pick a thing to rely on: using the first not-completely-null slim data,
                # if there's a non-null slim_{concept}_term_id, use that; otherwise use slim_{concept}_term_name.
                if slim_header_to_load == '':
                    if current_record[slim_id_header] is not None and current_record[slim_id_header] != '':
                        slim_header_to_load = slim_id_header
                    elif current_record[slim_name_header] is not None and current_record[slim_name_header] != '':
                        slim_header_to_load = slim_name_header

                if slim_header_to_load != '':
                    slim_value = current_record[slim_header_to_load]
                    if slim_value is not None and slim_value != '' and slim_value not in null_values:
                        if harmonized_value not in slim_map[concept]:
                            slim_map[concept][harmonized_value] = set()
                        slim_map[concept][harmonized_value].add( slim_value )

    harmonized_values_seen = set()
    slims_used = dict()
    slim_observation_count = dict()
    concept_display_name = dict()
    old_map_file = path.join( harmonization_root, f"{concept}.tsv" )
    old_map[concept] = dict()

    with open( old_map_file ) as IN:
        header = next( IN ).rstrip( '\n' )
        for next_line in IN:

            if concept == 'species':
                ( value, ncbi_tax_id, scientific_name, cda_common_name ) = next_line.rstrip( '\n' ).split( '\t' )
                lc_value = value.lower()
                target = dict()
                target['ncbi_tax_id'] = ncbi_tax_id
                target['scientific_name'] = scientific_name
                target['cda_common_name'] = cda_common_name

                if lc_value in old_map[concept]:
                    existing_dict = old_map[concept][lc_value]
                    for field_name in sorted( target ):
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )
                old_map[concept][lc_value] = target

            elif concept == 'disease':
                ( value, icd_o_3_code, icd_o_3_preferred_name, do_id, do_name, ncit_codes ) = next_line.rstrip( '\n' ).split( '\t' )
                lc_value = value.lower()
                target = dict()
                target['icd_o_3_code'] = icd_o_3_code
                target['icd_o_3_preferred_name'] = icd_o_3_preferred_name
                target['do_id'] = do_id
                target['do_name'] = do_name

                if lc_value in old_map[concept]:
                    existing_dict = old_map[concept][lc_value]
                    for field_name in sorted( target ):
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )

                # Once a match is confirmed or deemed irrelevant, update term names from the current ontology data.
                if icd_o_3_code not in null_values:
                    if icd_o_3_code not in icd_o_3_name:
                        print( f"WARNING: ICD-O-3 code '{icd_o_3_code}' not found in ICD-O-3 reference data. Leaving name as '{icd_o_3_preferred_name}' -- if this seems wrong, investigate.", file=sys.stderr )
                        icd_o_3_name[icd_o_3_code] = icd_o_3_preferred_name
                    target['icd_o_3_preferred_name'] = icd_o_3_name[icd_o_3_code]
                    concept_display_name[icd_o_3_code] = target['icd_o_3_preferred_name']

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
                old_map[concept][lc_value] = target

                # If we're slimming this concept and lc_value exists in the current data, record
                # the current (harmonized) term as seen; if there are slim values for this term,
                # log the map to those and increment observed (antecedent-aggregated) counts.
                if target_concept_field != '' and target_concept_field in target:
                    harmonized_value = target[target_concept_field]
                    if harmonized_value not in null_values and lc_value in observed_values[concept]:
                        harmonized_values_seen.add( harmonized_value )
                        concept_display_name[harmonized_value] = target['icd_o_3_preferred_name']
                        if harmonized_value in slim_map[concept]:
                            if harmonized_value not in slims_used:
                                slims_used[harmonized_value] = set()
                            for slim_value in sorted( slim_map[concept][harmonized_value] ):
                                slims_used[harmonized_value].add( slim_value )
                                if slim_value not in slim_observation_count:
                                    slim_observation_count[slim_value] = observed_values[concept][lc_value]
                                else:
                                    slim_observation_count[slim_value] = slim_observation_count[slim_value] + observed_values[concept][lc_value]
                        else:
                            if '' not in slim_observation_count:
                                slim_observation_count[''] = observed_values[concept][lc_value]
                            else:
                                slim_observation_count[''] = slim_observation_count[''] + observed_values[concept][lc_value]

            elif concept == 'anatomic_site':
                ( value, uberon_id, uberon_name ) = next_line.rstrip( '\n' ).split( '\t' )
                lc_value = value.lower()
                target = dict()
                target['UBERON id'] = uberon_id
                target['UBERON name'] = uberon_name

                if lc_value in old_map[concept]:
                    existing_dict = old_map[concept][lc_value]
                    for field_name in sorted( target ):
                        if field_name not in existing_dict or existing_dict[field_name] != target[field_name]:
                            print( f"YARRRGH! ({value}:{field_name}:{target[field_name]}) does not match loaded map data ({lc_value}:{field_name}:{existing_dict[field_name]})!!!", file=sys.stderr )

                # Once a match is confirmed or deemed irrelevant, update term names from the current ontology data.
                if uberon_id not in null_values:
                    if uberon_id not in uberon_id_to_name:
                        sys.exit( f"FATAL: UBERON term ID '{uberon_id}' not found in UBERON reference data. Please investigate." )
                    target['UBERON name'] = uberon_id_to_name[uberon_id]

                old_map[concept][lc_value] = target

                # If we're slimming this concept and lc_value exists in the current data, record
                # the current (harmonized) term as seen; if there are slim values for this term,
                # log the map to those and increment observed (antecedent-aggregated) counts.
                if target_concept_field != '' and target_concept_field in target:
                    harmonized_value = target[target_concept_field]
                    if harmonized_value not in null_values and lc_value in observed_values[concept]:
                        harmonized_values_seen.add( harmonized_value )
                        concept_display_name[harmonized_value] = target['UBERON name']
                        if harmonized_value in slim_map[concept]:
                            if harmonized_value not in slims_used:
                                slims_used[harmonized_value] = set()
                            for slim_value in sorted( slim_map[concept][harmonized_value] ):
                                slims_used[harmonized_value].add( slim_value )
                                concept_display_name[slim_value] = uberon_id_to_name[slim_value]
                                if slim_value not in slim_observation_count:
                                    slim_observation_count[slim_value] = observed_values[concept][lc_value]
                                else:
                                    slim_observation_count[slim_value] = slim_observation_count[slim_value] + observed_values[concept][lc_value]
                        else:
                            if '' not in slim_observation_count:
                                slim_observation_count[''] = observed_values[concept][lc_value]
                            else:
                                slim_observation_count[''] = slim_observation_count[''] + observed_values[concept][lc_value]

            else:
                ( value, target ) = next_line.rstrip( '\n' ).split( '\t' )
                lc_value = value.lower()

                if lc_value in old_map[concept] and old_map[concept][lc_value] != target:
                    print( f"YARRRGH! ({value}:{target}) does not match loaded map data ({lc_value}:{old_map[concept][lc_value]})!!!", file=sys.stderr )
                old_map[concept][lc_value] = target

    # Make sure we've assigned a concept_display_name value to everything that will need one.
    if concept == 'anatomic_site':
        for harmonized_value in sorted( slim_map[concept].keys() ):
            concept_display_name[harmonized_value] = uberon_id_to_name[harmonized_value]
            for slim_value in sorted( slim_map[concept][harmonized_value] ):
                concept_display_name[slim_value] = uberon_id_to_name[slim_value]
    elif concept == 'disease':
        for harmonized_value in sorted( slim_map[concept].keys() ):
            concept_display_name[harmonized_value] = icd_o_3_name[harmonized_value]
            # (Slim values here are bare strings: no map needed.)

    # If we're slimming this concept, update the slim map with new (harmonized) input values seen.
    if target_concept_field != '':
        new_slim_map_file = path.join( new_slim_dir, f"{concept}_slim.tsv" )

        with open( new_slim_map_file, 'w' ) as OUT:
            harmonized_term_id_header = f"harmonized_{concept}_term_id"
            harmonized_term_name_header = f"harmonized_{concept}_term_name"
            slim_id_header = f"slim_{concept}_term_id"
            slim_name_header = f"slim_{concept}_term_name"
            print( *[ harmonized_term_id_header, harmonized_term_name_header, slim_id_header, slim_name_header, 'most_recent_count_in_CDA_data' ], sep='\t', file=OUT )

            for harmonized_value in sorted( harmonized_values_seen | slim_map[concept].keys() ):
                if harmonized_value in slim_map[concept]:
                    for target_value in sorted( slim_map[concept][harmonized_value] ):
                        target_count = 0
                        if target_value in slim_observation_count:
                            target_count = slim_observation_count[target_value]
                        harmonized_id = ''
                        harmonized_name = ''
                        if harmonized_term_header_to_load == harmonized_term_id_header:
                            harmonized_id = harmonized_value
                            harmonized_name = concept_display_name[harmonized_value]
                        elif harmonized_term_header_to_load == harmonized_term_name_header:
                            harmonized_id = ''
                            harmonized_name = harmonized_value
                        else:
                            sys.exit( f"FATAL: Something (1) has gone horribly wrong processing harmonized id/name key configuration for '{concept}'; please fix." )
                        slim_id = ''
                        slim_name = ''
                        if slim_header_to_load == slim_id_header:
                            slim_id = target_value
                            slim_name = concept_display_name[target_value]
                        elif slim_header_to_load == slim_name_header:
                            slim_id = ''
                            slim_name = target_value
                        else:
                            sys.exit( f"FATAL: Something has gone horribly wrong processing slim id/name key configuration for '{concept}'; please fix." )
                        print( *[ harmonized_id, harmonized_name, slim_id, slim_name, target_count ], sep='\t', file=OUT )
                else:
                    target_count = 0
                    if '' in slim_observation_count:
                        target_count = slim_observation_count['']
                    harmonized_id = ''
                    harmonized_name = ''
                    if harmonized_term_header_to_load == harmonized_term_id_header:
                        harmonized_id = harmonized_value
                        harmonized_name = concept_display_name[harmonized_value]
                    elif harmonized_term_header_to_load == harmonized_term_name_header:
                        harmonized_id = ''
                        harmonized_name = harmonized_value
                    else:
                        sys.exit( f"FATAL: Something (2) has gone horribly wrong processing harmonized id/name key configuration for '{concept}'; please fix." )
                    print( *[ harmonized_id, harmonized_name, '', '', target_count ], sep='\t', file=OUT )

    # Make an updated harmonization map for `concept` including all newly observed values.
    output_file = path.join( output_dir, f"{concept}.tsv" )

    with open( output_file, 'w' ) as OUT:
        
        if concept == 'anatomic_site':
            print( *[ 'unharmonized value', 'UBERON id', 'UBERON name' ], sep='\t', file=OUT )
        elif concept == 'species':
            print( *[ 'unharmonized value', 'NCBI Taxonomy ID', 'scientific name', 'CDA curated common name' ], sep='\t', file=OUT )
        elif concept == 'disease':
            print( *[ 'unharmonized value', 'icd_o_3_code', 'icd_o_3_preferred_name', 'do_id', 'do_name', 'ncit_concept_codes' ], sep='\t', file=OUT )
        else:
            print( *[ 'unharmonized value', 'harmonized value' ], sep='\t', file=OUT )

        for observed_value in sorted( set( sorted( old_map[concept] ) + sorted( observed_values[concept] ) ) ):
            # Weed legacy delete-everywhere values from existing maps.
            if observed_value is not None and observed_value.strip() != '' and re.sub( r'\s', r'', observed_value.strip().lower() ) not in delete_everywhere:
                target_value = '__CDA_UNASSIGNED__'
                printed = False

                if observed_value in old_map[concept]:
                    # Track harmonized values associated with observed inputs.
                    if concept not in observed_harmonized_terms:
                        observed_harmonized_terms[concept] = set()

                    if concept == 'species':
                        target_dict = old_map[concept][observed_value]
                        print( *[ observed_value, target_dict['ncbi_tax_id'], target_dict['scientific_name'], target_dict['cda_common_name'] ], sep='\t', file=OUT )
                        printed = True
                        # Track harmonized values associated with observed inputs.
                        if target_dict['ncbi_tax_id'] not in null_values and re.sub( r'\s', r'', target_dict['ncbi_tax_id'].strip().lower() ) not in delete_everywhere:
                            observed_harmonized_terms[concept].add( target_dict['ncbi_tax_id'] )
                    elif concept == 'disease':
                        target_dict = old_map[concept][observed_value]
                        ncit_codes = '__CDA_UNASSIGNED__'

                        if target_dict['icd_o_3_code'] == '__CDA_UNASSIGNED__':
                            # We've seen this unharmonized value before, but it hasn't yet been connected
                            # with an ICD-O-3 term. Is this value itself an ICD-O-3 term (code) we know about?
                            # If so, log what we know.
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

                                # NB: The following is modifying old_map['disease'][observed_value], not just some local variable. Intended here,
                                # but worth drawing attention to.
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
                        # Track harmonized values associated with observed inputs.
                        if target_dict['icd_o_3_code'] not in null_values and re.sub( r'\s', r'', target_dict['icd_o_3_code'].strip().lower() ) not in delete_everywhere:
                            observed_harmonized_terms[concept].add( target_dict['icd_o_3_code'] )

                    elif concept == 'anatomic_site':
                        target_dict = old_map[concept][observed_value]
                        print( *[ observed_value, target_dict['UBERON id'], target_dict['UBERON name'] ], sep='\t', file=OUT )
                        printed = True
                        # Track harmonized values associated with observed inputs.
                        if re.sub( r'\s', r'', target_dict['UBERON id'].strip().lower() ) not in delete_everywhere:
                            observed_harmonized_terms[concept].add( target_dict['UBERON id'] )

                    else:
                        target_value = old_map[concept][observed_value]
                        # Track harmonized values associated with observed inputs.
                        if target_value not in null_values:
                            observed_harmonized_terms[concept].add( target_value )

                elif concept == 'anatomic_site':
                    # Newly-seen term, not mapped yet. Add with null harmonization targets.
                    print( *[ observed_value, target_value, target_value ], sep='\t', file=OUT )
                    printed = True

                elif concept == 'species':
                    # Newly-seen term, not mapped yet. Add with null harmonization targets.
                    print( *[ observed_value, target_value, target_value, target_value ], sep='\t', file=OUT )
                    printed = True

                elif concept == 'disease':
                    # Newly-seen term, not mapped yet. Add with null harmonization targets unless we know its
                    # name and also possibly its DO xrefs, in which case add what we know.
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

                        # Update the in-memory harmonization map.
                        new_entry = dict()
                        new_entry['icd_o_3_code'] = icd_o_3_code
                        new_entry['icd_o_3_preferred_name'] = icd_o_3_preferred_name
                        new_entry['do_id'] = do_id
                        new_entry['do_name'] = do_name
                        old_map[concept][observed_value] = new_entry
                        # Track harmonized values associated with observed inputs.
                        if concept not in observed_harmonized_terms:
                            observed_harmonized_terms[concept] = set()
                        observed_harmonized_terms[concept].add( icd_o_3_code )
                    else:
                        print( *[ observed_value, target_value, target_value, target_value, target_value, target_value ], sep='\t', file=OUT )
                    printed = True

                if not printed:
                    print( *[ observed_value, target_value], sep='\t', file=OUT )

    if concept == 'disease':
        # Update old_map['disease'] to ensure lookup by ICD-O-3 code is always possible (instead of just lookup by observed value, which only covers some of the codes used).
        records_to_add = dict()
        for observed_term in old_map[concept]:
            icd_o_3_code = old_map[concept][observed_term]['icd_o_3_code']
            if icd_o_3_code not in null_values and icd_o_3_code not in old_map[concept] and icd_o_3_code not in records_to_add:
                records_to_add[icd_o_3_code] = {
                    'icd_o_3_code': old_map[concept][observed_term]['icd_o_3_code'],
                    'icd_o_3_preferred_name': old_map[concept][observed_term]['icd_o_3_preferred_name'],
                    'do_id': old_map[concept][observed_term]['do_id'],
                    'do_name': old_map[concept][observed_term]['do_name']
                }

        for icd_o_3_code in records_to_add:
            old_map[concept][icd_o_3_code] = records_to_add[icd_o_3_code]

    elif concept == 'species':
        # Update old_map['species'] to ensure lookup by NCBI Taxonomy ID is always possible (instead of just lookup by observed value).
        records_to_add = dict()
        for observed_term in old_map[concept]:
            ncbi_tax_id = old_map[concept][observed_term]['ncbi_tax_id']
            if ncbi_tax_id not in null_values and ncbi_tax_id not in old_map[concept] and ncbi_tax_id not in records_to_add:
                records_to_add[ncbi_tax_id] = {
                    'ncbi_tax_id': old_map[concept][observed_term]['ncbi_tax_id'],
                    'scientific_name': old_map[concept][observed_term]['scientific_name'],
                    'cda_common_name': old_map[concept][observed_term]['cda_common_name']
                }

        for ncbi_tax_id in records_to_add:
            old_map[concept][ncbi_tax_id] = records_to_add[ncbi_tax_id]

controlled_term_columns = [ 'id_alias', 'id', 'name', 'url', 'definition', 'data_source', 'concept' ]

# Make the controlled_term table and populate its associated relationships.
with open( controlled_term_tsv, 'w' ) as CONTROLLED_TERM, \
    open( synonym_term_tsv, 'w' ) as SYNONYM_TERM, \
    open( slim_term_tsv, 'w' ) as SLIM_TERM, \
    open( related_term_tsv, 'w' ) as RELATED_TERM, \
    open( containing_term_tsv, 'w' ) as CONTAINING_TERM:

    next_term_alias = 0

    print( *controlled_term_columns, sep='\t', file=CONTROLLED_TERM )
    print( *[ 'synonym_one_alias', 'synonym_two_alias' ], sep='\t', file=SYNONYM_TERM )
    print( *[ 'general_term_alias', 'specific_term_alias' ], sep='\t', file=SLIM_TERM )
    print( *[ 'related_term_one_alias', 'related_term_two_alias' ], sep='\t', file=RELATED_TERM )
    print( *[ 'general_term_alias', 'specific_term_alias' ], sep='\t', file=CONTAINING_TERM )

    for concept in sorted( observed_harmonized_terms ):
        
        if concept == 'species':
            alias_of_term = dict()

            # By construction, these are always NCBI Taxonomy IDs.
            for observed_term in sorted( observed_harmonized_terms[concept] ):
                
                if observed_term not in alias_of_term:
                    observed_alias = next_term_alias
                    next_term_alias = next_term_alias + 1
                    alias_of_term[observed_term] = observed_alias
                    new_record = {
                        'id_alias': observed_alias,
                        'id': observed_term,
                        'name': old_map[concept][observed_term]['cda_common_name'],
                        'url': f"https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?mode=info&id={observed_term}",
                        'definition': old_map[concept][observed_term]['scientific_name'],
                        'data_source': 'NCBI',
                        'concept': concept
                    }

                    print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

        elif concept == 'disease':
            alias_of_term = dict()
            ncit_codes_seen = set()
            ncit_synonyms_printed = dict()

            # By construction, these are always ICD-O-3 codes.
            for observed_term in sorted( observed_harmonized_terms[concept] ):
                
                if observed_term not in alias_of_term:
                    observed_alias = next_term_alias
                    next_term_alias = next_term_alias + 1
                    alias_of_term[observed_term] = observed_alias
                    new_record = {
                        'id_alias': observed_alias,
                        'id': observed_term,
                        'name': old_map[concept][observed_term]['icd_o_3_preferred_name'],
                        'url': '',
                        'definition': '',
                        'data_source': 'ICD-O-3',
                        'concept': concept
                    }
                    print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                # If one or more non-null slim values are given for this term, log those in the slim map, minting new
                # controlled terms (in this case, bare display-name strings) as needed.
                if observed_term in slim_map[concept]:
                    observed_alias = alias_of_term[observed_term]
                    for slim_value in sorted( slim_map[concept][observed_term] ):
                        if slim_value not in alias_of_term:
                            alias_of_term[slim_value] = next_term_alias
                            next_term_alias = next_term_alias + 1
                            new_record = {
                                'id_alias': alias_of_term[slim_value],
                                'id': '',
                                'name': slim_value,
                                'url': '',
                                'definition': '',
                                'data_source': 'CDA',
                                'concept': concept
                            }
                            print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )
                        slim_alias = alias_of_term[slim_value]
                        print( *[ slim_alias, observed_alias ], sep='\t', file=SLIM_TERM )

                # If we have DO xrefs, add those and any associated NCIt codes as synonyms.
                if old_map[concept][observed_term]['do_id'] not in null_values:
                    do_id = old_map[concept][observed_term]['do_id']
                    do_name = old_map[concept][observed_term]['do_name']

                    if do_id not in alias_of_term:
                        do_alias = next_term_alias
                        next_term_alias = next_term_alias + 1
                        alias_of_term[do_id] = do_alias
                        do_record = {
                            'id_alias': do_alias,
                            'id': do_id,
                            'name': do_name,
                            'url': f"http://purl.obolibrary.org/obo/{re.sub( r'DOID:', r'DOID_', do_id )}",
                            'definition': '',
                            'data_source': 'DO',
                            'concept': concept
                        }
                        print( *[ do_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                        if do_id in do_id_to_ncit_code:   
                            if observed_term not in ncit_synonyms_printed:
                                ncit_synonyms_printed[observed_term] = set()

                            for ncit_code in sorted( do_id_to_ncit_code[do_id] ):
                                if ncit_code not in ncit_codes_seen:
                                    ncit_alias = next_term_alias
                                    next_term_alias = next_term_alias + 1
                                    alias_of_term[ncit_code] = ncit_alias
                                    ncit_record = {
                                        'id_alias': ncit_alias,
                                        'id': ncit_code,
                                        'name': '',
                                        'url': f"http://purl.obolibrary.org/obo/NCIT_{ncit_code}",
                                        'definition': '',
                                        'data_source': 'NCIt',
                                        'concept': concept
                                    }
                                    print( *[ ncit_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )
                                    ncit_codes_seen.add( ncit_code )

                                if ncit_code not in ncit_synonyms_printed[observed_term]:
                                    # Thought briefly about hooking this up as DO term -> synonym -> NCIt term, then thought about transitive synonym resolution, then backed away from a non-flat model in a hurry.
                                    print( *[ alias_of_term[observed_term], alias_of_term[ncit_code] ], sep='\t', file=SYNONYM_TERM )
                                    ncit_synonyms_printed[observed_term].add( ncit_code )

                    print( *[ alias_of_term[observed_term], alias_of_term[do_id] ], sep='\t', file=SYNONYM_TERM )

        elif concept == 'anatomic_site':
            alias_of_term = dict()

            # By construction, these are always UBERON IDs.
            for observed_term in sorted( observed_harmonized_terms[concept] ):
                
                if observed_term not in alias_of_term:
                    observed_alias = next_term_alias
                    next_term_alias = next_term_alias + 1
                    alias_of_term[observed_term] = observed_alias
                    new_record = {
                        'id_alias': observed_alias,
                        'id': observed_term,
                        'name': uberon_id_to_name[observed_term],
                        'url': uberon_terms[observed_term]['url'],
                        'definition': '',
                        'data_source': 'UBERON',
                        'concept': concept
                    }

                    print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                # If one or more non-null slim values are given for this term, log those in the slim map, minting new
                # controlled terms (in this case, UBERON IDs) as needed.
                if observed_term in slim_map[concept]:
                    observed_alias = alias_of_term[observed_term]
                    for slim_value in sorted( slim_map[concept][observed_term] ):
                        if slim_value not in alias_of_term:
                            alias_of_term[slim_value] = next_term_alias
                            next_term_alias = next_term_alias + 1
                            new_record = {
                                'id_alias': alias_of_term[slim_value],
                                'id': slim_value,
                                'name': uberon_id_to_name[slim_value],
                                'url': uberon_terms[slim_value]['url'],
                                'definition': '',
                                'data_source': 'UBERON',
                                'concept': concept
                            }
                            print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )
                        slim_alias = alias_of_term[slim_value]
                        print( *[ slim_alias, observed_alias ], sep='\t', file=SLIM_TERM )

                if observed_term in synonym_terms['UBERON']:
                    synonym_list = sorted( synonym_terms['UBERON'][observed_term] )

                    for synonym_term in synonym_list:
                        
                        if synonym_term not in alias_of_term:
                            synonym_alias = next_term_alias
                            next_term_alias = next_term_alias + 1
                            alias_of_term[synonym_term] = synonym_alias

                            synonym_record = dict()

                            if synonym_term in uberon_terms:
                                synonym_record = {
                                    'id_alias': synonym_alias,
                                    'id': synonym_term,
                                    'name': uberon_id_to_name[synonym_term],
                                    'url': uberon_terms[synonym_term]['url'],
                                    'definition': '',
                                    'data_source': 'UBERON',
                                    'concept': concept
                                }

                            else:
                                synonym_record = {
                                    'id_alias': synonym_alias,
                                    'id': '',
                                    'name': synonym_term,
                                    'url': '',
                                    'definition': '',
                                    'data_source': '',
                                    'concept': concept
                                }

                            print( *[ synonym_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                        print( *[ alias_of_term[observed_term], alias_of_term[synonym_term] ], sep='\t', file=SYNONYM_TERM )

                if observed_term in related_terms['UBERON']:
                    related_list = sorted( related_terms['UBERON'][observed_term] )

                    for related_term in related_list:
                        
                        if related_term not in alias_of_term:
                            related_alias = next_term_alias
                            next_term_alias = next_term_alias + 1
                            alias_of_term[related_term] = related_alias

                            related_record = dict()

                            if related_term in uberon_terms:
                                related_record = {
                                    'id_alias': related_alias,
                                    'id': related_term,
                                    'name': uberon_id_to_name[related_term],
                                    'url': uberon_terms[related_term]['url'],
                                    'definition': '',
                                    'data_source': 'UBERON',
                                    'concept': concept
                                }

                            else:
                                related_record = {
                                    'id_alias': related_alias,
                                    'id': '',
                                    'name': related_term,
                                    'url': '',
                                    'definition': '',
                                    'data_source': '',
                                    'concept': concept
                                }

                            print( *[ related_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                        print( *[ alias_of_term[observed_term], alias_of_term[related_term] ], sep='\t', file=RELATED_TERM )

                if observed_term in containing_terms['UBERON']:
                    containing_list = sorted( containing_terms['UBERON'][observed_term] )

                    for containing_term in containing_list:
                        
                        if containing_term not in alias_of_term:
                            containing_alias = next_term_alias
                            next_term_alias = next_term_alias + 1
                            alias_of_term[containing_term] = containing_alias

                            containing_record = dict()

                            if containing_term in uberon_terms:
                                containing_record = {
                                    'id_alias': containing_alias,
                                    'id': containing_term,
                                    'name': uberon_id_to_name[containing_term],
                                    'url': uberon_terms[containing_term]['url'],
                                    'definition': '',
                                    'data_source': 'UBERON',
                                    'concept': concept
                                }

                            elif containing_term in foreign_id_to_name:
                                containing_record = {
                                    'id_alias': containing_alias,
                                    'id': containing_term,
                                    'name': foreign_id_to_name[containing_term],
                                    'url': '',
                                    'definition': '',
                                    'data_source': '',
                                    'concept': concept
                                }

                            else:
                                containing_record = {
                                    'id_alias': containing_alias,
                                    'id': '',
                                    'name': containing_term,
                                    'url': '',
                                    'definition': '',
                                    'data_source': '',
                                    'concept': concept
                                }

                            print( *[ containing_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )

                        print( *[ alias_of_term[containing_term], alias_of_term[observed_term] ], sep='\t', file=CONTAINING_TERM )

        else:
            alias_of_term = dict()

            for observed_term in sorted( observed_harmonized_terms[concept] ):
                
                if observed_term not in alias_of_term:
                    observed_alias = next_term_alias
                    next_term_alias = next_term_alias + 1
                    alias_of_term[observed_term] = observed_alias
                    new_record = {
                        'id_alias': observed_alias,
                        'id': '',
                        'name': observed_term,
                        'url': '',
                        'definition': '',
                        'data_source': 'CDA',
                        'concept': concept
                    }

                    print( *[ new_record[column_name] for column_name in controlled_term_columns ], sep='\t', file=CONTROLLED_TERM )



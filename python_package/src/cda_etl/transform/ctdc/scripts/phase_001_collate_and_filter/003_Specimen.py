#!/usr/bin/env python -u

import sys

from os import path, makedirs

from cda_etl.lib import load_tsv_as_dict

# PARAMETERS

input_root = path.join( 'extracted_data', 'ctdc' )

# We need to start with this one. It contains a strict superset of Specimen data.
clinical_specimen_input_dir = path.join( input_root, 'ClinicalSpecimen' )
clinical_specimen_input_tsv = path.join( clinical_specimen_input_dir, 'ClinicalSpecimen.tsv' )

specimen_input_dir = path.join( input_root, 'Specimen' )
specimen_input_tsvs = [
    path.join( specimen_input_dir, 'Specimen.from_StudySpecimenByStudyShortName.tsv' ),
    path.join( specimen_input_dir, 'Specimen.from_getAllStudies.tsv' )
]

biospecimen_overview_input_dir = path.join( input_root, 'BiospecimenOverview' )
biospecimen_overview_input_tsv = path.join( biospecimen_overview_input_dir, 'BiospecimenOverview.tsv' )

output_root = path.join( 'extracted_data', 'ctdc_postprocessed' )

# I am unilaterally deciding that we're going to remove the "Clinical" prefix from all collated objects, merging parallel pairs where they exist. Object if you will.
specimen_output_dir = path.join( output_root, 'Specimen' )
specimen_output_tsv = path.join( specimen_output_dir, 'Specimen.tsv' )

# EXECUTION

for output_dir in [ output_root, specimen_output_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

specimen = dict()
specimen_columns = list()

################################################################################
# Collate all ClinicalSpecimen records to start. This entity contains a strict superset of the data offered by the Specimen entity.
clinical_specimen = load_tsv_as_dict( clinical_specimen_input_tsv )

# ASSUMPTION:
#     * First column is primary key (in this case specimen_record_id)
for specimen_record_id in clinical_specimen.keys():
    if specimen_record_id not in specimen:
        specimen[specimen_record_id] = dict()
    # Cache column names for later.
    if len( specimen_columns ) == 0:
        # ASSUMPTION: Python preserves dict insert order.
        specimen_columns = list( clinical_specimen[specimen_record_id].keys() ).copy()
        # This needs to go.
        specimen_columns.remove( 'participant_ids' )
    for column_name in specimen_columns:
        specimen[specimen_record_id][column_name] = clinical_specimen[specimen_record_id][column_name]

# Verify with other data sources and report conflicts.
for input_map_file in [ map_file for sub_list in [ specimen_input_tsvs, [ biospecimen_overview_input_tsv ] ] for map_file in sub_list ]:
    # ASSUMPTION: All of these files have a first-column primary key called 'specimen_record_id'.
    current_table = load_tsv_as_dict( input_map_file )
    for specimen_record_id in current_table:
        for specimen_column in specimen_columns:
            # Ignore nulls. Ignore case... there are differences. (*EYEROLL*) Complain about any other clashes. Break with a KeyError if an unloaded Specimen is encountered.
            if specimen_column in current_table[specimen_record_id] and current_table[specimen_record_id][specimen_column] is not None and current_table[specimen_record_id][specimen_column] != '' and current_table[specimen_record_id][specimen_column].lower() != specimen[specimen_record_id][specimen_column].lower():
                sys.exit( f"FATAL: Specimen mismatch in {input_map_file}: already loaded {specimen_record_id}->{specimen_column} == {specimen[specimen_record_id][specimen_column]}; now seeing '{current_table[specimen_record_id][specimen_column]}'" )

# Write aggregated Specimen records to output TSV.
with open( specimen_output_tsv, 'w' ) as OUT:
    print( *specimen_columns, sep='\t', end='\n', file=OUT )
    for specimen_record_id in sorted( specimen ):
        specimen_row = list()
        for column_name in specimen_columns:
            specimen_row.append( specimen[specimen_record_id][column_name] )
        print( *specimen_row, sep='\t', end='\n', file=OUT )



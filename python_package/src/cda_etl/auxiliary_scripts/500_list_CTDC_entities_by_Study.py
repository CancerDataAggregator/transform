#!/usr/bin/env python -u

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'ctdc_postprocessed' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )
specimen_input_tsv = path.join( input_root, 'Specimen', 'Specimen.tsv' )
study_participant_input_tsv = path.join( input_root, 'Study', 'Study.participant_id.tsv' )
participant_specimen_input_tsv = path.join( input_root, 'Participant', 'Participant.specimen_record_id.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__CTDC_supplemental_metadata' )
entity_output_file = path.join( output_dir, 'CTDC_entities_by_Study.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    makedirs( output_dir )

study = load_tsv_as_dict( study_input_tsv )
specimen = load_tsv_as_dict( specimen_input_tsv )
study_participant = map_columns_one_to_many( study_participant_input_tsv, 'study_id', 'participant_id' )
participant_specimen = map_columns_one_to_many( participant_specimen_input_tsv, 'participant_id', 'specimen_record_id' )

with open( entity_output_file, 'w' ) as OUT:
    print( *[ 'Study.study_id', 'Study.study_short_name', 'Study.study_accession', 'entity_id', 'entity_type' ], sep='\t', file=OUT )

    for study_id in sorted( study_participant ):
        for participant_id in sorted( study_participant[study_id] ):
            print( *[ study[study_id]['study_id'], study[study_id]['study_short_name'], study[study_id]['study_accession'], participant_id, 'participant' ], sep='\t', file=OUT )
            if participant_id in participant_specimen:
                for specimen_id in sorted( participant_specimen[participant_id] ):
                    print( *[ study[study_id]['study_id'], study[study_id]['study_short_name'], study[study_id]['study_accession'], specimen_id, 'specimen' ], sep='\t', file=OUT )



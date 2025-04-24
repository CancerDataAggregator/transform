#!/usr/bin/env python -u

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'cds' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis.tsv' )

participant_input_tsv = path.join( input_root, 'participant.tsv' )

program_input_tsv = path.join( input_root, 'program.tsv' )

sample_input_tsv = path.join( input_root, 'sample.tsv' )

study_input_tsv = path.join( input_root, 'study.tsv' )

diagnosis_of_participant_input_tsv = path.join( input_root, 'diagnosis_of_participant.tsv' )

participant_in_study_input_tsv = path.join( input_root, 'participant_in_study.tsv' )

sample_from_participant_input_tsv = path.join( input_root, 'sample_from_participant.tsv' )

study_in_program_input_tsv = path.join( input_root, 'study_in_program.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__CDS_supplemental_metadata' )

entity_output_file = path.join( output_dir, 'CDS_entities_by_program_and_study.tsv' )

project_output_file = path.join( output_dir, 'CDS_all_programs_and_studies.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

participant = load_tsv_as_dict( participant_input_tsv )

program = load_tsv_as_dict( program_input_tsv )

sample = load_tsv_as_dict( sample_input_tsv )

study = load_tsv_as_dict( study_input_tsv )

diagnosis_of_participant = map_columns_one_to_one( diagnosis_of_participant_input_tsv, 'diagnosis_uuid', 'participant_uuid' )

participant_in_study = map_columns_one_to_many( participant_in_study_input_tsv, 'participant_uuid', 'study_uuid' )

# This is one-to-one at time of writing (August 2024), but it's easy to generalize, so we do.

sample_from_participant = map_columns_one_to_many( sample_from_participant_input_tsv, 'sample_uuid', 'participant_uuid' )

study_in_program = map_columns_one_to_one( study_in_program_input_tsv, 'study_uuid', 'program_uuid' )

with open( project_output_file, 'w' ) as OUT:
    
    print( *[ 'program.uuid', 'program.program_acronym', 'program.program_name', 'study.uuid', 'study.phs_accession', 'study.study_acronym', 'study.study_name' ], sep='\t', file=OUT )

    printed_studies = set()

    for program_uuid in sorted( program ):
        
        for study_uuid in sorted( study_in_program ):
            
            if program_uuid in study_in_program[study_uuid]:
                
                program_acronym = program[program_uuid]['program_acronym']

                program_name = program[program_uuid]['program_name']

                study_phs_accession = study[study_uuid]['phs_accession']

                study_acronym = study[study_uuid]['study_acronym']

                study_name = study[study_uuid]['study_name']

                print( *[ program_uuid, program_acronym, program_name, study_uuid, study_phs_accession, study_acronym, study_name ], sep='\t', file=OUT )

                printed_studies.add( study_uuid )

    # As of March 2025, not all CDS studies have to be in programs.

    for study_uuid in sorted( study ):
        
        if study_uuid not in printed_studies:
            
            study_phs_accession = study[study_uuid]['phs_accession']

            study_acronym = study[study_uuid]['study_acronym']

            study_name = study[study_uuid]['study_name']

            print( *[ '', '', '', study_uuid, study_phs_accession, study_acronym, study_name ], sep='\t', file=OUT )

with open( entity_output_file, 'w' ) as OUT:
    
    print( *[ 'program.uuid', 'program.program_acronym', 'program.program_name', 'study.uuid', 'study.phs_accession', 'study.study_acronym', 'study.study_name', 'entity_submitter_id', 'entity_id', 'entity_type' ], sep='\t', file=OUT )

    for participant_uuid in sorted( participant ):
        
        participant_id = participant[participant_uuid]['participant_id']

        for study_uuid in sorted( participant_in_study[participant_uuid] ):
            
            # As of March 2025, not all CDS studies have to be in programs.

            program_uuid = ''

            program_acronym = ''

            program_name = ''

            if study_uuid in study_in_program:
                
                program_uuid = study_in_program[study_uuid]

                program_acronym = program[program_uuid]['program_acronym']

                program_name = program[program_uuid]['program_name']

            study_phs_accession = study[study_uuid]['phs_accession']

            study_acronym = study[study_uuid]['study_acronym']

            study_name = study[study_uuid]['study_name']

            print( *[ program_uuid, program_acronym, program_name, study_uuid, study_phs_accession, study_acronym, study_name, participant_id, participant_uuid, 'participant' ], sep='\t', file=OUT )

    for diagnosis_uuid in sorted( diagnosis ):
        
        diagnosis_id = diagnosis[diagnosis_uuid]['diagnosis_id']

        participant_uuid = diagnosis_of_participant[diagnosis_uuid]

        for study_uuid in sorted( participant_in_study[participant_uuid] ):
            
            # As of March 2025, not all CDS studies have to be in programs.

            program_uuid = ''

            program_acronym = ''

            program_name = ''

            if study_uuid in study_in_program:
                
                program_uuid = study_in_program[study_uuid]

                program_acronym = program[program_uuid]['program_acronym']

                program_name = program[program_uuid]['program_name']

            study_phs_accession = study[study_uuid]['phs_accession']

            study_acronym = study[study_uuid]['study_acronym']

            study_name = study[study_uuid]['study_name']

            print( *[ program_uuid, program_acronym, program_name, study_uuid, study_phs_accession, study_acronym, study_name, diagnosis_id, diagnosis_uuid, 'diagnosis' ], sep='\t', file=OUT )

    for sample_uuid in sorted( sample ):
        
        sample_id = sample[sample_uuid]['sample_id']

        for participant_uuid in sorted( sample_from_participant[sample_uuid] ):
            
            participant_id = participant[participant_uuid]['participant_id']

            for study_uuid in sorted( participant_in_study[participant_uuid] ):
                
                # As of March 2025, not all CDS studies have to be in programs.

                program_uuid = ''

                program_acronym = ''

                program_name = ''

                if study_uuid in study_in_program:
                    
                    program_uuid = study_in_program[study_uuid]

                    program_acronym = program[program_uuid]['program_acronym']

                    program_name = program[program_uuid]['program_name']

                study_phs_accession = study[study_uuid]['phs_accession']

                study_acronym = study[study_uuid]['study_acronym']

                study_name = study[study_uuid]['study_name']

                print( *[ program_uuid, program_acronym, program_name, study_uuid, study_phs_accession, study_acronym, study_name, sample_id, sample_uuid, 'sample' ], sep='\t', file=OUT )



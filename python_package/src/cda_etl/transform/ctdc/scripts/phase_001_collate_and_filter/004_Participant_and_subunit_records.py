#!/usr/bin/env python -u

import shutil
import sys

from os import path, makedirs

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many
# def map_columns_one_to_many( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):

# PARAMETERS

input_root = path.join( 'extracted_data', 'ctdc' )

biospecimen_overview_input_dir = path.join( input_root, 'BiospecimenOverview' )
biospecimen_overview_input_tsv = path.join( biospecimen_overview_input_dir, 'BiospecimenOverview.tsv' )

clinical_demographic_input_dir = path.join( input_root, 'ClinicalDemographic' )
clinical_demographic_input_tsv = path.join( clinical_demographic_input_dir, 'ClinicalDemographic.tsv' )

clinical_diagnosis_input_dir = path.join( input_root, 'ClinicalDiagnosis' )
clinical_diagnosis_input_tsv = path.join( clinical_diagnosis_input_dir, 'ClinicalDiagnosis.tsv' )

clinical_exposure_input_dir = path.join( input_root, 'ClinicalExposure' )
clinical_exposure_input_tsv = path.join( clinical_exposure_input_dir, 'ClinicalExposure.tsv' )

clinical_non_targeted_therapy_input_dir = path.join( input_root, 'ClinicalNonTargetedTherapy' )
clinical_non_targeted_therapy_input_tsv = path.join( clinical_non_targeted_therapy_input_dir, 'ClinicalNonTargetedTherapy.tsv' )

clinical_participant_status_input_dir = path.join( input_root, 'ClinicalParticipantStatus' )
clinical_participant_status_input_tsv = path.join( clinical_participant_status_input_dir, 'ClinicalParticipantStatus.tsv' )

clinical_radiotherapy_input_dir = path.join( input_root, 'ClinicalRadiotherapy' )
clinical_radiotherapy_input_tsv = path.join( clinical_radiotherapy_input_dir, 'ClinicalRadiotherapy.tsv' )

clinical_specimen_input_dir = path.join( input_root, 'ClinicalSpecimen' )
clinical_specimen_input_tsv = path.join( clinical_specimen_input_dir, 'ClinicalSpecimen.tsv' )

clinical_surgery_input_dir = path.join( input_root, 'ClinicalSurgery' )
clinical_surgery_input_tsv = path.join( clinical_surgery_input_dir, 'ClinicalSurgery.tsv' )

clinical_targeted_therapy_input_dir = path.join( input_root, 'ClinicalTargetedTherapy' )
clinical_targeted_therapy_input_tsv = path.join( clinical_targeted_therapy_input_dir, 'ClinicalTargetedTherapy.tsv' )

demographic_input_dir = path.join( input_root, 'Demographic' )
demographic_input_tsv = path.join( demographic_input_dir, 'Demographic.tsv' )

diagnosis_input_dir = path.join( input_root, 'Diagnosis' )
diagnosis_input_tsvs = [
    path.join( diagnosis_input_dir, 'Diagnosis.from_getAllStudies.tsv' ),
    path.join( diagnosis_input_dir, 'Diagnosis.from_studyDiagnosisByStudyShortName.tsv' )
]

exposure_input_dir = path.join( input_root, 'Exposure' )
exposure_input_tsv = path.join( exposure_input_dir, 'Exposure.tsv' )

non_targeted_therapy_input_dir = path.join( input_root, 'NonTargetedTherapy' )
non_targeted_therapy_input_tsv = path.join( non_targeted_therapy_input_dir, 'NonTargetedTherapy.tsv' )

participant_input_dir = path.join( input_root, 'Participant' )
participant_input_tsv = path.join( participant_input_dir, 'Participant.tsv' )
participant_demographic_record_id_input_tsv = path.join( participant_input_dir, 'Participant.demographic_record_id.tsv' )
participant_diagnosis_record_id_input_tsv = path.join( participant_input_dir, 'Participant.diagnosis_record_id.tsv' )
participant_exposure_record_id_input_tsv = path.join( participant_input_dir, 'Participant.exposure_record_id.tsv' )
participant_non_targeted_therapy_record_id_input_tsv = path.join( participant_input_dir, 'Participant.non_targeted_therapy_record_id.tsv' )
participant_radiological_procedure_record_id_input_tsv = path.join( participant_input_dir, 'Participant.radiological_procedure_record_id.tsv' )
participant_specimen_record_id_input_tsv = path.join( participant_input_dir, 'Participant.specimen_record_id.from_getAllStudies.tsv' )
participant_surgical_procedure_record_id_input_tsv = path.join( participant_input_dir, 'Participant.surgical_procedure_record_id.tsv' )
participant_targeted_therapy_record_id_input_tsv = path.join( participant_input_dir, 'Participant.targeted_therapy_record_id.tsv' )

participant_status_input_dir = path.join( input_root, 'ParticipantStatus' )
participant_status_input_tsv = path.join( participant_status_input_dir, 'ParticipantStatus.tsv' )

radiotherapy_input_dir = path.join( input_root, 'Radiotherapy' )
radiotherapy_input_tsv = path.join( radiotherapy_input_dir, 'Radiotherapy.tsv' )

specimen_input_dir = path.join( input_root, 'Specimen' )
specimen_participant_id_input_tsvs = [
    path.join( specimen_input_dir, 'Specimen.participant_id.from_biospecimenOverview.tsv' ),
    path.join( specimen_input_dir, 'Specimen.participant_id.from_biospecimen_data_files.tsv' )
]

surgery_input_dir = path.join( input_root, 'Surgery' )
surgery_input_tsv = path.join( surgery_input_dir, 'Surgery.tsv' )

targeted_therapy_input_dir = path.join( input_root, 'TargetedTherapy' )
targeted_therapy_input_tsv = path.join( targeted_therapy_input_dir, 'TargetedTherapy.tsv' )

output_root = path.join( 'extracted_data', 'ctdc_postprocessed' )

participant_output_dir = path.join( output_root, 'Participant' )
participant_output_tsv = path.join( participant_output_dir, 'Participant.tsv' )
participant_demographic_record_id_output_tsv = path.join( participant_output_dir, 'Participant.demographic_record_id.tsv' )
participant_diagnosis_record_id_output_tsv = path.join( participant_output_dir, 'Participant.diagnosis_record_id.tsv' )
participant_exposure_record_id_output_tsv = path.join( participant_output_dir, 'Participant.exposure_record_id.tsv' )
participant_non_targeted_therapy_record_id_output_tsv = path.join( participant_output_dir, 'Participant.non_targeted_therapy_record_id.tsv' )
participant_participant_status_output_tsv = path.join( participant_output_dir, 'Participant.participant_status_record_id.tsv' )
participant_radiological_procedure_record_id_output_tsv = path.join( participant_output_dir, 'Participant.radiological_procedure_record_id.tsv' )
participant_specimen_record_id_output_tsv = path.join( participant_output_dir, 'Participant.specimen_record_id.tsv' )
participant_surgical_procedure_record_id_output_tsv = path.join( participant_output_dir, 'Participant.surgical_procedure_record_id.tsv' )
participant_targeted_therapy_record_id_output_tsv = path.join( participant_output_dir, 'Participant.targeted_therapy_record_id.tsv' )

# I am unilaterally deciding that we're going to remove the "Clinical" prefix from all collated objects, merging parallel pairs where they exist. Object if you will.
demographic_output_dir = path.join( output_root, 'Demographic' )
demographic_output_tsv = path.join( demographic_output_dir, 'Demographic.tsv' )

diagnosis_output_dir = path.join( output_root, 'Diagnosis' )
diagnosis_output_tsv = path.join( diagnosis_output_dir, 'Diagnosis.tsv' )

exposure_output_dir = path.join( output_root, 'Exposure' )
exposure_output_tsv = path.join( exposure_output_dir, 'Exposure.tsv' )

non_targeted_therapy_output_dir = path.join( output_root, 'NonTargetedTherapy' )
non_targeted_therapy_output_tsv = path.join( non_targeted_therapy_output_dir, 'NonTargetedTherapy.tsv' )

participant_status_output_dir = path.join( output_root, 'ParticipantStatus' )
participant_status_output_tsv = path.join( participant_status_output_dir, 'ParticipantStatus.tsv' )

radiotherapy_output_dir = path.join( output_root, 'Radiotherapy' )
radiotherapy_output_tsv = path.join( radiotherapy_output_dir, 'Radiotherapy.tsv' )

surgery_output_dir = path.join( output_root, 'Surgery' )
surgery_output_tsv = path.join( surgery_output_dir, 'Surgery.tsv' )

targeted_therapy_output_dir = path.join( output_root, 'TargetedTherapy' )
targeted_therapy_output_tsv = path.join( targeted_therapy_output_dir, 'TargetedTherapy.tsv' )

# EXECUTION

for output_dir in [ output_root, demographic_output_dir, diagnosis_output_dir, exposure_output_dir, non_targeted_therapy_output_dir, participant_output_dir, participant_status_output_dir, radiotherapy_output_dir, surgery_output_dir, targeted_therapy_output_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

################################################################################
# Copy the main Participant data structure without modification.
shutil.copy2( participant_input_tsv, participant_output_tsv )

################################################################################
# Save the map from Participant to ParticipantStatus.
participant_participant_status = map_columns_one_to_many( clinical_participant_status_input_tsv, 'participant_ids', 'participant_status_record_id' )

with open( participant_participant_status_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'participant_status_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_participant_status ):
        for participant_status_record_id in sorted( participant_participant_status[participant_id] ):
            print( *[ participant_id, participant_status_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save ParticipantStatus records. Verify as feasible.
clinical_participant_status = load_tsv_as_dict( clinical_participant_status_input_tsv )
# This one has a column we need ('primary_cause_of_death') that isn't in ClinicalParticipantStatus. Add it manually to the columns list below.
participant_status = load_tsv_as_dict( participant_status_input_tsv )
participant_status_columns = list()

with open( participant_status_output_tsv, 'w' ) as OUT:
    for participant_status_record_id in sorted( clinical_participant_status ):
        participant_status_row = list()
        if len( participant_status_columns ) == 0:
            participant_status_columns = list( clinical_participant_status[participant_status_record_id].keys() ).copy()
            participant_status_columns.remove( 'participant_ids' )
            participant_status_columns.append( 'primary_cause_of_death' )
            print( *participant_status_columns, sep='\t', end='\n', file=OUT )
        for participant_status_column in participant_status_columns:
            if participant_status_column in clinical_participant_status[participant_status_record_id] and clinical_participant_status[participant_status_record_id][participant_status_column] is not None:
                # This should KeyError if participant_status_column isn't found in the ParticipantStatus record.
                if participant_status_record_id in participant_status and participant_status[participant_status_record_id][participant_status_column] is not None and participant_status[participant_status_record_id][participant_status_column] != '' and participant_status[participant_status_record_id][participant_status_column] != clinical_participant_status[participant_status_record_id][participant_status_column]:
                    sys.exit( f"FATAL: Loaded ClinicalParticipantStatus record '{participant_status_record_id}' with '{participant_status_column}' == '{clinical_participant_status[participant_status_record_id][participant_status_column]}'; but ParticipantStatus table has value '{participant_status[participant_status_record_id][participant_status_column]}' instead, please investigate." )
                participant_status_row.append( clinical_participant_status[participant_status_record_id][participant_status_column] )
            elif participant_status_column not in clinical_participant_status[participant_status_record_id] and participant_status_record_id in participant_status and participant_status[participant_status_record_id][participant_status_column] is not None and participant_status[participant_status_record_id][participant_status_column] != '':
                # This should KeyError if participant_status_column isn't found in the matching ParticipantStatus record.
                participant_status_row.append( participant_status[participant_status_record_id][participant_status_column] )
            else:
                participant_status_row.append( '' )
        print( *participant_status_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to Specimen.
participant_specimen_record_id = map_columns_one_to_many( biospecimen_overview_input_tsv, 'participant_id', 'specimen_record_id' )

for input_map_file in [ map_file for sub_list in [ specimen_participant_id_input_tsvs, [ participant_specimen_record_id_input_tsv ] ] for map_file in sub_list ]:
    current_map = map_columns_one_to_many( input_map_file, 'participant_id', 'specimen_record_id' )
    for participant_id in current_map:
        if participant_id is None or participant_id == '':
            sys.exit( f"FATAL: Unexpected null participant_id in {input_map_file}, please investigate or alter receiving code." )
        if participant_id not in participant_specimen_record_id:
            participant_specimen_record_id[participant_id] = set()
        for specimen_record_id in current_map[participant_id]:
            if specimen_record_id is None or specimen_record_id == '':
                sys.exit( f"FATAL: Unexpected null specimen_record_id in {input_map_file}, please investigate or alter receiving code." )
            participant_specimen_record_id[participant_id].add( specimen_record_id )

for input_map_file in [ clinical_specimen_input_tsv ]:
    ###                                                              --> \/ <--
    current_map = map_columns_one_to_many( input_map_file, 'participant_ids', 'specimen_record_id' )
    for participant_id in current_map:
        if participant_id is None or participant_id == '':
            sys.exit( f"FATAL: Unexpected null participant_id in {input_map_file}, please investigate or alter receiving code." )
        if participant_id not in participant_specimen_record_id:
            participant_specimen_record_id[participant_id] = set()
        for specimen_record_id in current_map[participant_id]:
            if specimen_record_id is None or specimen_record_id == '':
                sys.exit( f"FATAL: Unexpected null specimen_record_id in {input_map_file}, please investigate or alter receiving code." )
            participant_specimen_record_id[participant_id].add( specimen_record_id )

with open( participant_specimen_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'specimen_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_specimen_record_id ):
        for specimen_record_id in sorted( participant_specimen_record_id[participant_id] ):
            print( *[ participant_id, specimen_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to Demographic.
participant_demographic_record_id = map_columns_one_to_many( clinical_demographic_input_tsv, 'participant_ids', 'demographic_record_id' )
demographic_backup_map = map_columns_one_to_many( participant_demographic_record_id_input_tsv, 'participant_id', 'demographic_record_id' )

for participant_id in demographic_backup_map:
    if participant_id not in participant_demographic_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_demographic_record_id_input_tsv} not found in {clinical_demographic_input_tsv}; please investigate." )
    for demographic_record_id in demographic_backup_map[participant_id]:
        if demographic_record_id not in participant_demographic_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected demographic_record_id '{demographic_record_id}' linked in {participant_demographic_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_demographic_input_tsv}; please investigate." )

with open( participant_demographic_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'demographic_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_demographic_record_id ):
        for demographic_record_id in sorted( participant_demographic_record_id[participant_id] ):
            print( *[ participant_id, demographic_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save Demographic records. Verify as feasible.
clinical_demographic = load_tsv_as_dict( clinical_demographic_input_tsv )
demographic = load_tsv_as_dict( demographic_input_tsv )
demographic_columns = list()

with open( demographic_output_tsv, 'w' ) as OUT:
    for demographic_record_id in sorted( clinical_demographic ):
        demographic_row = list()
        if len( demographic_columns ) == 0:
            demographic_columns = list( clinical_demographic[demographic_record_id].keys() ).copy()
            demographic_columns.remove( 'participant_ids' )
            print( *demographic_columns, sep='\t', end='\n', file=OUT )
        for demographic_column in demographic_columns:
            # Break with a KeyError if this access goes awry.
            if clinical_demographic[demographic_record_id][demographic_column] is not None:
                demographic_row.append( clinical_demographic[demographic_record_id][demographic_column] )
            else:
                demographic_row.append( '' )
            if demographic_record_id in demographic and demographic[demographic_record_id][demographic_column] is not None and demographic[demographic_record_id][demographic_column] != '' and demographic[demographic_record_id][demographic_column] != clinical_demographic[demographic_record_id][demographic_column]:
                sys.exit( f"FATAL: Loaded ClinicalDemographic record '{demographic_record_id}' with '{demographic_column}' == '{clinical_demographic[demographic_record_id][demographic_column]}'; but Demographic table has value '{demographic[demographic_record_id][demographic_column]}' instead, please investigate." )
        print( *demographic_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to Diagnosis.
participant_diagnosis_record_id = map_columns_one_to_many( clinical_diagnosis_input_tsv, 'participant_ids', 'diagnosis_record_id' )
diagnosis_backup_map = map_columns_one_to_many( participant_diagnosis_record_id_input_tsv, 'participant_id', 'diagnosis_record_id' )

for participant_id in diagnosis_backup_map:
    if participant_id not in participant_diagnosis_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_diagnosis_record_id_input_tsv} not found in {clinical_diagnosis_input_tsv}; please investigate." )
    for diagnosis_record_id in diagnosis_backup_map[participant_id]:
        if diagnosis_record_id not in participant_diagnosis_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected diagnosis_record_id '{diagnosis_record_id}' linked in {participant_diagnosis_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_diagnosis_input_tsv}; please investigate." )

with open( participant_diagnosis_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'diagnosis_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_diagnosis_record_id ):
        for diagnosis_record_id in sorted( participant_diagnosis_record_id[participant_id] ):
            print( *[ participant_id, diagnosis_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save Diagnosis records. Verify as feasible.
diagnosis = dict()
# WEIRD NOTE: These have fields tumor_grade and stage_of_disease, both missing in ClinicalDiagnosis.
for input_map in diagnosis_input_tsvs:
    diagnosis[input_map] = load_tsv_as_dict( input_map )
clinical_diagnosis = load_tsv_as_dict( clinical_diagnosis_input_tsv )
diagnosis_columns = list()
diagnosis_records_printed = set()

with open( diagnosis_output_tsv, 'w' ) as OUT:
    # Use ClinicalDiagnosis as a master ID map, but load columns from the (more complete) Diagnosis entity tables.
    for diagnosis_record_id in sorted( clinical_diagnosis ):
        diagnosis_row = list()
        for input_map in diagnosis:
            if diagnosis_record_id in diagnosis[input_map]:
                if len( diagnosis_columns ) == 0:
                    diagnosis_columns = list( diagnosis[input_map][diagnosis_record_id].keys() ).copy()
                    print( *diagnosis_columns, sep='\t', end='\n', file=OUT )
                if diagnosis_record_id not in diagnosis_records_printed:
                    for diagnosis_column in diagnosis_columns:
                        # Break with a KeyError if this access goes awry.
                        if diagnosis[input_map][diagnosis_record_id][diagnosis_column] is not None:
                            diagnosis_row.append( diagnosis[input_map][diagnosis_record_id][diagnosis_column] )
                        else:
                            diagnosis_row.append( '' )
                        # Verify data with ClinicalDiagnosis where available.
                        if diagnosis_column in clinical_diagnosis[diagnosis_record_id] and clinical_diagnosis[diagnosis_record_id][diagnosis_column] is not None and clinical_diagnosis[diagnosis_record_id][diagnosis_column] != '' and clinical_diagnosis[diagnosis_record_id][diagnosis_column] != diagnosis[input_map][diagnosis_record_id][diagnosis_column]:
                            sys.exit( f"FATAL: Loaded ClinicalDiagnosis record '{diagnosis_record_id}' with '{diagnosis_column}' == '{clinical_diagnosis[diagnosis_record_id][diagnosis_column]}'; but Diagnosis table {input_map} has value '{diagnosis[input_map][diagnosis_record_id][diagnosis_column]}' instead, please investigate." )
                    print( *diagnosis_row, sep='\t', end='\n', file=OUT )
                    diagnosis_records_printed.add( diagnosis_record_id )

################################################################################
# Aggregate and save the map from Participant to Exposure.
participant_exposure_record_id = map_columns_one_to_many( clinical_exposure_input_tsv, 'participant_ids', 'exposure_record_id' )
exposure_backup_map = map_columns_one_to_many( participant_exposure_record_id_input_tsv, 'participant_id', 'exposure_record_id' )

for participant_id in exposure_backup_map:
    if participant_id not in participant_exposure_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_exposure_record_id_input_tsv} not found in {clinical_exposure_input_tsv}; please investigate." )
    for exposure_record_id in exposure_backup_map[participant_id]:
        if exposure_record_id not in participant_exposure_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected exposure_record_id '{exposure_record_id}' linked in {participant_exposure_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_exposure_input_tsv}; please investigate." )

with open( participant_exposure_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'exposure_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_exposure_record_id ):
        for exposure_record_id in sorted( participant_exposure_record_id[participant_id] ):
            print( *[ participant_id, exposure_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save Exposure records. Verify as feasible.
clinical_exposure = load_tsv_as_dict( clinical_exposure_input_tsv )
# WEIRD NOTE: This has the 'environmental_exposure_type' field, missing in ClinicalExposure.
# Unfortunately at time of writing is is also an empty table (2026-09). Adding by hand as a result. Yuck.
exposure = load_tsv_as_dict( exposure_input_tsv )
exposure_columns = list()

with open( exposure_output_tsv, 'w' ) as OUT:
    for exposure_record_id in sorted( clinical_exposure ):
        exposure_row = list()
        if len( exposure_columns ) == 0:
            exposure_columns = list( clinical_exposure[exposure_record_id].keys() ).copy()
            exposure_columns.remove( 'participant_ids' )
            exposure_columns.append( 'environmental_exposure_type' )
            print( *exposure_columns, sep='\t', end='\n', file=OUT )
        for exposure_column in exposure_columns:
            if exposure_column in clinical_exposure[exposure_record_id] and clinical_exposure[exposure_record_id][exposure_column] is not None:
                # Break here with a KeyError if exposure_column isn't found in this table.
                if exposure_record_id in exposure and exposure[exposure_record_id][exposure_column] is not None and exposure[exposure_record_id][exposure_column] != '' and exposure[exposure_record_id][exposure_column] != clinical_exposure[exposure_record_id][exposure_column]:
                    sys.exit( f"FATAL: Loaded ClinicalExposure record '{exposure_record_id}' with '{exposure_column}' == '{clinical_exposure[exposure_record_id][exposure_column]}'; but Exposure table has value '{exposure[exposure_record_id][exposure_column]}' instead, please investigate." )
                exposure_row.append( clinical_exposure[exposure_record_id][exposure_column] )
            elif exposure_column not in clinical_exposure[exposure_record_id] and exposure_record_id in exposure and exposure[exposure_record_id][exposure_column] is not None and exposure[exposure_record_id][exposure_column] != '':
                exposure_row.append( exposure[exposure_record_id][exposure_column] )
            else:
                exposure_row.append( '' )
        print( *exposure_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to NonTargetedTherapy.
participant_non_targeted_therapy_record_id = map_columns_one_to_many( clinical_non_targeted_therapy_input_tsv, 'participant_ids', 'non_targeted_therapy_record_id' )
non_targeted_therapy_backup_map = map_columns_one_to_many( participant_non_targeted_therapy_record_id_input_tsv, 'participant_id', 'non_targeted_therapy_record_id' )

for participant_id in non_targeted_therapy_backup_map:
    if participant_id not in participant_non_targeted_therapy_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_non_targeted_therapy_record_id_input_tsv} not found in {clinical_non_targeted_therapy_input_tsv}; please investigate." )
    for non_targeted_therapy_record_id in non_targeted_therapy_backup_map[participant_id]:
        if non_targeted_therapy_record_id not in participant_non_targeted_therapy_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected non_targeted_therapy_record_id '{non_targeted_therapy_record_id}' linked in {participant_non_targeted_therapy_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_non_targeted_therapy_input_tsv}; please investigate." )

with open( participant_non_targeted_therapy_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'non_targeted_therapy_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_non_targeted_therapy_record_id ):
        for non_targeted_therapy_record_id in sorted( participant_non_targeted_therapy_record_id[participant_id] ):
            print( *[ participant_id, non_targeted_therapy_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save NonTargetedTherapy records. Verify as feasible.
clinical_non_targeted_therapy = load_tsv_as_dict( clinical_non_targeted_therapy_input_tsv )
non_targeted_therapy = load_tsv_as_dict( non_targeted_therapy_input_tsv )
non_targeted_therapy_columns = list()

with open( non_targeted_therapy_output_tsv, 'w' ) as OUT:
    for non_targeted_therapy_record_id in sorted( clinical_non_targeted_therapy ):
        non_targeted_therapy_row = list()
        if len( non_targeted_therapy_columns ) == 0:
            non_targeted_therapy_columns = list( clinical_non_targeted_therapy[non_targeted_therapy_record_id].keys() ).copy()
            non_targeted_therapy_columns.remove( 'participant_ids' )
            print( *non_targeted_therapy_columns, sep='\t', end='\n', file=OUT )
        for non_targeted_therapy_column in non_targeted_therapy_columns:
            # Break with a KeyError if this access goes awry.
            if clinical_non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column] is not None:
                non_targeted_therapy_row.append( clinical_non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column] )
            else:
                non_targeted_therapy_row.append( '' )
            if non_targeted_therapy_record_id in non_targeted_therapy and non_targeted_therapy_column in non_targeted_therapy[non_targeted_therapy_record_id] and non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column] is not None and non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column] != '' and non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column] != clinical_non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column]:
                sys.exit( f"FATAL: Loaded ClinicalNonTargetedTherapy record '{non_targeted_therapy_record_id}' with '{non_targeted_therapy_column}' == '{clinical_non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column]}'; but NonTargetedTherapy table has value '{non_targeted_therapy[non_targeted_therapy_record_id][non_targeted_therapy_column]}' instead, please investigate." )
        print( *non_targeted_therapy_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to Radiotherapy.
participant_radiological_procedure_record_id = map_columns_one_to_many( clinical_radiotherapy_input_tsv, 'participant_ids', 'radiological_procedure_record_id' )
radiotherapy_backup_map = map_columns_one_to_many( participant_radiological_procedure_record_id_input_tsv, 'participant_id', 'radiological_procedure_record_id' )

for participant_id in radiotherapy_backup_map:
    if participant_id not in participant_radiological_procedure_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_radiological_procedure_record_id_input_tsv} not found in {clinical_radiotherapy_input_tsv}; please investigate." )
    for radiological_procedure_record_id in radiotherapy_backup_map[participant_id]:
        if radiological_procedure_record_id not in participant_radiological_procedure_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected radiological_procedure_record_id '{radiological_procedure_record_id}' linked in {participant_radiological_procedure_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_radiotherapy_input_tsv}; please investigate." )

with open( participant_radiological_procedure_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'radiological_procedure_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_radiological_procedure_record_id ):
        for radiological_procedure_record_id in sorted( participant_radiological_procedure_record_id[participant_id] ):
            print( *[ participant_id, radiological_procedure_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save Radiotherapy records. Verify as feasible.
clinical_radiotherapy = load_tsv_as_dict( clinical_radiotherapy_input_tsv )
radiotherapy = load_tsv_as_dict( radiotherapy_input_tsv )
radiotherapy_columns = list()

with open( radiotherapy_output_tsv, 'w' ) as OUT:
    for radiological_procedure_record_id in sorted( clinical_radiotherapy ):
        radiotherapy_row = list()
        if len( radiotherapy_columns ) == 0:
            radiotherapy_columns = list( clinical_radiotherapy[radiological_procedure_record_id].keys() ).copy()
            radiotherapy_columns.remove( 'participant_ids' )
            print( *radiotherapy_columns, sep='\t', end='\n', file=OUT )
        for radiotherapy_column in radiotherapy_columns:
            # Break with a KeyError if this access goes awry.
            if clinical_radiotherapy[radiological_procedure_record_id][radiotherapy_column] is not None:
                radiotherapy_row.append( clinical_radiotherapy[radiological_procedure_record_id][radiotherapy_column] )
            else:
                radiotherapy_row.append( '' )
            if radiological_procedure_record_id in radiotherapy and radiotherapy_column in radiotherapy[radiological_procedure_record_id] and radiotherapy[radiological_procedure_record_id][radiotherapy_column] is not None and radiotherapy[radiological_procedure_record_id][radiotherapy_column] != '' and radiotherapy[radiological_procedure_record_id][radiotherapy_column] != clinical_radiotherapy[radiological_procedure_record_id][radiotherapy_column]:
                sys.exit( f"FATAL: Loaded ClinicalRadiotherapy record '{radiological_procedure_record_id}' with '{radiotherapy_column}' == '{clinical_radiotherapy[radiological_procedure_record_id][radiotherapy_column]}'; but Radiotherapy table has value '{radiotherapy[radiological_procedure_record_id][radiotherapy_column]}' instead, please investigate." )
        print( *radiotherapy_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to Surgery.
participant_surgical_procedure_record_id = map_columns_one_to_many( clinical_surgery_input_tsv, 'participant_ids', 'surgical_procedure_record_id' )
surgery_backup_map = map_columns_one_to_many( participant_surgical_procedure_record_id_input_tsv, 'participant_id', 'surgical_procedure_record_id' )

for participant_id in surgery_backup_map:
    if participant_id not in participant_surgical_procedure_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_surgical_procedure_record_id_input_tsv} not found in {clinical_surgery_input_tsv}; please investigate." )
    for surgical_procedure_record_id in surgery_backup_map[participant_id]:
        if surgical_procedure_record_id not in participant_surgical_procedure_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected surgical_procedure_record_id '{surgical_procedure_record_id}' linked in {participant_surgical_procedure_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_surgery_input_tsv}; please investigate." )

with open( participant_surgical_procedure_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'surgical_procedure_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_surgical_procedure_record_id ):
        for surgical_procedure_record_id in sorted( participant_surgical_procedure_record_id[participant_id] ):
            print( *[ participant_id, surgical_procedure_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save Surgery records. Verify as feasible.
clinical_surgery = load_tsv_as_dict( clinical_surgery_input_tsv )
surgery = load_tsv_as_dict( surgery_input_tsv )
surgery_columns = list()

with open( surgery_output_tsv, 'w' ) as OUT:
    for surgical_procedure_record_id in sorted( clinical_surgery ):
        surgery_row = list()
        if len( surgery_columns ) == 0:
            surgery_columns = list( clinical_surgery[surgical_procedure_record_id].keys() ).copy()
            surgery_columns.remove( 'participant_ids' )
            print( *surgery_columns, sep='\t', end='\n', file=OUT )
        for surgery_column in surgery_columns:
            # Break with a KeyError if this access goes awry.
            if clinical_surgery[surgical_procedure_record_id][surgery_column] is not None:
                surgery_row.append( clinical_surgery[surgical_procedure_record_id][surgery_column] )
            else:
                surgery_row.append( '' )
            if surgical_procedure_record_id in surgery and surgery_column in surgery[surgical_procedure_record_id] and surgery[surgical_procedure_record_id][surgery_column] is not None and surgery[surgical_procedure_record_id][surgery_column] != '' and surgery[surgical_procedure_record_id][surgery_column] != clinical_surgery[surgical_procedure_record_id][surgery_column]:
                sys.exit( f"FATAL: Loaded ClinicalSurgery record '{surgical_procedure_record_id}' with '{surgery_column}' == '{clinical_surgery[surgical_procedure_record_id][surgery_column]}'; but Surgery table has value '{surgery[surgical_procedure_record_id][surgery_column]}' instead, please investigate." )
        print( *surgery_row, sep='\t', end='\n', file=OUT )

################################################################################
# Aggregate and save the map from Participant to TargetedTherapy.
participant_targeted_therapy_record_id = map_columns_one_to_many( clinical_targeted_therapy_input_tsv, 'participant_ids', 'targeted_therapy_record_id' )
targeted_therapy_backup_map = map_columns_one_to_many( participant_targeted_therapy_record_id_input_tsv, 'participant_id', 'targeted_therapy_record_id' )

for participant_id in targeted_therapy_backup_map:
    if participant_id not in participant_targeted_therapy_record_id:
        sys.exit( f"FATAL: Unexpected participant_id '{participant_id}' from {participant_targeted_therapy_record_id_input_tsv} not found in {clinical_targeted_therapy_input_tsv}; please investigate." )
    for targeted_therapy_record_id in targeted_therapy_backup_map[participant_id]:
        if targeted_therapy_record_id not in participant_targeted_therapy_record_id[participant_id]:
            sys.exit( f"FATAL: Unexpected targeted_therapy_record_id '{targeted_therapy_record_id}' linked in {participant_targeted_therapy_record_id_input_tsv} to participant '{participant_id}' not found in {clinical_targeted_therapy_input_tsv}; please investigate." )

with open( participant_targeted_therapy_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'participant_id', 'targeted_therapy_record_id' ], sep='\t', end='\n', file=OUT )
    for participant_id in sorted( participant_targeted_therapy_record_id ):
        for targeted_therapy_record_id in sorted( participant_targeted_therapy_record_id[participant_id] ):
            print( *[ participant_id, targeted_therapy_record_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Collate and save TargetedTherapy records. Verify as feasible.
clinical_targeted_therapy = load_tsv_as_dict( clinical_targeted_therapy_input_tsv )
targeted_therapy = load_tsv_as_dict( targeted_therapy_input_tsv )
targeted_therapy_columns = list()

with open( targeted_therapy_output_tsv, 'w' ) as OUT:
    for targeted_therapy_record_id in sorted( clinical_targeted_therapy ):
        targeted_therapy_row = list()
        if len( targeted_therapy_columns ) == 0:
            targeted_therapy_columns = list( clinical_targeted_therapy[targeted_therapy_record_id].keys() ).copy()
            targeted_therapy_columns.remove( 'participant_ids' )
            print( *targeted_therapy_columns, sep='\t', end='\n', file=OUT )
        for targeted_therapy_column in targeted_therapy_columns:
            # Break with a KeyError if this access goes awry.
            if clinical_targeted_therapy[targeted_therapy_record_id][targeted_therapy_column] is not None:
                targeted_therapy_row.append( clinical_targeted_therapy[targeted_therapy_record_id][targeted_therapy_column] )
            else:
                targeted_therapy_row.append( '' )
            if targeted_therapy_record_id in targeted_therapy and targeted_therapy_column in targeted_therapy[targeted_therapy_record_id] and targeted_therapy[targeted_therapy_record_id][targeted_therapy_column] is not None and targeted_therapy[targeted_therapy_record_id][targeted_therapy_column] != '' and targeted_therapy[targeted_therapy_record_id][targeted_therapy_column] != clinical_targeted_therapy[targeted_therapy_record_id][targeted_therapy_column]:
                sys.exit( f"FATAL: Loaded ClinicalTargetedTherapy record '{targeted_therapy_record_id}' with '{targeted_therapy_column}' == '{clinical_targeted_therapy[targeted_therapy_record_id][targeted_therapy_column]}'; but TargetedTherapy table has value '{targeted_therapy[targeted_therapy_record_id][targeted_therapy_column]}' instead, please investigate." )
        print( *targeted_therapy_row, sep='\t', end='\n', file=OUT )


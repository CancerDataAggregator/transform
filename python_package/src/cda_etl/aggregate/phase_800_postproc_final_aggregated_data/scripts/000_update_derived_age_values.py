#!/usr/bin/env python3 -u

import sys

from os import listdir, path, rename

input_dir = path.join( 'cda_tsvs', 'last_merge' )

subject_tsv = path.join( input_dir, 'subject.tsv' )
observation_tsv = path.join( input_dir, 'observation.tsv' )
temp_output_tsv = path.join( input_dir, 'observation_tmp.tsv' )

aux_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'values' )

# EXECUTION

updates = dict()
update_logs = dict()

for column_name in [ 'age_at_observation', 'vital_status' ]:
    updates[column_name] = dict()
    update_logs[column_name] = path.join( aux_dir, f"final_merged_CDA_data.{column_name}.updates.tsv" )

# Load non-null birth and death years for all subjects.
subject_id = dict()
year_of_birth = dict()
year_of_death = dict()

with open( subject_tsv ) as IN:
    column_names = next( IN ).rstrip('\n').split('\t')
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip('\n').split('\t') ) )
        subject_id[record['id_alias']] = record['id']
        year_of_birth[record['id_alias']] = record['year_of_birth']
        year_of_death[record['id_alias']] = record['year_of_death']

# Pass 1: Update/populate age_at_observation and update vital_status based on safe inferences about dates.

# Save 'dead_after' years for all subjects.
dead_after = dict()

with open( observation_tsv ) as IN, open( temp_output_tsv, 'w' ) as OUT:
    column_names = next( IN ).rstrip('\n').split('\t')
    print( *column_names, sep='\t', file=OUT )
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip('\n').split('\t') ) )
        subject_alias = record['subject_alias']
        year_of_observation = record['year_of_observation']
        # If year_of_observation is non-null,
        if year_of_observation != '':
            # If year_of_death is non-null,
            if year_of_death[subject_alias] != '':
                # Guess at vital_status based on year_of_observation and year_of_death and update, logging clashes.
                local_vital_status = ''
                if int( year_of_death[subject_alias] ) < int( year_of_observation ):
                    local_vital_status = 'dead'
                    if record['age_at_observation'] != '':
                        if subject_id[subject_alias] not in updates['age_at_observation']:
                            updates['age_at_observation'][subject_id[subject_alias]] = dict()
                        if record['age_at_observation'] not in updates['age_at_observation'][subject_id[subject_alias]]:
                            updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']] = set()
                        updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']].add( '' )
                        record['age_at_observation'] = ''
                elif int( year_of_death[subject_alias] ) > int( year_of_observation ):
                    local_vital_status = 'alive'

                if local_vital_status != '' and local_vital_status != record['vital_status']:
                    if subject_id[subject_alias] not in updates['vital_status']:
                        updates['vital_status'][subject_id[subject_alias]] = dict()
                    if record['vital_status'] not in updates['vital_status'][subject_id[subject_alias]]:
                        updates['vital_status'][subject_id[subject_alias]][record['vital_status']] = set()
                    updates['vital_status'][subject_id[subject_alias]][record['vital_status']].add( local_vital_status )
                    record['vital_status'] = local_vital_status

            # If year_of_birth is non-null and we have no affirmative reason to believe the subject has yet died,
            if year_of_birth[subject_alias] != '' and ( \
                    ( year_of_death[subject_alias] == '' and record['vital_status'].lower() != 'dead' ) or \
                    ( year_of_death[subject_alias] != '' and int( year_of_death[subject_alias] ) > int( year_of_observation ) ) or \
                    ( year_of_death[subject_alias] != '' and year_of_death[subject_alias] == year_of_observation and record['vital_status'].lower() != 'dead' ) \
                ):
                # Guess at age_at_observation based on year_of_observation and year_of_birth and update, logging clashes.
                local_age_at_observation = str( int( year_of_observation ) - int( year_of_birth[subject_alias] ) )
                if local_age_at_observation != record['age_at_observation']:
                    if subject_id[subject_alias] not in updates['age_at_observation']:
                        updates['age_at_observation'][subject_id[subject_alias]] = dict()
                    if record['age_at_observation'] not in updates['age_at_observation'][subject_id[subject_alias]]:
                        updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']] = set()
                    updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']].add( local_age_at_observation )
                    record['age_at_observation'] = local_age_at_observation

        # Save 'dead_after' years for all subjects.
        if year_of_death[subject_alias] != '':
            # If year_of_death is known, dead_after is known precisely.
            dead_after[subject_alias] = int(year_of_death[subject_alias])
        elif year_of_observation != '':
            # If year_of_death is unknown, but year_of_observation is known, vital_status is 'dead', and
            # year_of_observation is earlier than the best currently-known dead_after date, update to the new (better) value.
            if record['vital_status'] == 'dead' and ( subject_alias not in dead_after or dead_after[subject_alias] > int(year_of_observation) ):
                dead_after[subject_alias] = int(year_of_observation)

        print( *[ record[column_name] for column_name in column_names ], sep='\t', file=OUT )

rename( temp_output_tsv, observation_tsv )

# Pass 2: Use dead_after to delete age_at_observation values that clash with known ranges for
# death dates; update all age_at_observation values to be 90 for any computed to be > 90, and
# log all such modifications. Also update vital_status nulls to reflect new (aggregated)
# knowledge wherever such knowledge exists.

with open( observation_tsv ) as IN, open( temp_output_tsv, 'w' ) as OUT:
    column_names = next( IN ).rstrip('\n').split('\t')
    print( *column_names, sep='\t', file=OUT )
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip('\n').split('\t') ) )
        subject_alias = record['subject_alias']
        vital_status = record['vital_status']
        year_of_observation = record['year_of_observation']
        age_at_observation = record['age_at_observation']
        # If year_of_observation is non-null and past the dead_after date for this subject, delete age_at_observation.
        if year_of_observation != '' and subject_alias in dead_after and int(year_of_observation) > dead_after[subject_alias]:
            new_age_at_observation = ''
            if new_age_at_observation != age_at_observation:
                if subject_id[subject_alias] not in updates['age_at_observation']:
                    updates['age_at_observation'][subject_id[subject_alias]] = dict()
                if age_at_observation not in updates['age_at_observation'][subject_id[subject_alias]]:
                    updates['age_at_observation'][subject_id[subject_alias]][age_at_observation] = set()
                updates['age_at_observation'][subject_id[subject_alias]][age_at_observation].add( new_age_at_observation )
                record['age_at_observation'] = new_age_at_observation
            new_vital_status = 'dead'
            if new_vital_status != vital_status:
                if subject_id[subject_alias] not in updates['vital_status']:
                    updates['vital_status'][subject_id[subject_alias]] = dict()
                if vital_status not in updates['vital_status'][subject_id[subject_alias]]:
                    updates['vital_status'][subject_id[subject_alias]][vital_status] = set()
                updates['vital_status'][subject_id[subject_alias]][vital_status].add( new_vital_status )
                record['vital_status'] = new_vital_status
        # If age_at_observation is > 90, make it 90 and log the clash.
        age_at_observation = record['age_at_observation']
        if age_at_observation != '' and int(age_at_observation) > 90:
            new_age_at_observation = '90'
            if subject_id[subject_alias] not in updates['age_at_observation']:
                updates['age_at_observation'][subject_id[subject_alias]] = dict()
            if age_at_observation not in updates['age_at_observation'][subject_id[subject_alias]]:
                updates['age_at_observation'][subject_id[subject_alias]][age_at_observation] = set()
            updates['age_at_observation'][subject_id[subject_alias]][age_at_observation].add( new_age_at_observation )
            record['age_at_observation'] = new_age_at_observation

        # Update to fix any now-clashing (non-null age_at_observation) and (vital_status == 'dead') pairs.
        age_at_observation = record['age_at_observation']
        vital_status = record['vital_status']
        if age_at_observation != '' and vital_status.lower() == 'dead':
            new_age_at_observation = ''
            if subject_id[subject_alias] not in updates['age_at_observation']:
                updates['age_at_observation'][subject_id[subject_alias]] = dict()
            if age_at_observation not in updates['age_at_observation'][subject_id[subject_alias]]:
                updates['age_at_observation'][subject_id[subject_alias]][age_at_observation] = set()
            updates['age_at_observation'][subject_id[subject_alias]][age_at_observation].add( new_age_at_observation )
            record['age_at_observation'] = new_age_at_observation

        print( *[ record[column_name] for column_name in column_names ], sep='\t', file=OUT )

rename( temp_output_tsv, observation_tsv )

# Save any updates we performed to the designated log files.
for column_name in sorted( update_logs ):
    log_file = update_logs[column_name]
    with open( log_file, 'w' ) as OUT:
        print( *[ 'subject_id', 'observed_value', 'clashing_value', 'kept_value' ], sep='\t', file=OUT )
        for current_subject_id in sorted( updates[column_name] ):
            for observed_value in sorted( updates[column_name][current_subject_id] ):
                for clashing_value in sorted( updates[column_name][current_subject_id][observed_value] ):
                    print( *[ current_subject_id, observed_value, clashing_value, clashing_value ], sep='\t', file=OUT )



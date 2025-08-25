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

with open( observation_tsv ) as IN, open( temp_output_tsv, 'w' ) as OUT:
    column_names = next( IN ).rstrip('\n').split('\t')
    print( *column_names, sep='\t', file=OUT )
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip('\n').split('\t') ) )
        if record['year_of_observation'] != '':
            subject_alias = record['subject_alias']
            year_of_observation = record['year_of_observation']
            if year_of_death[subject_alias] != '':
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

            if year_of_birth[subject_alias] != '' and ( year_of_death[subject_alias] == '' or int( year_of_death[subject_alias] ) > int( year_of_observation ) or ( year_of_death[subject_alias] == year_of_observation and record['vital_status'] != 'dead' ) ):
                local_age_at_observation = str( int( year_of_observation ) - int( year_of_birth[subject_alias] ) )
                if local_age_at_observation != record['age_at_observation']:
                    if subject_id[subject_alias] not in updates['age_at_observation']:
                        updates['age_at_observation'][subject_id[subject_alias]] = dict()
                    if record['age_at_observation'] not in updates['age_at_observation'][subject_id[subject_alias]]:
                        updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']] = set()
                    updates['age_at_observation'][subject_id[subject_alias]][record['age_at_observation']].add( local_age_at_observation )
                    record['age_at_observation'] = local_age_at_observation

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



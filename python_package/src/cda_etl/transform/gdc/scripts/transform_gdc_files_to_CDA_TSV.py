#!/usr/bin/env python

import sys

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many
from os import makedirs, path

# PARAMETERS

input_dir = 'extracted_data/gdc/all_TSV_output'

case_input_tsv = path.join( input_dir, 'case.tsv' )
case_in_project_input_tsv = path.join( input_dir, 'case_in_project.tsv' )

file_input_tsv = path.join( input_dir, 'file.tsv' )
file_in_case_input_tsv = path.join( input_dir, 'file_in_case.tsv' )
file_associated_with_entity_input_tsv = path.join( input_dir, 'file_associated_with_entity.tsv' )
file_has_index_file_input_tsv = path.join( input_dir, 'file_has_index_file.tsv' )

program_input_tsv = path.join( input_dir, 'program.tsv' )

project_input_tsv = path.join( input_dir, 'project.tsv' )
project_in_program_input_tsv = path.join( input_dir, 'project_in_program.tsv' )

case_maps = {
    
    'aliquot': path.join( input_dir, 'aliquot_from_case.tsv' ),
    'analyte': path.join( input_dir, 'analyte_from_case.tsv' ),
    'portion': path.join( input_dir, 'portion_from_case.tsv' ),
    'sample': path.join( input_dir, 'sample_from_case.tsv' ),
    'slide': path.join( input_dir, 'slide_from_case.tsv' )
}

specimen_tsvs = {
    
    'aliquot': path.join( input_dir, 'aliquot.tsv' ),
    'analyte': path.join( input_dir, 'analyte.tsv' ),
    'portion': path.join( input_dir, 'portion.tsv' ),
    'sample': path.join( input_dir, 'sample.tsv' ),
    'slide': path.join( input_dir, 'slide.tsv' )
}

output_dir = path.join( 'cda_tsvs', 'gdc_raw_unharmonized' )

file_output_tsv = path.join( output_dir, 'file.tsv' )
file_associated_project_output_tsv = path.join( output_dir, 'file_associated_project.tsv' )
file_identifier_output_tsv = path.join( output_dir, 'file_identifier.tsv' )
file_specimen_output_tsv = path.join( output_dir, 'file_specimen.tsv' )
file_subject_output_tsv = path.join( output_dir, 'file_subject.tsv' )

output_records = dict()

file_output_fields = [
    
    'id',
    'label',
    'data_category',
    'data_type',
    'file_format',
    'drs_uri',
    'byte_size',
    'checksum',
    'data_modality',
    'imaging_modality',
    'dbgap_accession_number',
    'imaging_series'
]

# EXECUTION

if not path.exists(output_dir):
    
    makedirs(output_dir)

# Load mappable metadata from the base file table.

with open( file_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        id = record['file_id']

        if id in output_records:
            
            sys.exit(f"Unexpected failure? Duplicate file record {id}; aborting.")

        else:
            
            output_records[id] = dict()
            
            output_records[id]['id'] = id
            output_records[id]['label'] = record['file_name'] if record['file_name'] != '' else None
            output_records[id]['data_category'] = record['data_category'] if record['data_category'] != '' else None
            output_records[id]['data_type'] = record['data_type'] if record['data_type'] != '' else None
            output_records[id]['file_format'] = record['data_format'] if record['data_format'] != '' else None
            output_records[id]['associated_project'] = set()
            output_records[id]['drs_uri'] = 'drs://dg.4DFC:' + id
            output_records[id]['byte_size'] = int(record['file_size']) if record['file_size'] != '' else None
            output_records[id]['checksum'] =  record['md5sum'] if record['md5sum'] != '' else None
            output_records[id]['data_modality'] = 'Genomic'
            output_records[id]['imaging_modality'] = None
            output_records[id]['dbgap_accession_number'] = None
            output_records[id]['imaging_series'] = None
            output_records[id]['Subjects'] = set()
            output_records[id]['Specimens'] = set()

# Load file<->case ID pairs.

case_has_file = dict()

with open( file_in_case_input_tsv ) as IN:
    
    header = IN.readline()

    for line in IN:
        
        [ file_id, case_id ] = line.rstrip('\n').split('\t')

        if file_id not in output_records:
            
            sys.exit(f"Unexpected failure? file_id {file_id} from file_in_case.tsv not loaded from file.tsv; aborting.")

        else:
            
            if case_id not in case_has_file:
                
                case_has_file[case_id] = set()

            case_has_file[case_id].add(file_id)

# Load program names for all projects.

project_id_to_program_id = map_columns_one_to_one( project_in_program_input_tsv, 'project_id', 'program_id' )

program_name = map_columns_one_to_one( program_input_tsv, 'program_id', 'name' )

program_dbgap_accession_number = map_columns_one_to_one( program_input_tsv, 'program_id', 'dbgap_accession_number' )

project_in_program = dict()

for project_id in project_id_to_program_id:
    
    project_in_program[project_id] = program_name[project_id_to_program_id[project_id]]

# Load project IDs for all relevant cases.

case_in_project = map_columns_one_to_one( case_in_project_input_tsv, 'case_id', 'project_id' )

# Save project->file associations so we can propagate dbGaP PHS IDs from projects to files later on.

project_has_file = dict()

with open( case_in_project_input_tsv ) as IN:
    
    header = IN.readline()

    for line in IN:
        
        [ case_id, project_id ] = line.rstrip('\n').split('\t')

        if case_id in case_has_file:
            
            for file_id in case_has_file[case_id]:
                
                output_records[file_id]['associated_project'].add(project_id)

                if project_id not in project_has_file:
                    
                    project_has_file[project_id] = set()

                project_has_file[project_id].add(file_id)

# Convert 'associated_project' sets to lists.

for id in output_records:
    
    output_records[id]['associated_project'] = sorted(output_records[id]['associated_project'])

# Load case.submitter_id for all relevant cases and create Subject IDs by concatenating program IDs and case submitter_ids.

case_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'submitter_id' )

for case_id in case_submitter_id:
    
    if case_id in case_in_project and case_id in case_has_file:
        
        subject_id = f"{project_in_program[case_in_project[case_id]]}.{case_submitter_id[case_id]}"

        for file_id in case_has_file[case_id]:
            
            output_records[file_id]['Subjects'].add( subject_id )

# Convert 'Subjects' sets to lists.

for id in output_records:
    
    output_records[id]['Subjects'] = sorted(output_records[id]['Subjects'])

# Load project.dbgap_accession_number for all relevant projects. If this field
# is null for a particular project, then the relevant dbGaP accession has been
# helpfully attached to the _parent_program_ of that project instead; retrieve
# it from there.

with open( project_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        project_id = record['project_id']

        if project_id in project_has_file:
            
            for file_id in project_has_file[project_id]:
                
                if record['dbgap_accession_number'] != "":
                    
                    output_records[file_id]['dbgap_accession_number'] = record['dbgap_accession_number']

                else:
                    
                    output_records[file_id]['dbgap_accession_number'] = program_dbgap_accession_number[project_id_to_program_id[project_id]]

# Get specimen submitter_ids for all potential Specimen records so we can make IDs.

specimen_submitter_id = dict()

for specimen_type in specimen_tsvs:
    
    specimen_tsv = specimen_tsvs[specimen_type]

    specimen_id_field = f"{specimen_type}_id"

    current_id_to_submitter_id = map_columns_one_to_one( specimen_tsv, specimen_id_field, 'submitter_id' )

    if specimen_type not in specimen_submitter_id:
        
        specimen_submitter_id[specimen_type] = dict()

    for specimen_id in current_id_to_submitter_id:
        
        specimen_submitter_id[specimen_type][specimen_id] = current_id_to_submitter_id[specimen_id]

# Get case submitter_ids for all potential Specimen records so we can make IDs.

specimen_cda_id = dict()

for specimen_type in case_maps:
    
    case_map_tsv = case_maps[specimen_type]

    with open( case_map_tsv ) as IN:
        
        if specimen_type not in specimen_cda_id:
            
            specimen_cda_id[specimen_type] = dict()

        header = next(IN)

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            [ specimen_id, case_id ] = line.split('\t')

            specimen_label = specimen_submitter_id[specimen_type][specimen_id]

            if specimen_label == '' or specimen_label is None:
                
                specimen_label = specimen_id

            specimen_cda_id[specimen_type][specimen_id] = f"{case_in_project[case_id]}.{case_submitter_id[case_id]}.{specimen_label}"

# Mine associated_entities for Specimen IDs.

file_has_index_file = map_columns_one_to_many( file_has_index_file_input_tsv, 'file_id', 'index_file_id' )

with open( file_associated_with_entity_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        file_id = record['file_id']

        if file_id not in output_records:
            
            sys.exit(f"Unexpected failure? file_id {file_id} from file_associated_with_entity.tsv not loaded from file.tsv; aborting.")

        else:
            
            if record['entity_type'] in { 'aliquot', 'portion', 'slide' }:
                
                output_records[file_id]['Specimens'].add( specimen_cda_id[record['entity_type']][record['entity_id']] )

                # Index files don't appear as normal results from the `files` endpoint, so we need to attach them manually to related Specimens (transitively via the associations with the main [indexed] files).

                if file_id in file_has_index_file:
                    
                    for index_file_id in file_has_index_file[file_id]:
                        
                        if index_file_id not in output_records:
                            
                            sys.exit(f"Unexpected failure? index_file_id {index_file_id} from file_associated_with_entity.tsv (via {file_id}) not loaded from file.tsv; aborting.")

                        else:
                            
                            output_records[index_file_id]['Specimens'].add( specimen_cda_id[record['entity_type']][record['entity_id']] )

# Convert 'Specimens' sets to lists.

for id in output_records:
    
    output_records[id]['Specimens'] = sorted(output_records[id]['Specimens'])

# Write loaded data to output TSVs.

with open( file_output_tsv, 'w' ) as OUT:
    
    print( *file_output_fields, sep='\t', end='\n', file=OUT )

    for file_id in sorted( output_records ):
        
        output_row = list()

        for field_name in file_output_fields:
            
            if output_records[file_id][field_name] is None:
                
                output_row.append( '' )

            else:
                
                output_row.append( output_records[file_id][field_name] )

        print( *output_row, sep='\t', end='\n', file=OUT )

with open( file_associated_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'associated_project' ], sep='\t', end='\n', file=OUT )

    for file_id in sorted( output_records ):
        
        for project_id in sorted( output_records[file_id]['associated_project'] ):
            
            print( *[ file_id, project_id ], sep='\t', end='\n', file=OUT )

with open( file_identifier_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=OUT )

    for file_id in sorted( output_records ):
        
        print( *[ file_id, 'GDC', 'file.file_id', file_id ], sep='\t', end='\n', file=OUT )

with open( file_specimen_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'specimen_id' ], sep='\t', end='\n', file=OUT )

    for file_id in sorted( output_records ):
        
        for specimen_cda_id in sorted( output_records[file_id]['Specimens'] ):
            
            print( *[ file_id, specimen_cda_id ], sep='\t', end='\n', file=OUT )

with open( file_subject_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', end='\n', file=OUT )

    for file_id in sorted( output_records ):
        
        for subject_cda_id in sorted( output_records[file_id]['Subjects'] ):
            
            print( *[ file_id, subject_cda_id ], sep='\t', end='\n', file=OUT )



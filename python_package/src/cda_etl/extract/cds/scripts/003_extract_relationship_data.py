#!/usr/bin/env python3 -u

import gzip
import jsonlines
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <a gzipped Neo4j JSONL dump file>\n" )

dump_file = sys.argv[1]

# PARAMETERS

output_dir = path.join( 'extracted_data', 'cds' )

for target_dir in [ output_dir ]:
    
    if not path.exists( target_dir ):
        
        makedirs( target_dir )

# output_table_name = relationship_table_name[relationship_type][source_entity_type][dest_entity_type]

relationship_table_name = {
    
    'associated_with' : {
        
        'file' : {
            
            'file' : 'file_associated_with_file'
        }
    },
    'from_sample' : {
        
        'file' : {
            
            'sample' : 'file_from_sample'
        }
    },
    'of_file' : {
        
        'genomic_info' : {
            
            'file' : 'genomic_info_of_file'
        },
        'image' : {
            
            'file' : 'image_of_file'
        },
        'proteomic' : {
            
            'file' : 'proteomic_of_file'
        }
    },
    'of_image' : {
        
        'MultiplexMicroscopy' : {
            
            'image': 'MultiplexMicroscopy_of_image'
        }
    },
    'of_participant' : {
        
        'diagnosis' : {
            
            'participant' : 'diagnosis_of_participant'
        },
        'file' : {
            
            'participant' : 'file_of_participant'
        },
        'sample' : {
            
            'participant' : 'sample_from_participant'
        },
        'treatment' : {
            
            'participant' : 'treatment_of_participant'
        }
    },
    'of_program' : {
        
        'study' : {
            
            'program' : 'study_in_program'
        }
    },
    'of_study' : {
        
        'file' : {
            
            'study' : 'file_from_study'
        },
        'participant' : {
            
            'study' : 'participant_in_study'
        }
    }
}

# EXECUTION

# Assumes neo4j 'node'-type records' 'id' field contains a value unique to each 'node' record, regardless of 'label' [i.e. entity_type]. True at time of writing.

id_to_uuid = dict()

with gzip.open( dump_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        neo4j_type = record['type']

        if neo4j_type == 'node':
            
            neo4j_id = record['id']

            # Assumes every 'node' record has a 'properties'->'uuid'. True at time of writing.

            id_to_uuid[neo4j_id] = record['properties']['uuid']

output_data = dict()

new_relationships = dict()

with gzip.open( dump_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        neo4j_type = record['type']

        if neo4j_type == 'relationship':
            
            relationship_type = record['label']

            # Assumes 'start'->'labels' and 'end'->'labels' arrays only ever have one value each. True at time of writing.

            source_entity_type = record['start']['labels'][0]

            dest_entity_type = record['end']['labels'][0]

            if relationship_type not in relationship_table_name:
                
                if relationship_type not in new_relationships:
                    
                    new_relationships[relationship_type] = dict()

                if source_entity_type not in new_relationships[relationship_type]:
                    
                    new_relationships[relationship_type][source_entity_type] = dict()

                if dest_entity_type not in new_relationships[relationship_type][source_entity_type]:
                    
                    new_relationships[relationship_type][source_entity_type][dest_entity_type] = 1

                else:
                    
                    new_relationships[relationship_type][source_entity_type][dest_entity_type] = new_relationships[relationship_type][source_entity_type][dest_entity_type] + 1

            else:
                
                try:
                    
                    output_table_name = relationship_table_name[relationship_type][source_entity_type][dest_entity_type]

                except Exception as error:
                    
                    print( *[ relationship_type, source_entity_type, dest_entity_type ], sep='\t', file=sys.stderr )

                    print( f"error type '{type(error)}'", file=sys.stderr )

                    print( f"error message '{error}'", file=sys.stderr )

                if output_table_name not in output_data:
                    
                    output_data[output_table_name] = list()

                output_data[output_table_name].append( [ id_to_uuid[record['start']['id']], id_to_uuid[record['end']['id']] ] )

# Warn if we saw any unexpected relationships.

if len( new_relationships ) > 0:
    
    print( 'WARNING: Unanticipated relationships encountered:', end='\n\n', file=sys.stderr )

    for relationship_type in sorted( new_relationships ):
        
        for source_entity_type in sorted( new_relationships[relationship_type] ):
            
            for dest_entity_type in sorted( new_relationships[relationship_type][source_entity_type] ):
                
                print( f"    <{source_entity_type}> <{relationship_type}> <{dest_entity_type}> [{new_relationships[relationship_type][source_entity_type][dest_entity_type]} records]", file=sys.stderr )

    print( file=sys.stderr )

for output_table_name in sorted( output_data ):
    
    # This is a dumb way to do this, but what the heck, I'm tired, it works, and this is just a draft script

    source_entity_type = ''

    dest_entity_type = ''

    for rel_type in relationship_table_name:
        
        for source_type in relationship_table_name[rel_type]:
            
            for dest_type in relationship_table_name[rel_type][source_type]:
                
                if relationship_table_name[rel_type][source_type][dest_type] == output_table_name:
                    
                    source_entity_type = source_type
                    dest_entity_type = dest_type

    output_file = path.join( output_dir, f"{output_table_name}.tsv" )

    with open( output_file, 'w' ) as OUT:
        
        print( *[ f"{source_entity_type}_uuid", f"{dest_entity_type}_uuid" ], sep='\t', file=OUT )

        for record in output_data[output_table_name]:
            
            print( *record, sep='\t', file=OUT )



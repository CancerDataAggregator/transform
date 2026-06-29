#!/usr/bin/env python3 -u

import gzip
import re
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <a gzipped Neo4j JSONL dump file>\n" )

dump_file = sys.argv[1]

# PARAMETERS

output_dir = path.join( 'extracted_data', 'gc' )

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
        'Characterization': {
            'sample' : 'Characterization_of_sample'
        },
        'Composition': {
            'sample' : 'Composition_of_sample'
        },
        'Protocol': {
            'sample' : 'Protocol_of_sample'
        },
        'Publication': {
            'sample' : 'Publication_describes_sample'
        },
        'diagnosis' : {
            'sample' : 'diagnosis_from_sample'
        },
        'pdx' : {
            'sample' : 'pdx_from_sample'
        },
        'file' : {
            'sample' : 'file_from_sample'
        }
    },
    'of_consent_group' : {
        'participant' : {
            'consent_group' : 'participant_in_consent_group'
        }
    },
    'of_file' : {
        'Protocol' : {
            'file' : 'Protocol_of_file'
        },
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
        },
        'NonDICOMpathologyImages' : {
            'image': 'NonDICOMpathologyImages_of_image'
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
    'of_pdx' : {
        'sample' : {
            'pdx' : 'sample_from_pdx'
        }
    },
    'of_program' : {
        'study' : {
            'program' : 'study_in_program'
        }
    },
    'of_protocol': {
        'Publication' : {
            'Protocol' : 'Publication_describes_Protocol'
        }
    },
    'of_study' : {
        'Protocol' : {
            'study' : 'Protocol_from_study'
        },
        'consent_group' : {
            'study' : 'consent_group_in_study'
        },
        'file' : {
            'study' : 'file_from_study'
        },
        'investigator' : {
            'study' : 'investigator_from_study'
        },
        'participant' : {
            'study' : 'participant_in_study'
        },
        'pdx' : {
            'study' : 'pdx_in_study'
        },
        'sample' : {
            'study' : 'sample_in_study'
        }
    }
}

# EXECUTION

# Assumes memgraph '__mg_vertex__'-type records' '__mg_id__' field contains a value unique to each record, regardless of node type. Verified true at time of writing (2026-04-21).
id_to_uuid = dict()
id_to_type = dict()

output_data = dict()
new_relationships = dict()

with open( dump_file ) as IN:
    for next_line in IN:
        # CREATE (:__mg_vertex__:`genomic_info` {__mg_id__: 9806845, `uuid`: "431c6110-07d9-5d20-a4d1-feae5c5ffbcd", `created`: "2026-03-16T15:59:45.872745+00:00[Etc/UTC]", `library_selection`: "Unspecified", `library_layout`: "Not Reported", `instrument_model`: "Illumina HiSeq X Ten", `reference_genome_assembly`: "Not specified in data", `platform`: "Illumina", `genomic_info_id`: "id_9996", `library_strategy`: "WGS", `platform_concept_code`: "C146817", `library_strategy_concept_code`: "C101294"});
        node_block_result = re.search( r'^CREATE\s+\(:__mg_vertex__:`([^`]+)`\s+{__mg_id__:\s+(\S+),\s+`(\S.*)}\);$', next_line.rstrip( '\n' ) )

        if node_block_result is not None:
            entity_type = node_block_result.group( 1 )
            node_id = node_block_result.group( 2 )
            node_data_line = node_block_result.group( 3 )

            id_to_type[node_id] = entity_type

            for key_value_string in node_data_line.split( r', `' ):
                ( key, value ) = key_value_string.split( r'`: ' )
                if key == 'uuid':
                    id_to_uuid[node_id] = value.strip( '"' )

        # Assumption: all __mg_vertex__ CREATE definitions will have been seen before the first MATCH-based relationship definition. True at time of writing (2026-04-21).

        # MATCH (u:__mg_vertex__), (v:__mg_vertex__) WHERE u.__mg_id__ = 8286618 AND v.__mg_id__ = 9353455 CREATE (u)-[:`of_file` {`created`: DATETIME("2025-05-20T20:08:41.292127+00:00[Etc/UTC]")}]->(v);
        relationship_block_result = re.search( r'^MATCH\s+.*\s+WHERE\s+u\.__mg_id__\s+=\s+(\S+)\s+AND\s+v\.__mg_id__\s+=\s+(\S+)\s+CREATE\s+\(u\)-\[:`([^`]+)`\s.*\]->\(v\);$', next_line.rstrip( '\n' ) )

        if relationship_block_result is not None:
            source_entity_id = relationship_block_result.group( 1 )
            source_entity_type = id_to_type[source_entity_id]
            dest_entity_id = relationship_block_result.group( 2 )
            dest_entity_type = id_to_type[dest_entity_id]
            relationship_type = relationship_block_result.group( 3 )

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
                output_data[output_table_name].append( [ id_to_uuid[source_entity_id], id_to_uuid[dest_entity_id] ] )

# Warn if we saw any unexpected relationships.
if len( new_relationships ) > 0:
    print( 'WARNING: Unanticipated relationships encountered:', end='\n\n', file=sys.stderr )
    for relationship_type in sorted( new_relationships ):
        for source_entity_type in sorted( new_relationships[relationship_type] ):
            for dest_entity_type in sorted( new_relationships[relationship_type][source_entity_type] ):
                print( f"    <{source_entity_type}> <{relationship_type}> <{dest_entity_type}> [{new_relationships[relationship_type][source_entity_type][dest_entity_type]} records]", file=sys.stderr )
    print( file=sys.stderr )

for output_table_name in sorted( output_data ):
    # This is a dumb way to do this, but what the heck, I'm tired, it works, and it's not exactly hard to understand
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



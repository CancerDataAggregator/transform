#!/usr/bin/env bash

# A gzipped CDS Neo4j JSONL dump file is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if [[ $# -eq 0 ]]
then
    script_name=$(basename "$0")
    echo
    echo "   ERROR: Usage: $script_name <a gzipped Neo4j JSONL dump file>"
    echo
    exit 0
fi

dump_file=$1

chmod 755 ./package_root/extract/cds/scripts/*py

echo ./package_root/extract/cds/scripts/000_get_field_lists.py $dump_file
./package_root/extract/cds/scripts/000_get_field_lists.py $dump_file

echo ./package_root/extract/cds/scripts/001_extract_node_data.py $dump_file
./package_root/extract/cds/scripts/001_extract_node_data.py $dump_file

echo ./package_root/extract/cds/scripts/002_extract_relationship_data.py $dump_file
./package_root/extract/cds/scripts/002_extract_relationship_data.py $dump_file



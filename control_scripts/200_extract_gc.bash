#!/usr/bin/env bash

chmod 755 ./package_root/extract/gc/scripts/*py

echo dump_file=\$\(./package_root/extract/gc/scripts/000_preprocess_gc_and_print_dump_file_path.py\)
dump_file=$(./package_root/extract/gc/scripts/000_preprocess_gc_and_print_dump_file_path.py)

echo ./package_root/extract/gc/scripts/001_get_field_lists.py $dump_file
./package_root/extract/gc/scripts/001_get_field_lists.py $dump_file

echo ./package_root/extract/gc/scripts/002_extract_node_data.py $dump_file
./package_root/extract/gc/scripts/002_extract_node_data.py $dump_file

echo ./package_root/extract/gc/scripts/003_extract_relationship_data.py $dump_file
./package_root/extract/gc/scripts/003_extract_relationship_data.py $dump_file



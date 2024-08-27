#!/usr/bin/env bash

chmod 755 ./package_root/extract/cds/scripts/*py

echo dump_file=\$\(./package_root/extract/cds/scripts/000_preprocess_cds_and_print_dump_file_path.py\)
dump_file=$(./package_root/extract/cds/scripts/000_preprocess_cds_and_print_dump_file_path.py)

echo ./package_root/extract/cds/scripts/001_get_field_lists.py $dump_file
./package_root/extract/cds/scripts/001_get_field_lists.py $dump_file

echo ./package_root/extract/cds/scripts/002_extract_node_data.py $dump_file
./package_root/extract/cds/scripts/002_extract_node_data.py $dump_file

echo ./package_root/extract/cds/scripts/003_extract_relationship_data.py $dump_file
./package_root/extract/cds/scripts/003_extract_relationship_data.py $dump_file



#!/usr/bin/env bash

read -p "Postgres host: " db_host

read -p "Username: " db_username

read -p "Database: " db_name

output_dir=ddl_schema

mkdir -p $output_dir

output_file="${output_dir}/cda_database_ddl_schema.sql"

pg_dump -h $db_host -U $db_username -d $db_name --schema-only > $output_file

echo "Schema saved to '${output_file}'."



# How to generate the json schema needed by cda-service

1. Create and initialize a Python virtual environment:

		python3 -m venv venv
		source venv/bin/activate

2. Install the Simple DDL Parser (sdp) Python package: 

		pip install simple-ddl-parser

3. Export the SQL database schema to a DDL (database definition language) file (let's use the name `db_schema.sql` as an example):
    * From pgAdmin, use the left-side navigation pane to browse the components of your DB instance:
        * <database_name> --> Schemas --> public (right-click "public")
        * Select "Backup"
        * Set filename to `db_schema.sql` in your working directory
        * Set "Format" to "Plain"
        * Switch to the "Data/Objects" tab and uncheck everything except "Only schema"
        * Hit "Backup"

4. Run `sdp`, which will take `db_schema.sql` as an input and will output a JSON file `db_schema.json`. Use the `-t` option to specify a target subdirectory into which `db_schema.json` will be written.

		sdp -t <schema_subdirectory> db_schema.sql
		
5. Extract the comments from the DDL file and add them to the data in `db_schema.json`, creating a new (final) JSON schema file `db_schema_with_comments.json` containing the comments:

		python add_comments_to_json.py db_schema.sql <schema_subjdirectory>/db_schema.json
		
6. Copy the final commented JSON schema file `<schema_subdirectory>/db_schema_with_comments.json` over into the `cda-service` repo, specifically at `cda-service/src/main/resources/schema/cda-prototype-schema.json`

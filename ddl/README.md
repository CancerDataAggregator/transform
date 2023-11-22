# How to generate the json schema needed by cda-service

1. Create/Initialize your python virtual environment

		source venv/bin/activate
2. Install the Simple DDL Parser (sdp) 

		pip install simple-ddl-parser
3. Download a backup of the database where you have only exported the schema into the ddl directory
4. Run sdp from the ddl directory to create a json file. It will be put in the `schema` subdirectory

		sdp <ddl>
		
5. Then run the python script to add the comments from the ddl to the json schema. This will create a new file with the results 


		python add_comments_to_json.py <ddl> schema/<json>
		
6. Copy the output file to `cda-service/src/main/resources/schema/cda-prototype-schema.json`
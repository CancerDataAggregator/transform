#!/usr/bin/env python3 -u

import json

from jsonschema import validate

with open( 'cda_metadata_submission.schema.json' ) as SCHEMA, open( 'submission_instance_example.json' ) as INSTANCE:
    
    my_schema = json.load( SCHEMA )

    my_instance = json.load( INSTANCE )

    validate( instance=my_instance, schema=my_schema )



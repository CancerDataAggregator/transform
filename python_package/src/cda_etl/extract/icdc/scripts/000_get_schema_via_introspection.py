#!/usr/bin/env python3 -u

import requests
import json
import re
import sys

from os import makedirs, path

api_url = 'https://caninecommons.cancer.gov/v1/graphql/'

output_dir = path.join( 'auxiliary_metadata', '__schemas' )

out_json = path.join( output_dir, 'ICDC_schema.json' )

out_gql = path.join( output_dir, 'ICDC_schema.gql' )

query = '''{
  __schema {
    description
    types {
      name
      kind
      description
      fields {
        name
        description
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
        args {
          name
          description
          type {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
          defaultValue
        }
      }
      enumValues {
        name
        description
      }
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
    }
  }
}'''

if not path.exists( output_dir ):
    
    makedirs( output_dir )

response = requests.post( api_url, json={ 'query': query } )

if( response.ok ):
    
    schema = json.loads( response.content )

    with open( out_json, 'w' ) as OUT:
        
        print ( json.dumps( schema, indent=4, sort_keys=False ), file=OUT )

    with open( out_gql, 'w' ) as OUT:
        
        for currentType in schema['data']['__schema']['types']:
            
            if re.search( r'^__', currentType['name'] ) is None and currentType['name'] not in [ 'Boolean', 'Int', 'Float', 'String' ]:
                
                if currentType['kind'] == 'SCALAR':
                    
                    if currentType['description'] is not None:
                        
                        print( f'"""{currentType["description"]}"""', file=OUT )

                    print( f"scalar {currentType['name']}", file=OUT )

                elif currentType['kind'] == 'ENUM':
                    
                    print( f"enum {currentType['name']} {{", file=OUT )

                    for enumValue in currentType['enumValues']:
                        
                        print( f"  {enumValue['name']}", file=OUT )

                    print( '}', file=OUT )

                elif currentType['kind'] == 'OBJECT' or currentType['kind'] == 'INPUT_OBJECT':
                    
                    # Type description.

                    if currentType['description'] is not None:
                        
                        print( f'"""{currentType["description"]}"""', file=OUT )

                    # Type name.

                    if currentType['kind'] == 'OBJECT':
                        
                        print( f"type {currentType['name']} {{", file=OUT )

                    else:
                        
                        print( f"input {currentType['name']} {{", file=OUT )

                    if currentType['fields'] is None:
                        
                        currentType['fields'] = []

                    for field in currentType['fields']:
                        
                        # Field description.

                        if field['description'] is not None:
                            
                            print( f'  """{field["description"]}"""', file=OUT )

                        # Field name.

                        print( f"  {field['name']}", end='', file=OUT )

                        # Argument list.

                        if field['args'] is not None and len( field['args'] ) > 0:
                            
                            print( '(', end='', file=OUT )

                            first = True

                            for arg in field['args']:
                                
                                if first:
                                    
                                    print( f"{arg['name']}: ", end='', file=OUT )

                                    first = False

                                else:
                                    
                                    print( f", {arg['name']}: ", end='', file=OUT )

                                if arg['type']['kind'] == 'NON_NULL' and arg['type']['ofType']['kind'] == 'LIST' and arg['type']['ofType']['ofType']['kind'] == 'NON_NULL' \
                                    and 'ofType' in arg['type']['ofType']['ofType'] and arg['type']['ofType']['ofType']['ofType']['kind'] in [ 'SCALAR', 'OBJECT', 'ENUM' ]:
                                    
                                    print( f"[{arg['type']['ofType']['ofType']['ofType']['name']}!]!", end='', file=OUT )

                                elif arg['type']['kind'] == 'NON_NULL' and arg['type']['ofType']['kind'] == 'LIST' and arg['type']['ofType']['ofType']['kind'] in [ 'SCALAR', 'OBJECT', 'ENUM' ]:
                                    
                                    print( f"[{arg['type']['ofType']['ofType']['name']}]!", end='', file=OUT )

                                elif arg['type']['kind'] == 'NON_NULL':
                                    
                                    print( f"{arg['type']['ofType']['name']}!", end='', file=OUT )

                                elif arg['type']['kind'] == 'SCALAR' or arg['type']['kind'] == 'INPUT_OBJECT':
                                    
                                    print( f"{arg['type']['name']}", end='', file=OUT )

                                elif arg['type']['kind'] == 'LIST' and arg['type']['ofType']['kind'] in [ 'SCALAR', 'OBJECT' ]:
                                    
                                    print( f"[{arg['type']['ofType']['name']}]", end='', file=OUT )

                                elif arg['type']['kind'] == 'LIST' and arg['type']['ofType']['kind'] == 'NON_NULL' and arg['type']['ofType']['ofType']['kind'] in [ 'SCALAR', 'OBJECT', 'ENUM' ]:
                                    
                                    print( f"[{arg['type']['ofType']['ofType']['name']}!]", end='', file=OUT )

                                else:
                                    
                                    sys.exit( f"Unexpected result: arg[type][name] is {arg['type']['name']}" )

                            print( ')', end='', file=OUT )

                        # end if ( field['args'] is not None )

                        print( ': ', end='', file=OUT )

                        # Field return type.

                        if field['type']['kind'] in [ 'SCALAR', 'OBJECT' ]:
                            
                            print( field['type']['name'], file=OUT )

                        elif field['type']['kind'] == 'NON_NULL' and field['type']['ofType']['kind'] in [ 'SCALAR', 'OBJECT' ]:
                            
                            print( field['type']['ofType']['name'] + '!', file=OUT )

                        elif field['type']['kind'] == 'NON_NULL' and field['type']['ofType']['kind'] == 'LIST' and field['type']['ofType']['ofType']['kind'] == 'NON_NULL' and field['type']['ofType']['ofType']['ofType']['kind'] in [ 'SCALAR', 'OBJECT', 'ENUM' ]:
                            
                            print( f"[{field['type']['ofType']['ofType']['ofType']['name']}!]!", file=OUT )

                        elif field['type']['kind'] == 'NON_NULL' and field['type']['ofType']['kind'] == 'LIST' and field['type']['ofType']['ofType']['kind'] in [ 'SCALAR', 'OBJECT', 'ENUM' ]:
                            
                            print( f"[{field['type']['ofType']['ofType']['name']}]!", file=OUT )

                        elif field['type']['kind'] == 'LIST':
                            
                            # This won't generalize to arbitrarily deeply listed LISTs, but it will take care of two-level LISTs, and that's currently all we need to care about.

                            if field['type']['ofType']['name'] is None:
                                
                                print( f"[[{field['type']['ofType']['ofType']['name']}]]", file=OUT )

                            else:
                                
                                print( f"[{field['type']['ofType']['name']}]", file=OUT )

                        else:
                            
                            sys.exit( f"Unexpected result: field[type][kind] is {field['type']['kind']}" )

                    print('}', file=OUT)

                else:
                    
                    sys.exit( f"Unexpected result: currentType['kind'] is {currentType['kind']}" )

                print( file=OUT )

else:
    
    print( query )

    # If response code is not ok (200), print the resulting http error code with description

    response.raise_for_status()



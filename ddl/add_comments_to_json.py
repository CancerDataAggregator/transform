


import argparse
import json

def find_table(table, schema):
    for item in schema:
        if "table_name" in item and item["table_name"] == table:
            return item
    return None

def find_column_in_table(column, table):
    for item in table["columns"]:
        if item["name"] == column:
            return item
    return None


def main(args):
    comments = []

    with open(args.ddl, 'r') as ddlfile:
        for line in ddlfile.readlines():
            if line.startswith("COMMENT ON"):
                comments.append(line)
    with open(args.schema, 'r') as jsonfile: 
        schema = json.load(jsonfile)    
    
    for comment in comments:
        # COMMENT ON <table|column> <name> IS '<comment>'
        tokens = comment.split()
        commentstring = " ".join(tokens[5:]).replace("'", "")[:-1] # get rid of trailing ;
        if tokens[2] == "TABLE":
            (skip, table) = tokens[3].split(".")
            table = find_table(table, schema)
            table["comment"] = commentstring
        elif tokens[2] == "COLUMN":
            (skip, table, column) = tokens[3].split(".")
            table = find_table(table, schema)
            column = find_column_in_table(column, table)
            column["comment"] = commentstring

    (base, ext) = args.schema.split(".")
    newfilename = base + "_with_comments." + ext
    with open(newfilename, 'w') as jsonfile: 
        json.dump(schema, jsonfile, indent=1)


parser = argparse.ArgumentParser(description="Just an example",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("ddl", help="The ddl or mssql file")
parser.add_argument("schema", help="The json file tranformed by sdp")
args = parser.parse_args()
config = vars(args)
print(config)
main(args)

# Number of partitions into which we need to split lists of fields we're
# requesting by name from the API. Too many fields in one query string
# makes the server vomit.

number_of_field_list_chunks = 1

# Number of results we want the API to return per page of output.

result_page_size = 500

# Sub-objects we'll need to ask for with the API's 'expand' parameter
# because asking for them field-by-field makes URLs that are too big
# for the server to handle.
# 
# These will be treated as string literals and saved in a special list.
# 
# They will also be converted (below) to regular expressions and used
# to filter out individual fields from the target request list.

groups_to_expand = set()

# Fields we don't want.
# 
# Note: these will be interpreted as regular expressions matching
# entire field names end-to-end.

fields_to_filter = set()

# Prefixes for fields we know we don't want yet (except for IDs;
# see fields_to_use).
# 
# Note: these will be interpreted as regular expressions.

prefixes_to_filter = {
    
    'project'
}

# Fields we want to keep. These will be filtered because of matches
# to prefixes_to_filter unless we explicitly preserve them, so that's
# what this data structure does.
# 
# Note: these will be interpreted as string literals.

fields_to_use = {
    
    'project.project_id'
}

# Prefixes for substructures we know we don't want to scan for field names.
# 
# Note: these will be interpreted as regular expressions.

substructures_to_filter = {
    
    # We grab IDs from these and build relevant association tables. No
    # non-ID substructure is queried or stored.

    'project'
}

# Substructures containing statistical summaries, not sub-entity records.

statistical_summary_substructures = set()

# Once in a while we get an array of strings attached to the top-level entity
# for which we want to create a sub-entity table (e.g. files.acl). These need to
# be managed separately, since they're not picked up during the nested-object scan in
# get_substructure_field_lists() (because they have no substructures). We'll
# name the sub-entity table for each (key, value) pair in this dict using the key,
# and the value will indicate the association map in which we'll store links
# back to the top-level entity.
# 
# Handle arrays buried deeper than the top-level entity by prepending their parent
# entity names (e.g. 'diagnosis.sites_of_involvement').

array_entities = dict()

# Once in a while we need to save a thing with one name as a thing with
# a different name. The elements of the file.downstream_analyses list,
# for example, should be saved as 'analysis' objects, not 'downstream_analysis' objects.

save_entity_list_as = dict()

# Dicts in which to store association relationship data.

association_maps = {
    
    'annotation_in_project' : dict()
}

# Do we need to load association data by recursively scanning substructures
# of records at this endpoint? (If not, we'll scrape all needed association
# data from the top level without recursion.)

scan_substructures_for_association_relationships = False




# Number of partitions into which we need to split lists of fields we're
# requesting by name from the API. Too many fields in one query string
# makes the server vomit.

number_of_field_list_chunks = 2

# Number of results we want the API to return per page of output.

result_page_size = 5000

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
    
    'analysis\.input_files',
    'annotations',
    'cases',
    'downstream_analyses\.output_files',
    'index_files',
    'metadata_files'
}

# Fields we want to keep. These will be filtered because of matches
# to prefixes_to_filter unless we explicitly preserve them, so that's
# what this data structure does.
# 
# Note: these will be interpreted as string literals.

fields_to_use = {
    
    'analysis.input_files.file_id',
    'annotations.annotation_id',
    'cases.case_id',
    'downstream_analyses.output_files.file_id',
    'index_files.file_id',
    'metadata_files.file_id'
}

# Prefixes for substructures we know we don't want to scan for field names.
# 
# Note: these will be interpreted as regular expressions.

substructures_to_filter = {

    # We grab IDs from these and build relevant association tables. No
    # non-ID substructure is queried or stored.

    'analysis\.input_files',
    'annotations',
    'associated_entities',
    'cases',
    'downstream_analyses\.output_files',
    'index_files',
    'metadata_files',

    # I know this makes 'downstream_analyses\.output_files' above redundant,
    # but I'm putting it here anyway to document: we skip downstream_analyses
    # entirely during the substructure field-detection scan because this task is
    # already handled when we scan the 'analysis' entity, into which these
    # records will be merged, and we don't want to create what looks like a new
    # and separate entity type.

    'downstream_analyses'
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

array_entities = {
    
    'acl' : 'file_has_acl'
}

# Once in a while we need to save a thing with one name as a thing with
# a different name. The elements of the file.downstream_analyses list,
# for example, should be saved as 'analysis' objects, not 'downstream_analysis' objects.

save_entity_list_as = {
    
    'downstream_analyses': 'analysis'
}

# Dicts in which to store association relationship data.

association_maps = {
    
    'analysis_consumed_input_file' : dict(),
    'analysis_downstream_from_file' : dict(),
    'analysis_produced_file' : dict(),
    'downstream_analysis_produced_output_file' : dict(),
    'file_associated_with_entity' : dict(),
    'file_from_center' : dict(),
    'file_has_acl' : dict(),
    'file_has_annotation' : dict(),
    'file_has_index_file' : dict(),
    'file_has_metadata_file' : dict(),
    'file_in_archive' : dict(),
    'file_in_case' : dict(),
    'read_group_in_analysis' : dict(),
    'read_group_qc_in_read_group' : dict(),
    'file_has_acl' : dict()
}

# Do we need to load association data by recursively scanning substructures
# of records at this endpoint? (If not, we'll scrape all needed association
# data from the top level without recursion.)

scan_substructures_for_association_relationships = True



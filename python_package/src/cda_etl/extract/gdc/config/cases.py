
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

groups_to_expand = {
    
    'diagnoses',
    'diagnoses.pathology_details',
    'diagnoses.treatments',
    'exposures',
    'family_histories',
    'follow_ups',
    'follow_ups.molecular_tests',
    'samples',
    'samples.portions',
    'samples.portions.center',
    'samples.portions.analytes',
    'samples.portions.analytes.aliquots',
    'samples.portions.analytes.aliquots.center',
    'samples.portions.slides'
}

# Fields we don't want.
# 
# Note: these will be interpreted as regular expressions matching
# entire field names end-to-end.

fields_to_filter = {
    
    'aliquot_ids',
    'analyte_ids',
    'diagnosis_ids',
    'portion_ids',
    'sample_ids',
    'slide_ids',
    'submitter_aliquot_ids',
    'submitter_analyte_ids',
    'submitter_diagnosis_ids',
    'submitter_portion_ids',
    'submitter_sample_ids',
    'submitter_slide_ids'
}

# Prefixes for fields we know we don't want yet (except for IDs;
# see fields_to_use).
# 
# Note: these will be interpreted as regular expressions.

prefixes_to_filter = {
    
    'annotations',
    'diagnoses\.annotations',
    'files',
    'project',
    'samples\.annotations',
    'samples\.portions\.analytes\.aliquots\.annotations',
    'samples\.portions\.analytes\.annotations',
    'samples\.portions\.annotations',
    'samples\.portions\.slides\.annotations',
    'summary'
}

# Fields we want to keep. These will be filtered because of matches
# to prefixes_to_filter unless we explicitly preserve them, so that's
# what this data structure does.
# 
# Note: these will be interpreted as string literals.

fields_to_use = {
    
    'annotations.annotation_id',
    'diagnoses.annotations.annotation_id',
    'files.file_id',
    'project.project_id',
    'samples.annotations.annotation_id',
    'samples.portions.analytes.aliquots.annotations.annotation_id',
    'samples.portions.analytes.annotations.annotation_id',
    'samples.portions.annotations.annotation_id',
    'samples.portions.slides.annotations.annotation_id'
}

# Prefixes for substructures we know we don't want to scan for field names.
# 
# Note: these will be interpreted as regular expressions.

substructures_to_filter = {
    
    # We grab IDs from these and build relevant association tables. No
    # non-ID substructure is queried or stored.

    'annotations',
    'diagnoses\.annotations',
    'files',
    'project',
    'samples\.annotations',
    'samples\.portions\.analytes\.aliquots\.annotations',
    'samples\.portions\.analytes\.annotations',
    'samples\.portions\.annotations',
    'samples\.portions\.slides\.annotations',
    'summary'
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

array_entities = {
    
    'diagnosis.sites_of_involvement' : 'diagnosis_has_site_of_involvement',
    'diagnosis.weiss_assessment_findings' : 'diagnosis_has_weiss_assessment_finding'
}

# Once in a while we need to save a thing with one name as a thing with
# a different name. The elements of the file.downstream_analyses list,
# for example, should be saved as 'analysis' objects, not 'downstream_analysis' objects.

save_entity_list_as = dict()

# Dicts in which to store association relationship data.

association_maps = {
    
    'case_has_annotation' : dict(),
    'diagnosis_has_annotation' : dict(),
    'diagnosis_has_site_of_involvement' : dict(),
    'diagnosis_has_weiss_assessment_finding' : dict(),
    'sample_has_annotation' : dict(),
    'portion_has_annotation' : dict(),
    'analyte_has_annotation' : dict(),
    'aliquot_has_annotation' : dict(),
    'slide_has_annotation' : dict(),
    'case_in_project' : dict(),
    'aliquot_of_analyte' : dict(),
    'aliquot_from_center' : dict(),
    'analyte_from_portion' : dict(),
    'slide_from_portion' : dict(),
    'portion_from_sample' : dict(),
    'portion_from_center' : dict(),
    'sample_from_case' : dict(),
    'demographic_of_case' : dict(),
    'diagnosis_of_case' : dict(),
    'exposure_of_case' : dict(),
    'family_history_of_case' : dict(),
    'follow_up_of_case' : dict(),
    'molecular_test_from_follow_up' : dict(),
    'pathology_detail_of_diagnosis' : dict(),
    'treatment_of_diagnosis' : dict(),
    'tissue_source_site_of_case' : dict(),
    'aliquot_from_case' : dict(),
    'analyte_from_case' : dict(),
    'portion_from_case' : dict(),
    'slide_from_case' : dict()
}

# Do we need to load association data by recursively scanning substructures
# of records at this endpoint? (If not, we'll scrape all needed association
# data from the top level without recursion.)

scan_substructures_for_association_relationships = True



#Functions for merging fields either between DCs or within one DC
def merge_all_fields(data_commons_fields_dict,full_merge,nested_levels = ['Diagnosis']):
    #start with RS_merge
    dat = merge_fields_level(data_commons_fields_dict,RS_merge)
    dat_dict = make_dat_dict_for_transforms(data_commons_fields_dict,'Diagnosis',hierarchy) 
    print(dat_dict)
def source_hierarchy_by_time(records_dict):
    #accepts list of dicts, finds created time, sorts by oldest to newest
    times = []
    to_dat_dict = dict()
    for rec in records_dict:
        if 'created_datetime' in rec:
            ctime = rec.pop('created_datetime')
        else:
            ctime = ''
        times.append(ctime)
        to_dat_dict[ctime] = rec
    times.sort()
    return times,to_dat_dict
def merge_demo_records_time_hierarchy(records_dict,how_to_merge):
    time_hier,dat_dict = source_hierarchy_by_time(records_dict)
    return merge_fields_level(dat_dict,how_to_merge,source_hierarchy =time_hier)
def merge_fields_level(data_commons_fields_dict,how_to_merge,source_hierarchy):
    dat = dict()
    for field in how_to_merge:
        dat[field] = how_to_merge[field]['default_value']
        hierarchy = source_hierarchy
        if 'source_hierarchy' in how_to_merge[field]:
            hierarchy = how_to_merge[field]['source_hierarchy']
        if how_to_merge[field]['merge_type'] == 'append_field_vals':
            dat_list = []
            for source in hierarchy:
                if source in data_commons_fields_dict and field in data_commons_fields_dict[source]:
                    dat_list.append(data_commons_fields_dict[source][field])
            dat[field] = append_field_vals_to_single_list(dat_list)
        elif how_to_merge[field]['merge_type'] == 'coalesce': #Needs a data dictionary, not list
            dat_dict = make_dat_dict_for_transforms(data_commons_fields_dict,field,hierarchy)
            dat[field] = coalesce_field_values(dat_dict,
                                                   how_to_merge[field]['default_value'],hierarchy)
        else: #merge_codeable_concept
            dat_dict = make_dat_dict_for_transforms(data_commons_fields_dict,field,hierarchy)
            dat[field] = merge_codeable_concept(dat_dict,
                                                   how_to_merge[field]['default_value'],hierarchy)
    return dat
        #for source in data_commons_fields_dict:
            #if field in data_commons_fields_dict[source]:
                
def merge_codeable_concept(data_commons_fields_dict,default_value,source_hierarchy):
    #Need to coalesce the text field, and append others
    dat = dict()
    #make dictionary of only 'text' from all data commons entries
    txt_dat_dict = dict()
    for source in data_commons_fields_dict:
        for rec in data_commons_fields_dict[source]:
            txt_dat_dict[source]=rec['text']
    dat['text'] = coalesce_field_values(txt_dat_dict,default_value,source_hierarchy)
    #make list of only 'coding' from all data commons entries
    coding_dat_dict = dict({'coding':[]})
    for source in data_commons_fields_dict:
        for rec in data_commons_fields_dict[source]:
            coding_dat_dict['coding'].append(rec['coding'])
    dat['coding'] = append_field_vals_to_single_list(coding_dat_dict['coding'])
    return [dat]
    
def append_field_vals_to_single_list(field_vals_list_of_lists):
    #strictly appends values from multiple data sources in one list. Does nothing to
    #examine matching data types (one string and one list, etc.).
    dat = []
    if len(field_vals_list_of_lists)>0:
        for i in field_vals_list_of_lists:
            if i !=[]:
                if isinstance(i,list):
                    dat = dat + i
                else:
                    dat.append(i)
    return dat
        
def coalesce_field_values(data_dictionary,default_value,source_hierarchy):
    dat_return = default_value
    for source in source_hierarchy:
        if source in data_dictionary and data_dictionary[source] is not None:
            dat_return = data_dictionary[source]
            break
    return dat_return
def make_dat_dict_for_transforms(data_commons_fields_dict,field,source_hierarchy):
    dat_dict = dict()
    for source in source_hierarchy:
        if source in data_commons_fields_dict and field in data_commons_fields_dict[source]:
            dat_dict[source]=data_commons_fields_dict[source][field]
    return dat_dict
import argparse
import json
import sys
import jsonlines
import yaml
import datetime
import gzip

import cdatransform.merge.merge_functions as mf

def get_patient_info_1_DC(input_file):
    All_Patients = []
    All_Entries = dict({})
    with gzip.open(input_file, "r") as infp:
        readDC = jsonlines.Reader(infp)
        for case in readDC:
            patient = case.get('id')
            All_Patients.append(patient)
            All_Entries[patient] = case
    return All_Patients,All_Entries

def get_patient_info_all_DCs(input_file_dict):
    All_Patient_ids = dict({})
    All_Entries_All_DCs = dict({})
    for source in input_file_dict:
        source_patient_list,All_Entries_All_DCs[source] = get_patient_info_1_DC(input_file_dict[source])
        for patient in source_patient_list:
            if patient in All_Patient_ids:
                All_Patient_ids[patient].append(source)
            else:
                All_Patient_ids[patient] = [source]
    return All_Patient_ids,All_Entries_All_DCs

def main():
    parser = argparse.ArgumentParser(description="Merge data between DCs")
    parser.add_argument("merge_file", help="file name for how to merge fields. Should end with .yml")
    parser.add_argument("output_file", help="Output file name. Should end with .gz")
    parser.add_argument("--gdc", help="GDC file name. Should end with .gz")
    parser.add_argument("--pdc", help="GDC file name. Should end with .gz")
    args = parser.parse_args()
    
    with open(args.merge_file) as file:
        how_to_merge = yaml.full_load(file)
    #Need all info from the files, and dictionary listing patient_ids and sources found.
    input_file_dict = dict({"gdc":args.gdc,"pdc":args.pdc})
    All_Patients_sources,All_Entries_All_DCs = get_patient_info_all_DCs(input_file_dict)
    total_patient = len(All_Patients_sources)
    #loop over all patient_ids, merge data if found in multiple sources
    with gzip.open(args.output_file, "w") as outfp:
        writer = jsonlines.Writer(outfp)
        count = 0
        for patient in All_Patients_sources:
            #for every patient, count number of sources. If more than one, merging is needed.
            if len(All_Patients_sources[patient])==1:
                writer.write(All_Entries_All_DCs[All_Patients_sources[patient][0]][patient])
            else:
                #make entities - - - - {'gdc': gdc data for patient, 'pdc': pdc data for patient}
                entities = dict({})
                for source in All_Patients_sources[patient]:
                    entities[source] = All_Entries_All_DCs[source][patient]
                merged_entry = mf.merge_fields_level(entities,how_to_merge['Patient_merge'],['gdc','pdc'])
                writer.write(merged_entry)
            count +=1
            if count % 5000 == 0:
                sys.stderr.write(f"Processed {count} cases out of {total_patient}.\n")
if __name__ == "__main__":
    main()
    

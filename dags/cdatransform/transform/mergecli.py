import argparse
import gzip
import logging
import sys
from collections import defaultdict

import jsonlines
import yaml

try:
    import cdatransform.transform.merge.merge_functions as mf
    from cdatransform.lib import yamlPathMapping
    from cdatransform.services.storage_service import StorageService
    from cdatransform.transform.validate import LogValidation
except ImportError:
    import dags.cdatransform.transform.merge.merge_functions as mf
    from dags.cdatransform.lib import yamlPathMapping
    from dags.cdatransform.services.storage_service import StorageService
    from dags.cdatransform.transform.validate import LogValidation

logger = logging.getLogger(__name__)


class Merge:
    def __init__(self, how_to_merge_file: str, output_file: str, *args, **kwargs):
        self.storage_service = StorageService()

        with open(yamlPathMapping(how_to_merge_file, "merge")) as file:
            self.how_to_merge: dict = yaml.full_load(file)

        self.output_file = output_file

        self.input_file_dict: dict = {
            source: kwargs[source]
            for source in ["gdc", "pdc", "idc"]
            if kwargs.get(source)
        }

    def _get_patient_info_1_DC(self, input_file):
        All_IDs: list = []
        All_Entries: dict = {}
        with self.storage_service.get_session(input_file, "r") as infp:
            readDC = jsonlines.Reader(infp)
            for line in readDC:
                All_IDs.append(line["id"])
                All_Entries[line["id"]] = line
        return All_IDs, All_Entries

    def _get_endpoint_info_all_DCs(self):
        All_ID_Sources = defaultdict(list)  # {'TARGET1': ['GDC', 'PDC']}
        All_Entries_All_DCs: dict = (
            {}
        )  # {'GDC': {'TARGET1': {GDC target1 record}, 'TARGET2': ... },
        #   'PDC': {'TARGET2': {PDC target2 record}, ... } }
        for source in self.input_file_dict:
            source_ID_list, All_Entries_All_DCs[source] = self._get_patient_info_1_DC(
                self.input_file_dict[source]
            )
            for id in source_ID_list:
                All_ID_Sources[id].append(source)
            # for id in source_ID_list:
            #    if id in All_ID_Sources:
            #        All_ID_Sources[id].append(source)
            #    else:
            #        All_ID_Sources[id] = [source]
        return All_ID_Sources, All_Entries_All_DCs

    def _get_coalesce_field_names(self, merge_field_dict) -> list:
        coal_fields: list = [
            key
            for key, val in merge_field_dict.items()
            if val.get("merge_type") == "coalesce"
        ]
        # coal_fields:list = []
        # for key, val in merge_field_dict.items():
        #    if val.get("merge_type") == "coalesce":
        #        coal_fields.append(key)
        return coal_fields

    def _prep_log_merge_error(self, entities, merge_field_dict):
        sources: list = list(entities.keys())
        coal_fields: list = self._get_coalesce_field_names(merge_field_dict)
        ret_dat: dict = {}
        for source in sources:
            temp_dict: dict = {
                field: entities.get(source).get(field) for field in coal_fields
            }
            temp: dict = {}
            # for field in coal_fields:
            #    temp[field] = entities.get(source).get(field)
            ret_dat[source] = temp

        for source, val in entities.items():
            id: str = val["id"]
            break
        for source, val in entities.items():
            if (
                val.get("ResearchSubject")[0].get("member_of_research_project")
                is not None
            ):
                project: str = val.get("ResearchSubject")[0].get(
                    "member_of_research_project"
                )
                break
        return coal_fields, ret_dat, id, project

    def _log_merge_error(self, entities, all_sources, fields, log):
        coal_fields, coal_dat, patient_id, project = self._prep_log_merge_error(
            entities, fields
        )
        all_sources.insert(0, "patient")
        all_sources.insert(1, patient_id)
        all_sources.insert(2, "project")
        all_sources.insert(3, project)
        prefix = "_".join(all_sources)
        log.agree_sources(coal_dat, prefix, coal_fields)
        return log

    def merge_subjects(self):
        All_endpoints_sources, All_Entries_All_DCs = self._get_endpoint_info_all_DCs()
        total_patient = len(All_endpoints_sources)
        # loop over all patient_ids, merge data if found in multiple sources

        with self.storage_service.get_session(self.output_file, "w") as outfp:
            writer = jsonlines.Writer(outfp)
            count = 0
            log = LogValidation()
            for patient in All_endpoints_sources:
                # for every patient, count number of sources. If more than one, merging is needed.
                if len(All_endpoints_sources[patient]) == 1:
                    writer.write(
                        All_Entries_All_DCs[All_endpoints_sources[patient][0]][patient]
                    )
                else:
                    # make entities - - - - {'gdc': gdc data for patient, 'pdc': pdc data for patient}
                    entities: dict = {
                        source: All_Entries_All_DCs[source][patient]
                        for source in All_endpoints_sources[patient]
                    }
                    # entities:dict = {}
                    # for source in All_endpoints_sources[patient]:
                    #    entities[source] = All_Entries_All_DCs[source][patient]

                    merged_entry = mf.merge_fields_level(
                        entities,
                        self.how_to_merge["Patient_merge"],
                        ["gdc", "pdc", "idc"],
                    )
                    log = self._log_merge_error(
                        entities,
                        ["gdc", "pdc", "idc"],
                        self.how_to_merge["Patient_merge"],
                        log,
                    )
                    writer.write(merged_entry)
                count += 1
                if count % 5000 == 0:
                    sys.stderr.write(
                        f"Processed {count} cases out of {total_patient}.\n"
                    )
            log.generate_report(logging.getLogger("test"))

        return self.output_file

    # As of 9-20-2022, CDA assumes each file_id is a UUID, and each file can only originate/be stored
    # at one DC. merge_files can be edited to be able to work like merge_subjects. Until then, only
    # concatenate files together.
    def merge_files(self):
        # All_endpoints_sources, All_Entries_All_DCs = get_endpoint_info_all_DCs(
        #    input_file_dict
        ##)
        # total_files = len(All_endpoints_sources)

        # loop over all file_ids, merge data if found in multiple sources
        with self.storage_service.get_session(self.output_file, "w") as outfp:
            writer = jsonlines.Writer(outfp)
            count = 0
            log = LogValidation()
            for source, files in self.input_file_dict.items():
                with self.storage_service.get_session(files, "r") as file_recs:
                    reader = jsonlines.Reader(file_recs)
                    for file_rec in reader:
                        count += 1
                        writer.write(file_rec)
                        if count % 50000 == 0:
                            sys.stderr.write(f"Processed {count} files.\n")
            # for file_key, file_rec in All_endpoints_sources.items():
            #    # for every patient, count number of sources. If more than one, merging is needed.
            #    if len(file_rec) == 1:
            #        #find all subjects in record
            #        temp_subjects = []
            #        #sys.stderr.write(f"More than one source for a file. {str(file_rec)}\n")
            #        for subj in All_Entries_All_DCs[All_endpoints_sources[file_key][0]][file_key]["Subject"]:
            #            temp_subjects.append(all_subjects[subj['id']])
            #        All_Entries_All_DCs[All_endpoints_sources[file_key][0]][file_key]["Subject"] = temp_subjects
            #        writer.write(All_Entries_All_DCs[All_endpoints_sources[file_key][0]][file_key])
            #    else:
            #        sys.stderr.write(f"More than one source for a file. {str(file_rec)}\n")
            #    count += 1
            #    if count % 5000 == 0:
            #        sys.stderr.write(f"Processed {count} cases out of {total_files}.\n")

        return self.output_file

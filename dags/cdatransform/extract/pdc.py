import argparse
import gzip
import json
import pathlib
import shutil
import sys
import time
from collections import defaultdict
from math import ceil

import jsonlines
from cdatransform.lib import get_case_ids

from .lib import retry_get
from .pdc_query_lib import *


class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class PDC:
    def __init__(
        self, endpoint="https://pdc.cancer.gov/graphql", make_spec_file=None
    ) -> None:
        self.endpoint = endpoint
        self.make_spec_file = make_spec_file

    def cases(
        self,
        case_ids=None,
    ):

        if case_ids:
            # Have one case_id or list of case_ids from a file.
            # Get all info from the case endpoint query in PDC. Need to split fields
            # for the queries and merge. PDC hates long query strings
            # Note: used mostly for testing. Does not get Study info for each case
            for case_id in case_ids:
                case_info_a = retry_get(
                    self.endpoint, params={"query": query_single_case_a(case_id)}
                )
                print("got part a")
                case_info_b = retry_get(
                    self.endpoint, params={"query": query_single_case_b(case_id)}
                )
                print("got part b")
                # Merge two halves of case info
                case = {
                    **case_info_a.json()["data"]["case"][0],
                    **case_info_b.json()["data"]["case"][0],
                }
                print("merged_dicts")
                yield case
        else:
            # Downloading bulk case info. Used for populating data. PDC API is
            # awful, and has no good way to get ALL bulk info for cases. Must
            # loop over all programs, projects, studies and extract case demographics,
            # diagnoses, sample/aliquot, and taxon info per study, and merge results.
            jData = retry_get(
                self.endpoint, params={"query": make_all_programs_query()}
            )
            AllPrograms = jData.json()["data"]["allPrograms"]
            out = []
            for program in AllPrograms:
                for project in program["projects"]:
                    added_info = dict(
                        {"project_submitter_id": project["project_submitter_id"]}
                    )
                    for study in project["studies"]:
                        # get study_id and embargo_date, and other info for study
                        pdc_study_id = study["pdc_study_id"]
                        study_info = retry_get(
                            self.endpoint,
                            params={"query": make_study_query(pdc_study_id)},
                        )
                        study_rec = study_info.json()["data"]["study"][0]
                        study_rec.update(study)
                        # Get demographic info
                        dem = self.demographics_for_study(pdc_study_id, 100)
                        # Get diagnosis info
                        diag = self.diagnoses_for_study(pdc_study_id, 100)
                        # Get samples info
                        samp = self.samples_for_study(pdc_study_id, 100)
                        # Get taxon info
                        taxon = self.taxon_for_study(pdc_study_id)
                        # Aggregate all case info for this study
                        out = agg_cases_info_for_study(
                            study_rec, dem, diag, samp, taxon, added_info
                        )
                        for case in out:
                            yield case

    def get_case_id_list(self):
        # This function was previously used to get all case_ids in PDC, then ping PDC
        # for every single case_id found. They did not like that
        result = retry_get(self.endpoint, params={"query": query_all_cases()})
        for case in result.json()["data"]["allCases"]:
            yield case["case_id"]

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                # Save for another script
                # samples_files_list = []
                # for index, sample in enumerate(case["samples"]):
                # Based on the PDC data model, all files in PDC are associated
                # with samples/aliquots. Can append all samples files
                #    samples_files_list.extend(
                #        self._files_per_sample_dict.get(sample["sample_id"], [])
                #    )
                #    case["samples"][index]["files"] = self._files_per_sample_dict.get(
                #        sample["sample_id"]
                #    )
                #    for index_aliquot, aliquot in enumerate(
                #        case["samples"][index]["aliquots"]
                #    ):
                #        case["samples"][index]["aliquots"][index_aliquot][
                #            "files"
                #        ] = self._files_per_sample_dict.get(aliquot["aliquot_id"])
                # case["files"] = list(
                #    {v["file_id"]: v for v in samples_files_list}.values()
                # )
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def save_files(self, out_file, file_ids=None):
        t0 = time.time()
        n = 0
        # Get and write metadata_files_chunks
        # This makes a dictionary of specimen_id: file_ids to link later on
        specimen_files_dict = defaultdict(list)
        metadata_files = {
            file["file_id"]: file
            for chunk in self._metadata_files_chunk(file_ids)
            for file in chunk
        }
        with gzip.open("meta_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for file, rec in metadata_files.items():
                if file_ids is None or file in file_ids:
                    writer.write(rec)
                    for aliquot in rec.get("aliquots", []):
                        specimen_files_dict[aliquot["sample_id"]].append(rec["file_id"])
                        specimen_files_dict[aliquot["aliquot_id"]].append(
                            rec["file_id"]
                        )
                n += 1
                if n % 500 == 0:
                    sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} metadata_files in {time.time() - t0}s\n")
        # Write specimen_files_dict to file
        if self.make_spec_file:
            for specimen, files in specimen_files_dict.items():
                specimen_files_dict[specimen] = list(set(files))
            with gzip.open(self.make_spec_file, "wt", encoding="ascii") as out:
                json.dump(specimen_files_dict, out)
        # Get and write UIfile chunks - PDC does not support using any UI calls, but
        # Seven Bridges does it... so here we are. Note: CDA does not use anything from
        # the UI files, and it is strictly for Seven Bridges benefit
        n = 0
        if file_ids:
            uifiles = {
                file["file_id"]: file
                for chunk in self._UIfiles_chunk()
                for file in chunk
                if file["file_id"] in file_ids
            }
        else:
            uifiles = {
                file["file_id"]: file
                for chunk in self._UIfiles_chunk()
                for file in chunk
            }
        with gzip.open("ui_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for file, rec in uifiles.items():
                if file_ids is None or file in file_ids:
                    writer.write(rec)
                n += 1
                if n % 500 == 0:
                    sys.stderr.write(f"Wrote {n} files in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} ui_files in {time.time() - t0}s\n")
        # Get and write files from studies. Similar to bulk cases, loop over program,
        # project, study and extract files per study
        study_files = defaultdict(list)
        n = 0
        with gzip.open("studyfiles_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for chunk in self._study_files_chunk():
                for file in chunk:
                    if file_ids is None or file["file_id"] in file_ids:
                        writer.write(file)
                    study_files[file["file_id"]].append(file)
                    n += 1
                    if n % 5000 == 0:
                        sys.stderr.write(
                            f"Pulled {n} study files in {time.time() - t0}s\n"
                        )
        sys.stderr.write(f"Pulled {n} study files in {time.time() - t0}s\n")
        with gzip.open(out_file, "wb") as f_out:
            for f in ["meta_out.jsonl.gz", "studyfiles_out.jsonl.gz"]:
                with gzip.open(f) as f_in:
                    shutil.copyfileobj(f_in, f_out)

    def filter_cases(self, records, case_ids):
        out = []
        for rec in records:
            if rec["case_id"] in case_ids:
                out.append(rec)
        return out

    def _fetch_file_data_from_cache(self, cache_file):
        if not (cache_file.exists()):
            sys.stderr.write(f"Cache file {cache_file} not found. Generating.\n")
            files_per_sample_dict = self._get_files_per_sample_dict(cache_file)
        else:
            sys.stderr.write(f"Loading files linkages from file {cache_file}.\n")
            files_per_sample_dict = defaultdict(list)
            with gzip.open(cache_file, "rb") as f_in:
                reader = jsonlines.Reader(f_in)
                for f in reader:
                    aliquots = f.get("aliquots")
                    if aliquots:
                        for aliquot in aliquots:
                            files_per_sample_dict[aliquot["sample_id"]].append(
                                {"file_id": f.get("file_id")}
                            )
                            files_per_sample_dict[aliquot["aliquot_id"]].append(
                                {"file_id": f.get("file_id")}
                            )
                # files_per_sample_dict = json.load(f_in)
        with open("files_per_sample_dict.json", "w") as dict_out:
            json.dump(files_per_sample_dict, dict_out)

        return files_per_sample_dict

    def _get_files_per_sample_dict(self, cache_file) -> dict:
        t0 = time.time()
        n = 0
        files_per_sample = defaultdict(list)
        sys.stderr.write("Started collecting files.\n")
        with gzip.open(cache_file, "wb") as f_out:
            writer = jsonlines.Writer(f_out)
            for fc in self._files_chunk():
                for f in fc:
                    aliquots = f.get("aliquots", [])
                    for aliquot in aliquots:
                        files_per_sample[aliquot["sample_id"]].append(
                            {"file_id": f.get("file_id")}
                        )
                        n += 1
                        files_per_sample[aliquot["aliquot_id"]].append(
                            {"file_id": f.get("file_id")}
                        )
                        n += 1
                    writer.write(f)  # Writes all file info to cache file
                sys.stderr.write(
                    f"Chunk completed. Wrote {n} sample-file pairs in {time.time() - t0}s\n"
                )
        sys.stderr.write(f"Wrote {n} sample-file pairs in {time.time() - t0}s\n")

        t1 = time.time()
        sys.stderr.write(
            f"Created a files look-up dict for {len(files_per_sample)} samples in {time.time() - t1}s\n"
        )
        sys.stderr.write(
            f"Entire files preparation completed in {time.time() - t0}s\n\n"
        )
        return files_per_sample

    def _metadata_files_chunk(self, file_ids=None):
        if file_ids:
            files = []
            for file_id in file_ids:
                result = retry_get(
                    self.endpoint, params={"query": query_metadata_file(file_id)}
                )
                files.extend(result.json()["data"]["fileMetadata"])
            yield files
        else:
            totalfiles = self._get_total_files()
            limit = 750
            for page in range(0, totalfiles, limit):
                sys.stderr.write(
                    f"<< Processing page {int(page/limit) + 1}/{ceil(totalfiles/limit)} Metadata Files >>\n"
                )
                result = retry_get(
                    self.endpoint, params={"query": query_files_bulk(page, limit)}
                )
                yield result.json()["data"]["fileMetadata"]

    def _UIfiles_chunk(self):
        totalfiles = self._get_total_uifiles()
        limit = 750
        for page in range(0, totalfiles, limit):
            sys.stderr.write(
                f"<< Processing page {int(page/limit) + 1}/{ceil(totalfiles/limit)}  UI Files >>\n"
            )
            sys.stderr.write("\n")
            result = retry_get(
                self.endpoint, params={"query": query_UIfiles_bulk(page, limit)}
            )
            yield result.json()["data"]["getPaginatedUIFile"]["uiFiles"]

    def _study_files_chunk(
        self,
    ):
        # loop over studies to get files
        jData = retry_get(self.endpoint, params={"query": make_all_programs_query()})
        AllPrograms = jData.json()["data"]["allPrograms"]
        for program in AllPrograms:
            for project in program["projects"]:
                project_id = project["project_submitter_id"]
                for study in project["studies"]:
                    pdc_study_id = study["pdc_study_id"]
                    if pdc_study_id is None:
                        sys.stderr.write("WTF\n")
                        sys.stderr.write(str(project))

                    study_files = retry_get(
                        self.endpoint, params={"query": query_study_files(pdc_study_id)}
                    )
                    files_recs = study_files.json()["data"]["filesPerStudy"]
                    files_recs_update = [
                        {**file, **{"project_submitter_id": project_id}}
                        for file in files_recs
                    ]
                    for file in files_recs_update:
                        if file["pdc_study_id"] is None:
                            sys.stderr.write("\nfile")
                            sys.stderr.write(str(file))
                            sys.stderr.write("\n")
                    yield files_recs_update
                    # if file["file_id"] in study_files_dict:
                    #    for field in append_fields:
                    #        study_files_dict[file["file_id"]][field].append(file[field])
                    # else:
                    #    study_files_dict[file["file_id"]] = file
                    #    for field in append_fields:
                    #        study_files_dict[field] = [file[field]]

    def _get_total_files(self):
        result = retry_get(self.endpoint, params={"query": query_files_paginated(0, 1)})
        return result.json()["data"]["getPaginatedFiles"]["total"]

    def _get_total_uifiles(self):
        result = retry_get(
            self.endpoint, params={"query": query_uifiles_paginated_total(0, 1)}
        )
        return result.json()["data"]["getPaginatedUIFile"]["total"]

    def demographics_for_study(self, study_id, limit):
        page = 1
        offset = 0
        demo_info = retry_get(
            self.endpoint, params={"query": case_demographics(study_id, offset, limit)}
        )
        out = demo_info.json()["data"]["paginatedCaseDemographicsPerStudy"][
            "caseDemographicsPerStudy"
        ]
        total_pages = demo_info.json()["data"]["paginatedCaseDemographicsPerStudy"][
            "pagination"
        ]["pages"]
        while page < total_pages:
            offset += limit
            demo_info = retry_get(
                self.endpoint,
                params={"query": case_demographics(study_id, offset, limit)},
            )
            out += demo_info.json()["data"]["paginatedCaseDemographicsPerStudy"][
                "caseDemographicsPerStudy"
            ]
            page += 1
        return out

    def diagnoses_for_study(self, study_id, limit):
        page = 1
        offset = 0
        diag_info = retry_get(
            self.endpoint, params={"query": case_diagnoses(study_id, offset, limit)}
        )
        out = diag_info.json()["data"]["paginatedCaseDiagnosesPerStudy"][
            "caseDiagnosesPerStudy"
        ]
        total_pages = diag_info.json()["data"]["paginatedCaseDiagnosesPerStudy"][
            "pagination"
        ]["pages"]
        while page < total_pages:
            offset += limit
            diag_info = retry_get(
                self.endpoint, params={"query": case_diagnoses(study_id, offset, limit)}
            )
            out += diag_info.json()["data"]["paginatedCaseDiagnosesPerStudy"][
                "caseDiagnosesPerStudy"
            ]
            page += 1
        return out

    def samples_for_study(self, study_id, limit):
        page = 1
        offset = 0
        samp_info = retry_get(
            self.endpoint, params={"query": case_samples(study_id, offset, limit)}
        )
        out = samp_info.json()["data"]["paginatedCasesSamplesAliquots"][
            "casesSamplesAliquots"
        ]
        total_pages = samp_info.json()["data"]["paginatedCasesSamplesAliquots"][
            "pagination"
        ]["pages"]
        while page < total_pages:
            offset += limit
            samp_info = retry_get(
                self.endpoint, params={"query": case_samples(study_id, offset, limit)}
            )
            out += samp_info.json()["data"]["paginatedCasesSamplesAliquots"][
                "casesSamplesAliquots"
            ]
            page += 1
        return out

    def taxon_for_study(self, study_id):
        taxon_info = retry_get(
            self.endpoint, params={"query": specimen_taxon(study_id)}
        )
        out = taxon_info.json()["data"]["biospecimenPerStudy"]
        seen = dict({})
        for case_taxon in out:
            if case_taxon["case_id"] not in seen:
                seen[case_taxon["case_id"]] = case_taxon["taxon"]
            else:
                if case_taxon["taxon"] != seen[case_taxon["case_id"]]:
                    print("taxon does not match for case_id:")
                    print(case_taxon["case_id"])
        return seen

    def add_case_info_to_files(
        self, file_ids, cases_out_file, files_out_file, cache_file
    ):
        # Make a dictionary where keys are case_id's and values are associated projects
        # Get info from the recently written cases info file (output_file)
        case_recs = defaultdict(list)
        sample_recs = defaultdict(list)
        with gzip.open(cases_out_file, "r") as fp:
            reader = jsonlines.Reader(fp)
            for case in reader:
                case.pop("files")
                for sample in case["samples"]:
                    sample.pop("files")
                    for aliquot in sample["aliquots"]:
                        aliquot.pop("files")
                    # sample_recs[sample.get('sample_id')].append(sample)
                # samples = case['samples']
                case_recs[case["case_id"]].append(case)
                # for sample in samples:
                # sample_recs[sample.get('sample_id')].append(sample)
        # sample_recs = remove_dups_from_dict_of_list_of_dicts(sample_recs)
        # for case,val in cases_and_associated_projects.items():
        #    cases_and_associated_projects[case] = list(set(cases_and_associated_projects[case]))
        # Have dictionary, now we can scan through files info and write new one with associated project
        sys.stderr.write(f"Got case_associated_projects\n")
        counter = 0
        with gzip.open(cache_file, "r") as fr:
            reader = jsonlines.Reader(fr)
            with gzip.open(files_out_file, "wb") as fw:
                writer = jsonlines.Writer(fw)
                for file in reader:
                    if file_ids is not None and file["file_id"] not in file_ids:
                        continue
                    file["project_submitter_id"] = []
                    file["cases"] = []
                    aliquot_ids = []
                    sample_ids = []
                    case_ids = []
                    for aliquot in file["aliquots"]:
                        aliquot_ids.append(aliquot["aliquot_id"])
                        sample_ids.append(aliquot["sample_id"])
                        case_ids.append(aliquot["case_id"])
                    aliquot_ids = list(set(aliquot_ids))
                    sample_ids = list(set(sample_ids))
                    case_ids = list(set(case_ids))
                    for case in case_ids:
                        case_copy = case_recs[case].copy()
                        for record in case_copy:
                            file["project_submitter_id"].append(
                                record.get("project_submitter_id")
                            )
                            for sample in record["samples"]:
                                if sample["sample_id"] in sample_ids:
                                    for aliquot in sample["aliquots"]:
                                        if aliquot["aliquot_id"] not in aliquot_ids:
                                            sample["aliquots"].remove(aliquot)
                                else:
                                    record["samples"].remove(sample)
                        file["cases"].extend(case_copy)

                    file["project_submitter_id"] = list(
                        set(file["project_submitter_id"])
                    )
                    if len(file["project_submitter_id"]) == 1:
                        file["project_submitter_id"] = file["project_submitter_id"][0]
                    elif len(file["project_submitter_id"]) > 1:
                        print(
                            "more than one project_submitter_id for file: "
                            + file["file_id"]
                        )
                        print(str(file["project_submitter_id"]))
                    # sample_ids = list(set(sample_ids))
                    # aliquot_ids = list(set(aliquot_ids))
                    # file['samples'] = []
                    # file['samples'] = [v[0] for k, v in sample_recs.items() if k in sample_ids]
                    # for index in range(len(file['samples'])):
                    #    file['samples'][index]['aliquots'] = [v for v in file['samples'][index]['aliquots']
                    #        if v['aliquot_id'] in aliquot_ids]
                    #    for aliquot in file['samples'][index]['aliquots']:
                    #        try:
                    #            aliquot.pop('files')
                    #        except:
                    #            continue
                    #    try:
                    #        file['samples'][index].pop('files')
                    #    except:
                    #        continue
                    file.pop("aliquots")
                    writer.write(file)
                    counter += 1
                    if counter % 500 == 0:
                        print(str(counter) + " files written")


def get_file_metadata(file_metadata_record) -> dict:
    return {
        field: file_metadata_record.get(field)
        for field in [
            "file_id",
            "file_name",
            "file_location",
            "file_submitter_id",
            "file_type",
            "file_format",
            "file_size",
            "data_category",
            "experiment_type",
            "md5sum",
            "dbgap_control_number",
        ]
    }


def agg_cases_info_for_study(study, demo, diag, sample, taxon, added_info):
    out = []
    for demo_case in demo:
        case_id = demo_case["case_id"]
        demo_case.update(added_info)
        demo_case["taxon"] = taxon[case_id]
        for diag_ind in range(len(diag)):
            if diag[diag_ind]["case_id"] == case_id:
                demo_case["diagnoses"] = diag.pop(diag_ind)["diagnoses"]
                break
        for sample_index in range(len(sample)):
            if sample[sample_index]["case_id"] == case_id:
                demo_case["samples"] = sample[sample_index]["samples"].copy()
                sample.pop(sample_index)
                break
        demo_case["study"] = study
        out.append(demo_case)
    return out


def remove_dups_from_dict_of_list_of_dicts(records):
    for k, vals in records.items():
        counter = len(vals) - 1
        while counter > 0:
            if vals[0] == vals[counter]:
                vals.pop(counter)
            counter -= 1
            # else:
    return records


def main():
    parser = argparse.ArgumentParser(description="Pull case data from PDC API.")
    parser.add_argument(
        "out_file", help="Out cases endpoint file name. Should end with .gz"
    )
    parser.add_argument("--endpoint", help="endpoint to extract, if bulk")
    # parser.add_argument("file_linkage", help="Used to link files and specimens/cases")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    parser.add_argument("--file", help="Extract just this file")
    parser.add_argument(
        "--files", help="Optional file with list of file ids (one to a line)"
    )
    parser.add_argument(
        "--spec_out_file",
        help="Name of specimen_id: file_ids output file. Should end with .json.gz",
    )
    args = parser.parse_args()
    print(str(get_case_ids(case=args.file, case_list_file=args.files)))
    pdc = PDC(make_spec_file=args.spec_out_file)

    if args.case or args.cases or args.endpoint == "cases":
        pdc.save_cases(
            args.out_file,
            case_ids=get_case_ids(case=args.case, case_list_file=args.cases),
        )
    if args.file or args.files or args.endpoint == "files":
        pdc.save_files(
            args.out_file,
            file_ids=get_case_ids(case=args.file, case_list_file=args.files),
        )
    # if not (pathlib.Path(args.cases_out_file).exists()):
    #    pdc.save_cases(
    #        args.cases_out_file,
    #        case_ids=get_case_ids(case=args.case, case_list_file=args.cases),
    #    )
    # if args.files_out_file is not None:
    #    print("making files file")
    #    pdc.add_case_info_to_files(
    #        get_case_ids(case=args.file, case_list_file=args.files),
    #        args.cases_out_file,
    #        args.files_out_file,
    #        args.cache_file,
    #    )


if __name__ == "__main__":
    main()

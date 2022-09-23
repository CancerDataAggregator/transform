import json
from typing import Iterable
from collections import defaultdict
from math import ceil
import jsonlines
import time
import sys
import gzip
import argparse
import pathlib
import shutil

from cdatransform.lib import get_ids
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
        case_ids:list[str]=None,
    ) -> Iterable[dict[str,str|int|list]]:

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
                case:dict[str,str|int|list] = {
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
            # Determine
            # Get list of Programs, projects per program, studies per project
            jData = retry_get(
                self.endpoint, params={"query": make_all_programs_query()}
            )
            AllPrograms:list[dict[str,str|int|list]] = jData.json()["data"]["allPrograms"]
            # Loop over studies, and get demographics, diagnoses, samples, and taxon
            out = []
            for program in AllPrograms:
                for project in program["projects"]:
                    added_info = {"project_submitter_id": project["project_submitter_id"]}
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

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                writer.write(case)
                n += 1
                if n % 500 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

    def save_files(self, out_file, file_ids=None):
        t0 = time.time()
        n = 0
        # Get and write metadata_files_chunks
        # This portion gets all file info from metadata query, and
        # makes a dictionary of specimen_id: file_ids. the linking of specimen_id: file_ids
        # is not needed on our end, but leaving it for now
        specimen_files_dict = defaultdict(list)
        with gzip.open("pdc_meta_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for chunk in self._metadata_files_chunk(file_ids):
                for rec in chunk:
                    if file_ids is None or rec["file_id"] in file_ids:
                        writer.write(rec)
                        # add to specimen:file dictionary
                        for aliquot in rec.get("aliquots", []):
                            specimen_files_dict[aliquot["sample_id"]].append(
                                rec["file_id"]
                            )
                            specimen_files_dict[aliquot["aliquot_id"]].append(
                                rec["file_id"]
                            )
                        n += 1
                        if n % 500 == 0:
                            sys.stderr.write(
                                f"Wrote {n} metadata files in {time.time() - t0}s\n"
                            )
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
        with gzip.open("pdc_uifiles_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for chunk in self._UIfiles_chunk():
                for rec in chunk:
                    if file_ids is None or rec["file_id"] in file_ids:
                        writer.write(rec)
                    n += 1
                    if n % 500 == 0:
                        sys.stderr.write(f"Wrote {n} ui_files in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} ui_files in {time.time() - t0}s\n")
        # Get and write files from studies. Similar to bulk cases, loop over program,
        # project, study and extract files per study
        n = 0
        with gzip.open("pdc_studyfiles_out.jsonl.gz", "wb") as fp:
            writer = jsonlines.Writer(fp)
            for chunk in self._study_files_chunk():
                for file in chunk:
                    if file_ids is None or file["file_id"] in file_ids:
                        writer.write(file)
                        n += 1
                        if n % 5000 == 0:
                            sys.stderr.write(
                                f"Pulled {n} study files in {time.time() - t0}s\n"
                            )
        sys.stderr.write(f"Pulled {n} study files in {time.time() - t0}s\n")
        # concatenate metadata and studyfiles for CDA to transform. THIS IS THE
        # FILE TO USE FOR TRANSFORMATION! All other files are for the cloud resources
        # (ISB-CGC, Seven Bridges, Terra) to use instead of using PDC API
        with gzip.open(out_file, "wb") as f_out:
            for f in ["pdc_meta_out.jsonl.gz", "pdc_studyfiles_out.jsonl.gz"]:
                with gzip.open(f) as f_in:
                    shutil.copyfileobj(f_in, f_out)

    def _metadata_files_chunk(self, file_ids=None)->Iterable[list[dict]]:
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

    def _UIfiles_chunk(self)->Iterable[list[dict]]:
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
    )->Iterable[list[dict]]:
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
                            exit()
                    yield files_recs_update

    def _get_total_files(self)->int:
        result = retry_get(self.endpoint, params={"query": query_files_paginated(0, 1)})
        return result.json()["data"]["getPaginatedFiles"]["total"]

    def _get_total_uifiles(self)->int:
        result = retry_get(
            self.endpoint, params={"query": query_uifiles_paginated_total(0, 1)}
        )
        return result.json()["data"]["getPaginatedUIFile"]["total"]

    def demographics_for_study(self, study_id, limit)->:
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
            out.append(demo_info.json()["data"]["paginatedCaseDemographicsPerStudy"][
                "caseDemographicsPerStudy"
            ])
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
        seen:dict = {}
        for case_taxon in out:
            if case_taxon["case_id"] not in seen:
                seen[case_taxon["case_id"]] = case_taxon["taxon"]
            else:
                if case_taxon["taxon"] != seen[case_taxon["case_id"]]:
                    print("taxon does not match for case_id:")
                    print(case_taxon["case_id"])
        return seen


def agg_cases_info_for_study(study:list[dict], demo:list[dict], diag:list[dict], sample:list[dict], taxon:dict, added_info:dict)->list[dict]:
    out:list[dict] = []
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
    pdc = PDC(make_spec_file=args.spec_out_file)

    if args.case or args.cases or args.endpoint == "cases":
        pdc.save_cases(
            args.out_file,
            case_ids=get_ids(id=args.case, id_list_file=args.cases),
        )
    if args.file or args.files or args.endpoint == "files":
        pdc.save_files(
            args.out_file,
            file_ids=get_ids(id=args.file, id_list_file=args.files),
        )


if __name__ == "__main__":
    main()

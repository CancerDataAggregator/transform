import json
from collections import defaultdict
from math import ceil
import jsonlines
import time
import sys
import gzip
import argparse
import pathlib

from cdatransform.lib import get_case_ids
from .lib import retry_get
from .pdc_query_lib import (
    query_all_cases,
    query_files_bulk,
    query_files_paginated,
    make_all_programs_query,
)
from .pdc_query_lib import (
    make_study_query,
    case_demographics,
    case_diagnoses,
    case_samples,
    specimen_taxon,
)


class PDC:
    def __init__(self, cache_file, endpoint="https://pdc.cancer.gov/graphql") -> None:
        self.endpoint = endpoint
        self.temp_files_file = 'temp_cache.jsonl.gz'
        self._files_per_sample_dict = self._fetch_file_data_from_cache(cache_file)


    def cases(
        self,
        case_ids=None,
    ):

        # if case_ids is None:
        #    case_ids = self.get_case_id_list()
        jData = retry_get(self.endpoint, params={"query": make_all_programs_query()})
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
                        self.endpoint, params={"query": make_study_query(pdc_study_id)}
                    )
                    study_rec = study_info.json()["data"]["study"][0]
                    study_rec.update(study)
                    # Get demographic info
                    dem = self.demographics_for_study(pdc_study_id, 100)
                    if case_ids is not None:
                        dem = self.filter_cases(dem, case_ids)
                        if dem == []:
                            continue
                    # Get diagnosis info
                    diag = self.diagnoses_for_study(pdc_study_id, 100)
                    if case_ids is not None:
                        diag = self.filter_cases(diag, case_ids)
                    # Get samples info
                    samp = self.samples_for_study(pdc_study_id, 100)
                    if case_ids is not None:
                        samp = self.filter_cases(samp, case_ids)
                    taxon = self.taxon_for_study(pdc_study_id)
                    out = agg_cases_info_for_study(
                        study_rec, dem, diag, samp, taxon, added_info
                    )
                    for case in out:
                        yield case
        # for case_id in case_ids:
        #    result = retry_get(
        #        self.endpoint, params={"query": query_single_case(case_id=case_id)}
        #    )
        #    yield result.json()["data"]["case"][0]

    def get_case_id_list(self):
        result = retry_get(self.endpoint, params={"query": query_all_cases()})
        for case in result.json()["data"]["allCases"]:
            yield case["case_id"]

    def save_cases(self, out_file, case_ids=None):
        t0 = time.time()
        n = 0
        with gzip.open(out_file, "wb") as fp:
            writer = jsonlines.Writer(fp)
            for case in self.cases(case_ids):
                samples_files_list = []
                for index, sample in enumerate(case["samples"]):
                    # Based on the PDC data model, all files in PDC are associated
                    # with samples/aliquots. Can append all samples files
                    if self._files_per_sample_dict.get(sample["sample_id"]) is not None:
                        samples_files_list += self._files_per_sample_dict.get(
                            sample["sample_id"]
                        )
                    case["samples"][index]["files"] = samples_files_list
                    for index_aliquot, aliquot in enumerate(
                        case["samples"][index]["aliquots"]
                    ):
                        case["samples"][index]["aliquots"][index_aliquot][
                            "files"
                        ] = self._files_per_sample_dict.get(aliquot["aliquot_id"])
                case["files"] = list(
                    {v["file_id"]: v for v in samples_files_list}.values()
                )
                writer.write(case)
                n += 1
                if n % 100 == 0:
                    sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")
        sys.stderr.write(f"Wrote {n} cases in {time.time() - t0}s\n")

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
            files_per_sample_dict = {}
            with gzip.open(cache_file, "r") as f_in:
                for f in f_in:
                    aliquots = f.get("aliquots")
                    if aliquots:
                        for aliquot in aliquots:
                            files_per_sample_dict[aliquot["sample_id"]].append({'file_id': f.get('file_id')})
                            files_per_sample_dict[aliquot["aliquot_id"]].append({'file_id': f.get('file_id')})
                #files_per_sample_dict = json.load(f_in)
        return files_per_sample_dict

    def _get_files_per_sample_dict(self) -> dict:
        t0 = time.time()
        n = 0
        files_per_sample = defaultdict(list)
        sys.stderr.write("Started collecting files.\n")
        with gzip.open(self.temp_files_file, "wt") as f_out:
            writer = jsonlines.Writer(f_out)
            for fc in self._files_chunk():
                for f in fc:
                    aliquots = f.get("aliquots")
                    if aliquots:
                        for aliquot in aliquots:
                            files_per_sample[aliquot["sample_id"]].append({'file_id': f.get('file_id')})
                            n += 1
                            files_per_sample[aliquot["aliquot_id"]].append({'file_id': f.get('file_id')})
                            n += 1
                    writer.write(f)
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

    def _files_chunk(self):
        totalfiles = self._get_total_files()
        limit = 750
        for page in range(0, totalfiles, limit):
            sys.stderr.write(
                f"<< Processing page {int(page/limit) + 1}/{ceil(totalfiles/limit)} >>\n"
            )
            result = retry_get(
                self.endpoint, params={"query": query_files_bulk(page, limit)}
            )
            yield result.json()["data"]["fileMetadata"]

    def _get_total_files(self):
        result = retry_get(self.endpoint, params={"query": query_files_paginated(0, 1)})
        return result.json()["data"]["getPaginatedFiles"]["total"]

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

    def add_case_info_to_files(self, out_file, cache_file):
        # Make a dictionary where keys are case_id's and values are associated projects
        # Get info from the recently written cases info file (output_file)
        cases_and_associated_projects = {}
        with gzip.open(out_file, "r") as fp:
            reader = jsonlines.Reader(fp)
            for case in reader:
                if case['case_id'] in cases_and_associated_projects:
                    cases_and_associated_projects[case['case_id']].append(case['project_submitter_id'])
                else:
                    cases_and_associated_projects[case['case_id']] = [case['project_submitter_id']]
        for case,val in cases_and_associated_projects.items():
            cases_and_associated_projects[case] = list(set(cases_and_associated_projects[case]))
        # Have dictionary, now we can scan through files info and write new one with associated project
        sys.stderr.write(f"Got case_associated_projects\n")
        with gzip.open(self.temp_files_file, "r") as fr:
            reader = jsonlines.Reader(fr)
            with gzip.open(cache_file, "wb") as fw:
                writer = jsonlines.Writer(fw)
                for file in reader:
                    file["project_submitter_id"] = []      
                    for aliquot in file["aliquots"]:
                        file["project_submitter_id"].extend(cases_and_associated_projects[aliquot["case_id"]])
                    file["project_submitter_id"] = list(set(file["project_submitter_id"]))
                    writer.write(file)

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


def main():
    parser = argparse.ArgumentParser(description="Pull case data from PDC API.")
    parser.add_argument("out_file", help="Out file name. Should end with .gz")
    parser.add_argument("cache_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("file_linkage", help="Used to link files and specimens/cases")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    args = parser.parse_args()

    pdc = PDC(pathlib.Path(args.cache_file), pathlib.Path(args.file_linkage))
    #pdc.save_cases(
    ##    args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    #)
    pdc.add_case_info_to_files(args.out_file, args.cache_file)

if __name__ == "__main__":
    main()

import json
from typing import AsyncGenerator, Generator, Iterable, Union
from collections import defaultdict
from math import ceil
import jsonlines
import time
import sys
import gzip
import argparse
import gzip
import json
import pathlib
import shutil
import sys
import aiohttp
from collections import defaultdict
from math import ceil
import asyncio

from dags.cdatransform.services.storage_service import StorageService

from .extractor import Extractor

from .lib import retry_get
from .pdc_query_lib import *


class hashabledict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))


class PDC(Extractor):
    def __init__(
        self,
        dest_bucket,
        uuid,
        endpoint="https://pdc.cancer.gov/graphql",
        make_spec_file=None
    ) -> None:
        self.endpoint = endpoint
        self.make_spec_file = make_spec_file
        self.uuid = uuid
        super().__init__(dest_bucket=dest_bucket)

    def _get_initial_results(self, query_info, page_key, study_key):
        out = query_info["data"][page_key][study_key]
        total_pages = query_info["data"][page_key]["pagination"]["pages"]

        return (out, total_pages)

    def _extend_results(self, out, results, page_key, study_key):
        for results_info in results:
            out.extend(results_info["data"][page_key][study_key])

        return out

    async def _paginate_files_or_cases(
        self,
        endpt: str,
        ids: Union[list, None] = None,
        page_size: int = 500,
        num_field_chunks: int = 2,
        session=None,
        loop: asyncio.AbstractEventLoop = None,
    ):

        # if case_ids:
        #     # Have one case_id or list of case_ids from a file.
        #     # Get all info from the case endpoint query in PDC. Need to split fields
        #     # for the queries and merge. PDC hates long query strings
        #     # Note: used mostly for testing. Does not get Study info for each case
        #     for case_id in case_ids:
        #         future_a = retry_get(
        #             self.endpoint, params={"query": query_single_case_a(case_id)}
        #         )
        #         print("got part a")
        #         future_b = retry_get(
        #             self.endpoint, params={"query": query_single_case_b(case_id)}
        #         )
        #         print("got part b")
        #         case_a, case_b = await asyncio.gather(future_a, future_b)
        #         # Merge two halves of case info
        #         case: dict[str, str | int | list] = {
        #             **case_a.json()["data"]["case"][0],
        #             **case_b.json()["data"]["case"][0],
        #         }
        #         print("merged_dicts")
        #         yield case
        # else:
        # Downloading bulk case info. Used for populating data. PDC API is
        # awful, and has no good way to get ALL bulk info for cases. Must
        # loop over all programs, projects, studies and extract case demographics,
        # diagnoses, sample/aliquot, and taxon info per study, and merge results.
        # Determine
        # Get list of Programs, projects per program, studies per project
        jData = await retry_get(
            session=session,
            endpoint=self.endpoint,
            params={"query": make_all_programs_query()},
        )
        AllPrograms: list[dict[str, Union[str, int, list]]] = jData["data"]["allPrograms"]
        # Loop over studies, and get demographics, diagnoses, samples, and taxon
        out = []
        for program in AllPrograms:
            for project in program["projects"]:
                added_info = {"project_submitter_id": project["project_submitter_id"]}
                for study in project["studies"]:
                    # get study_id and embargo_date, and other info for study
                    pdc_study_id = study["pdc_study_id"]
                    study_info_future = retry_get(
                        session=session,
                        endpoint=self.endpoint,
                        params={"query": make_study_query(pdc_study_id)},
                    )

                    initial_dem_future = retry_get(
                        session=session,
                        endpoint=self.endpoint,
                        params={"query": case_demographics(pdc_study_id, 0, 100)},
                    )

                    initial_diag_future = retry_get(
                        session=session,
                        endpoint=self.endpoint,
                        params={"query": case_diagnoses(pdc_study_id, 0, 100)},
                    )

                    initial_samp_future = retry_get(
                        session=session,
                        endpoint=self.endpoint,
                        params={"query": case_samples(pdc_study_id, 0, 100)},
                    )

                    # Get taxon info
                    taxon_future = self.taxon_for_study(pdc_study_id, session)

                    (
                        study_info,
                        initial_dem,
                        initial_diag,
                        initial_samp,
                        taxon,
                    ) = await asyncio.gather(
                        *[
                            study_info_future,
                            initial_dem_future,
                            initial_diag_future,
                            initial_samp_future,
                            taxon_future,
                        ]
                    )

                    dem_out, dem_total_pages = self._get_initial_results(
                        initial_dem,
                        "paginatedCaseDemographicsPerStudy",
                        "caseDemographicsPerStudy",
                    )
                    diag_out, diag_total_pages = self._get_initial_results(
                        initial_diag,
                        "paginatedCaseDiagnosesPerStudy",
                        "caseDiagnosesPerStudy",
                    )
                    samp_out, samp_total_pages = self._get_initial_results(
                        initial_samp,
                        "paginatedCasesSamplesAliquots",
                        "casesSamplesAliquots",
                    )

                    futures = []
                    # Get demographic info
                    futures.extend(
                        self.get_all_results_async(
                            pdc_study_id,
                            100,
                            case_demographics,
                            session,
                            dem_total_pages,
                        )
                    )
                    # Get diagnosis info
                    futures.extend(
                        self.get_all_results_async(
                            pdc_study_id, 100, case_diagnoses, session, diag_total_pages
                        )
                    )
                    # Get samples info
                    futures.extend(
                        self.get_all_results_async(
                            pdc_study_id, 100, case_samples, session, samp_total_pages
                        )
                    )

                    page_results = await asyncio.gather(*futures)

                    for page_result in page_results:
                        if "paginatedCaseDemographicsPerStudy" in page_result["data"]:
                            dem_out = self._extend_results(
                                dem_out,
                                [page_result],
                                "paginatedCaseDemographicsPerStudy",
                                "caseDemographicsPerStudy",
                            )
                        if "paginatedCaseDiagnosesPerStudy" in page_result["data"]:
                            diag_out = self._extend_results(
                                diag_out,
                                [page_result],
                                "paginatedCaseDiagnosesPerStudy",
                                "caseDiagnosesPerStudy",
                            )
                        if "paginatedCasesSamplesAliquots" in page_result["data"]:
                            samp_out = self._extend_results(
                                samp_out,
                                [page_result],
                                "paginatedCasesSamplesAliquots",
                                "casesSamplesAliquots",
                            )

                    study_rec = study_info["data"]["study"][0]
                    study_rec.update(study)

                    # Aggregate all case info for this study
                    out = agg_cases_info_for_study(
                        study_rec, dem_out, diag_out, samp_out, taxon, added_info
                    )
                    for case in out:
                        yield case

    def save_cases(self, out_file, case_ids=None):
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(self.http_call_save_files_or_cases(case_ids, out_file=out_file))

    def save_files(self, out_file, file_ids=None):
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(self._get_all_files(out_file, file_ids))

    async def _get_all_files(self, out_file, file_ids=None):
        t0 = time.time()
        specimen_files_dict = {}
        # Get and write metadata_files_chunks
        # This portion gets all file info from metadata query, and
        # makes a dictionary of specimen_id: file_ids. the linking of specimen_id: file_ids
        # is not needed on our end, but leaving it for now
        n = 0
        specimen_files_dict = defaultdict(list)
        #with gzip.open("pdc_meta_out.jsonl.gz", "wb") as fp:
        meta_out_path = f"{self.dest_bucket}/pdc.meta_out.{self.uuid}.jsonl.gz"
        async with aiohttp.ClientSession() as session:
            with self.storage_service.get_session(
                gcp_buck_path=meta_out_path,
                mode="w") as fp:
                with jsonlines.Writer(fp) as writer:
                    async for chunk in self._metadata_files_chunk(session, file_ids):
                        for rec in chunk:
                            if file_ids is None or rec["file_id"] in file_ids:
                                writer.write(rec)
                                # add to specimen:file dictionary
                                if rec.get("aliquots") is not None:
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
        # Get and write UIfile chunks - PDC does not support using any UI calls, but
        # Seven Bridges does it... so here we are. Note: CDA does not use anything from
        # the UI files, and it is strictly for Seven Bridges benefit
        n = 0
        #with gzip.open("pdc_uifiles_out.jsonl.gz", "wb") as fp:
        async with aiohttp.ClientSession() as session:
            with self.storage_service.get_session(
                gcp_buck_path=f"{self.dest_bucket}/pdc.uifiles_out.{self.uuid}.jsonl.gz",
                mode="w") as fp:
                with jsonlines.Writer(fp) as writer:
                    async for chunk in self._UIfiles_chunk(session):
                        for rec in chunk:
                            if file_ids is None or rec["file_id"] in file_ids:
                                writer.write(rec)
                            n += 1
                            if n % 500 == 0:
                                sys.stderr.write(
                                    f"Wrote {n} ui_files in {time.time() - t0}s\n"
                                )
        sys.stderr.write(f"Wrote {n} ui_files in {time.time() - t0}s\n")
        # Get and write files from studies. Similar to bulk cases, loop over program,
        # project, study and extract files per study
        n = 0
        #with gzip.open("pdc_studyfiles_out.jsonl.gz", "wb") as fp:
        study_files_path = f"{self.dest_bucket}/pdc.studyfiles_out.{self.uuid}.jsonl.gz"
        async with aiohttp.ClientSession() as session:
            with self.storage_service.get_session(
                gcp_buck_path=study_files_path,
                mode="w"
            ) as fp:
                with jsonlines.Writer(fp) as writer:
                    async for chunk in self._study_files_chunk(session):
                        for file in chunk:
                            if file_ids is None or file["file_id"] in file_ids:
                                writer.write(file)
                                n += 1
                                if n % 5000 == 0:
                                    sys.stderr.write(
                                        f"Pulled {n} study files in {time.time() - t0}s\n"
                                    )
        sys.stderr.write(f"Pulled {n} study files in {time.time() - t0}s\n")

        # Write specimen_files_dict to file
        if self.make_spec_file:
            for specimen, files in specimen_files_dict.items():
                specimen_files_dict[specimen] = list(set(files))
            with gzip.open(self.make_spec_file, "wt", encoding="ascii") as out:
                json.dump(specimen_files_dict, out)

        # concatenate metadata and studyfiles for CDA to transform. THIS IS THE
        # FILE TO USE FOR TRANSFORMATION! All other files are for the cloud resources
        # (ISB-CGC, Seven Bridges, Terra) to use instead of using PDC API
        #with gzip.open(out_file, "wb") as f_out:
        #with self.storage_service.get_session(gcp_buck_path=f"{self.dest_bucket}/{out_file}", mode="w") as f_out:
            # for f in ["pdc_meta_out.jsonl.gz", "pdc_studyfiles_out.jsonl.gz"]:
            #     with gzip.open(f) as f_in:
            #         shutil.copyfileobj(f_in, f_out)
        out_path = f"{self.dest_bucket}/{out_file}"
        self.storage_service.compose_blobs([meta_out_path, study_files_path], self.dest_bucket, out_path)
        return out_path

    async def _metadata_files_chunk(self, session, file_ids=None) -> AsyncGenerator[list, dict]:
        futures = []

        if file_ids:
            files = []
            for file_id in file_ids:
                result = await retry_get(
                    session=session,
                    endpoint=self.endpoint,
                    params={"query": query_metadata_file(file_id)},
                )
                files.extend(result["data"]["fileMetadata"])
            yield files
        else:
            totalfiles = await self._get_total_files(session)
            limit = 750
            future_limit = 5
            for page in range(0, totalfiles, limit):
                # sys.stderr.write(
                #     f"<< Processing page {int(page/limit) + 1}/{ceil(totalfiles/limit)} Metadata Files >>\n"
                # )
                futures.append(
                    retry_get(
                        session=session,
                        endpoint=self.endpoint,
                        params={"query": query_files_bulk(page, limit)},
                    )
                )

                if len(futures) == future_limit:
                    results = await asyncio.gather(*futures)

                    futures = []

                    for result in results:
                        yield result["data"]["fileMetadata"]
                # yield result["data"]["fileMetadata"]
            if len(futures) > 0:
                results = await asyncio.gather(*futures)

                futures = []

                for result in results:
                    yield result["data"]["fileMetadata"]

    async def _UIfiles_chunk(self, session) -> AsyncGenerator[list, dict]:
        totalfiles = await self._get_total_uifiles(session)
        limit = 750
        futures = []

        for page in range(0, totalfiles, limit):
            # sys.stderr.write(
            #     f"<< Processing page {int(page/limit) + 1}/{ceil(totalfiles/limit)}  UI Files >>\n"
            # )
            sys.stderr.write("\n")
            futures.append(
                retry_get(
                    session=session,
                    endpoint=self.endpoint,
                    params={"query": query_UIfiles_bulk(page, limit)},
                )
            )

            if len(futures) == 5:
                results = await asyncio.gather(*futures)

                futures = []

                for result in results:
                    yield result["data"]["getPaginatedUIFile"]["uiFiles"]

        if len(futures) > 0:
            results = await asyncio.gather(*futures)

            futures = []

            for result in results:
                yield result["data"]["getPaginatedUIFile"]["uiFiles"]

    async def _study_files_chunk(
        self,
        session
    ) -> AsyncGenerator[list, dict]:
        # loop over studies to get files
        jData = await retry_get(
            session=session,
            endpoint=self.endpoint,
            params={"query": make_all_programs_query()},
        )
        AllPrograms = jData["data"]["allPrograms"]
        for program in AllPrograms:
            for project in program["projects"]:
                project_id = project["project_submitter_id"]
                futures = []
                num_studies = len(project["studies"])
                for study in project["studies"]:
                    pdc_study_id = study["pdc_study_id"]
                    if pdc_study_id is None:
                        sys.stderr.write("WTF\n")
                        sys.stderr.write(str(project))

                    futures.append(
                        retry_get(
                            session=session,
                            endpoint=self.endpoint,
                            params={"query": query_study_files(pdc_study_id)},
                        )
                    )
                    num_studies -= 1

                    if num_studies == 0 or len(futures) == 2:
                        results = await asyncio.gather(*futures)

                        futures = []

                        for study_files in results:
                            files_recs = study_files["data"]["filesPerStudy"]
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

    async def _get_total_files(self, session) -> int:
        result = await retry_get(
            session=session,
            endpoint=self.endpoint,
            params={"query": query_files_paginated(0, 1)},
        )
        return result["data"]["getPaginatedFiles"]["total"]

    async def _get_total_uifiles(self, session) -> int:
        result = await retry_get(
            session=session,
            endpoint=self.endpoint,
            params={"query": query_uifiles_paginated_total(0, 1)},
        )
        return result["data"]["getPaginatedUIFile"]["total"]

    def get_all_results_async(self, study_id, limit, query_func, session, total_pages):
        page = 1
        offset = 0

        futures = []

        while True:
            offset += limit

            if page >= total_pages:
                break

            futures.append(
                retry_get(
                    session=session,
                    endpoint=self.endpoint,
                    params={"query": query_func(study_id, offset, limit)},
                )
            )

            page += 1

        return futures

    async def demographics_for_study(self, study_id, limit, session, total_pages):
        return await self.get_all_results_async(
            study_id,
            limit,
            case_demographics,
            "paginatedCaseDemographicsPerStudy",
            "caseDemographicsPerStudy",
            session,
            total_pages,
        )

    async def diagnoses_for_study(self, study_id, limit, session, total_pages):
        return await self.get_all_results_async(
            study_id,
            limit,
            case_diagnoses,
            "paginatedCaseDiagnosesPerStudy",
            "caseDiagnosesPerStudy",
            session,
            total_pages,
        )

    async def samples_for_study(self, study_id, limit, session, total_pages):
        return await self.get_all_results_async(
            study_id,
            limit,
            case_samples,
            "paginatedCasesSamplesAliquots",
            "casesSamplesAliquots",
            session,
            total_pages,
        )

    async def taxon_for_study(self, study_id, session=None):
        taxon_info = await retry_get(
            session=session,
            endpoint=self.endpoint,
            params={"query": specimen_taxon(study_id)},
        )
        out = taxon_info["data"]["biospecimenPerStudy"]
        seen: dict = {}
        for case_taxon in out:
            if case_taxon["case_id"] not in seen:
                seen[case_taxon["case_id"]] = case_taxon["taxon"]
            else:
                if case_taxon["taxon"] != seen[case_taxon["case_id"]]:
                    print("taxon does not match for case_id:")
                    print(case_taxon["case_id"])
        return seen


def agg_cases_info_for_study(
    study: list,
    demo: list,
    diag: list,
    sample: list,
    taxon: dict,
    added_info: dict,
) -> list:
    out: list = []
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

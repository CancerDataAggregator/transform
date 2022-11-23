import asyncio
import sys
from time import time
import jsonlines
import gzip

import requests
from google.cloud import storage
import json
import aiohttp
from typing import Union
import os
from google.cloud.storage import Client
from smart_open import open
from cdatransform.services.storage_service import StorageService
from cdatransform.models.extraction_result import ExtractionResult


class Extractor:
    def __init__(self,
                 dest_bucket:str,
                 *args,
                 **kwargs):
        self.storage_service = StorageService()
        self.file_index = -1  # this will set file to 0 in the loop
        self.dest_bucket = dest_bucket
        self.items = []

    def current_time_rate(self, t0):
        return round(time() - t0, 2)

    def write_file(self, out_file, cases, t):
        with gzip.open(out_file, "wb") as fp:
            with jsonlines.Writer(fp) as wri:
                for index, value in enumerate(cases):
                    index += 1
                    if index % 50 == 0:
                        print(
                            f"Wrote {index} cases in {self.current_time_rate(t)}s\n",
                            flush=True,
                        )
                    wri.write(value)

    async def _paginate_files_or_cases(
        self,
        endpt: str,
        ids: Union[list, None] = None,
        page_size: int = 500,
        num_field_chunks: int = 2,
        session=None,
    ):
        raise "Not Implemented"

    async def http_call_save_files_or_cases(
        self,
        endpoint: str = "",
        end_cases: Union[list, None] = None,
        case_ids=None,
        page_size: int = 500,
        num_field_chunks: int = 3,
        out_file: str = "",
    ) -> str:
        """
        this will call the case api asynchronously and write the each case to a array
        Args:
            end_cases (Union[list, None], optional): _description_. Defaults to None.
            case_ids (_type_, optional): _description_. Defaults to None.
            page_size (int, optional): _description_. Defaults to 500.
        """
        t0 = time()
        end_cases = end_cases or []
        output_path = f"{self.dest_bucket}/{out_file}"
        files_written = 0
        async with aiohttp.ClientSession() as session:
            with self.storage_service.get_session(gcp_buck_path=output_path, mode="w") as fp:
                with jsonlines.Writer(fp) as wri:
                    async for case in self._paginate_files_or_cases(
                        ids=case_ids,
                        endpt=endpoint,
                        page_size=page_size,
                        num_field_chunks=num_field_chunks,
                        session=session,
                    ):
                        files_written += 1
                        if (files_written % 50 == 0):
                            print(f"Wrote {files_written} rows in {time() - t0}s\n")
                        wri.write(case)
        return output_path

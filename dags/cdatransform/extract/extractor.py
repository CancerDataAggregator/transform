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


class Extractor:
    def __init__(self, *args, **kwargs):
        pass

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
    ):
        raise "Not Implemented"

    async def http_call_save_files_or_cases(
        self,
        endpoint: str = "",
        end_cases: Union[list, None] = None,
        case_ids=None,
        page_size: int = 500,
        num_field_chunks: int = 3,
        out_file:str = ""
    ):
        """
        this will call the case api asynchronously and write the each case to a array
        Args:
            end_cases (Union[list, None], optional): _description_. Defaults to None.
            case_ids (_type_, optional): _description_. Defaults to None.
            page_size (int, optional): _description_. Defaults to 500.
        """
        n = 0
        t0 = time()
        end_cases = end_cases or []
        async for case in self._paginate_files_or_cases(
            ids=case_ids,
            endpt=endpoint,
            page_size=page_size,
            num_field_chunks=num_field_chunks,
        ):
            end_cases.append(case)
            if len(end_cases) >= 50_000:
                end_cases = self._write_items_and_clear(end_cases, out_file, t0)

        if (len(end_cases) > 0):
            end_cases = self._write_items_and_clear(end_cases, out_file, t0)
        return end_cases

    def _write_items_and_clear(self, items, out_file, t0):
        with gzip.open(out_file, "ab") as fp:
                with jsonlines.Writer(fp) as wri:
                    for index, value in enumerate(items):
                        index += 1
                        if index % 50 == 0:
                            print(
                                f"Wrote {index} cases in {self.current_time_rate(t0)}s\n",
                                flush=True,
                            )
                        wri.write(value)

        items = []
        return items

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
          writer: jsonlines.Writer = jsonlines.Writer(fp)
          with writer as wri:
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
      ids: list | None = None,
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
        async with aiohttp.ClientSession() as session:
            async for case in self._paginate_files_or_cases(
                ids=case_ids,
                endpt=endpoint,
                page_size=page_size,
                num_field_chunks=3,
                session=session
            ):

                end_cases.append(case)
                n += 1
                if n % page_size == 0:
                    print(
                        f"Wrote {n} cases in {self.current_time_rate(t0)}s\n", flush=True
                    )
        return end_cases

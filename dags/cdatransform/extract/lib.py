import asyncio
import sys
import time
import jsonlines
import gzip

import requests
from google.cloud import storage
import json
import aiohttp
from typing import Union


async def retry_get(session, endpoint, params, base_retry_interval=180.0):
    retry_interval = base_retry_interval
    await asyncio.sleep(1)
    async with session.get(
        url=endpoint, params=params, timeout=retry_interval
    ) as response:
        while True:
            try:
                result = response
                if result.ok:
                    return await result.json()
                else:
                    sys.stderr.write(str(result.content))
                    sys.stderr.write(
                        f"API call failed. Retrying in {retry_interval}s ...\n"
                    )
                    await asyncio.sleep(retry_interval)
                    retry_interval *= 2
            except:
                sys.stderr.write(
                    f"API call failed. Retrying in {retry_interval}s ...\n"
                )
                await asyncio.sleep(retry_interval)

                retry_interval *= 2


def send_json_to_storage(json_obj):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket("broad-cda-dev")

    blob = bucket.blob("extractions/gdc-extraction.json")
    blob.upload_from_string(data=json.dumps(json_obj), content_type="application/json")

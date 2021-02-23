import requests
import sys
import time


def retry_get(endpoint, params, base_retry_interval=180.0):
    retry_interval = base_retry_interval
    while True:
        result = requests.get(endpoint, params=params)
        if result.ok:
            return result
        else:
            sys.stderr.write(f"API call failed. Retrying in {retry_interval}s ...\n")
            time.sleep(retry_interval)
            retry_interval *= 2

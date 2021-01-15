import requests
import sys
import time


def get_case_ids(case_list_file):
    if case_list_file is None:
        return None
    else:
        with open(case_list_file, "r") as fp:
            return [l.strip() for l in fp.readlines()]


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

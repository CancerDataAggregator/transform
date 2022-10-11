import requests


def make_request(file_name):
    url = f"https://raw.githubusercontent.com/CancerDataAggregator/cda-data-model/main/src/schema/json/{file_name}.json"
    with requests.get(url=url) as response:
        return response.json()


def url_download(value_keys):
    for i in value_keys:
        yield (i, make_request(i))

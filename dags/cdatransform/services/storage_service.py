from io import TextIOWrapper
import os
from google.cloud import storage
from google.cloud.storage import Client
from smart_open import open
from typing import Any, Callable, Union

class StorageService:
  def __init__(self) -> None:
    self.service_account_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    self.gcp_client = Client.from_service_account_json(self.service_account_path)

  def get_session(self, gcp_buck_path: str, mode: str):
    return open(gcp_buck_path, mode, transport_params=dict(client=self.gcp_client))

  def compose_blobs(self, files: list, bucket_name: str, output_file: str):
    bucket_name_to_use = self.get_bucket_name(bucket_name)
    bucket = self.gcp_client.bucket(bucket_name_to_use)


    bucket.blob(self.get_file_path_in_bucket(output_file)).compose([
      bucket.get_blob(self.get_file_path_in_bucket(file))
      for file in files
    ])

  def get_bucket_name(self, name: str)-> str:
    no_gs = name.replace("gs://", "")
    path_split = no_gs.split("/")
    return path_split[0]

  def get_file_path_in_bucket(self, file_path: str) -> str:
    if "gs://" in file_path:
      no_gs = file_path.replace("gs://", "")
      path_split = no_gs.split("/")
      return "/".join([s for s in map(lambda i: path_split[i], range(1, len(path_split)))])
    return file_path

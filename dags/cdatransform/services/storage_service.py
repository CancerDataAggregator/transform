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

  def open_session(self,
                   callback: Callable[[Union[TextIOWrapper, Any]], Any],
                   gcp_buck_path: str,
                   mode: str = "r") -> Any:
    with open(
        gcp_buck_path, mode, transport_params=dict(client=self.gcp_client)
    ) as fp:
      return callback(fp)

  def open_session_with_reader(self,
                   callback: Callable[[Union[TextIOWrapper, Any], [TextIOWrapper, Any]], Any],
                   reader: Union[TextIOWrapper, Any],
                   gcp_buck_path: str,
                   mode: str = "r") -> Any:
    with open(
        gcp_buck_path, mode, transport_params=dict(client=self.gcp_client)
    ) as fp:
      return callback(fp, reader)

  def get_session(self, gcp_buck_path: str, mode: str):
    return open(gcp_buck_path, mode, transport_params=dict(client=self.gcp_client))

  def compose_blobs(self, files: list, bucket_name: str, output_file: str):
    bucket = self.gcp_client.bucket(bucket_name)
    bucket.blob(output_file).compose(files)

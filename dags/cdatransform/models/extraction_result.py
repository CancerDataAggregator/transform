class ExtractionResult:
  def __init__(self,
               file_name: str,
               bucket_name: str) -> None:
    self.file_name = file_name
    self.bucket_name = bucket_name

class ExtractionResult:
  def __init__(self,
               prefix: str,
               last_index: int,
               bucket_name: str) -> None:
    self.prefix = prefix
    self.last_index = last_index
    self.bucket_name = bucket_name

  def file_loop(self, file_ext: str = "jsonl.gz"):
    for i in range(0, self.last_index):
      yield f"{self.bucket_name}/{self.prefix}-index-{i}.{file_ext}"

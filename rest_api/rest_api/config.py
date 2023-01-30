import os
from pathlib import Path


PIPELINE_YAML_PATH = os.getenv(
    "PIPELINE_YAML_PATH", str((Path(__file__).parent / "pipeline" / "lukaz.haystack-pipeline.yml").absolute())
)
QUERY_PIPELINE_NAME = os.getenv("QUERY_PIPELINE_NAME", "query")
INDEXING_PIPELINE_NAME = os.getenv("INDEXING_PIPELINE_NAME", "indexing")
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "tekr.es.europe-west3.gcp.cloud.es.io")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD", "EaTmUIlTRQdRCnDuHmI1BNsl")

FILE_UPLOAD_PATH = os.getenv("FILE_UPLOAD_PATH", str((Path(__file__).parent / "file-upload").absolute()))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
ROOT_PATH = os.getenv("ROOT_PATH", "/")

CONCURRENT_REQUEST_PER_WORKER = int(os.getenv("CONCURRENT_REQUEST_PER_WORKER", "4"))

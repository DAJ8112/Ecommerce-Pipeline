"""Shared pipeline configuration loaded from environment variables."""

import os


def _require_env(name):
    """Return required environment variable or raise a descriptive error."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Expected environment variable '{name}' to be set.")
    return value


# GCP Settings
PROJECT_ID = _require_env("PIPELINE_PROJECT_ID")
REGION = os.getenv("PIPELINE_REGION", "us-central1")

# Cloud Storage
BUCKET_NAME = _require_env("PIPELINE_BUCKET_NAME")
RAW_LAYER = f"gs://{BUCKET_NAME}/raw"
CLEANED_LAYER = f"gs://{BUCKET_NAME}/cleaned"
CURATED_LAYER = f"gs://{BUCKET_NAME}/curated"
QUARANTINE_LAYER = f"gs://{BUCKET_NAME}/quarantine"
METADATA_LAYER = f"gs://{BUCKET_NAME}/metadata"

# BigQuery
SOURCE_DATASET = os.getenv(
    "PIPELINE_SOURCE_DATASET",
    "bigquery-public-data.thelook_ecommerce",
)
CURATED_DATASET = os.getenv(
    "PIPELINE_CURATED_DATASET",
    f"{PROJECT_ID}.ecommerce_curated",
)
METADATA_DATASET = os.getenv(
    "PIPELINE_METADATA_DATASET",
    f"{PROJECT_ID}.ecommerce_metadata",
)
TEMP_BUCKET = os.getenv("PIPELINE_TEMP_BUCKET", BUCKET_NAME)

# Tables to extract
TABLES_TO_EXTRACT = [
    "orders",
    "order_items",
    "users",
    "products",
    "distribution_centers",
]
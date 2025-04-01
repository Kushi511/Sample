import os
import sys
import logging
import pandas as pd
import pyarrow.parquet as pq
import base64
import json
import tempfile
from google.cloud import storage
from google.cloud import bigquery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('etl-loader')

# Get environment variables
SOURCE_TABLE = os.environ.get("SOURCE_TABLE")
DESTINATION_TABLE = os.environ.get("DESTINATION_TABLE")
GCS_BUCKET = os.environ.get("GCS_BUCKET", 'cf-it-di-us-west1-bkt-stripe-stage')
DATA_DIR = os.environ.get("DATA_DIR", SOURCE_TABLE)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "5000"))
BIGQUERY_PROJECT = os.environ.get("BIGQUERY_PROJECT")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
GCP_CREDENTIALS_BASE64 = os.environ.get("GCP_CREDENTIALS_BASE64")

def setup_gcp_credentials():
    logger.info("Setting up GCP credentials from base64-encoded string")
    if not GCP_CREDENTIALS_BASE64:
        logger.error("GCP_CREDENTIALS_BASE64 environment variable is required")
        return False
    try:
        credentials_json = base64.b64decode(GCP_CREDENTIALS_BASE64).decode('utf-8')
        temp = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        temp.write(credentials_json)
        temp.close()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp.name
        return True
    except Exception as e:
        logger.error(f"Error setting up GCP credentials: {e}")
        return False

def list_partition_dirs():
    logger.info(f"Listing partition directories in GCS bucket {GCS_BUCKET} for source table {SOURCE_TABLE}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=f"{DATA_DIR}/"))

    partition_dirs = set()
    for blob in blobs:
        if blob.name == f"{DATA_DIR}/":
            continue
        parts = blob.name.split('/')
        if len(parts) >= 2:
            partition_dir = f"{parts[0]}/{parts[1]}"
            partition_dirs.add(partition_dir)
    logger.info(f"Found {len(partition_dirs)} partition directories")
    return list(partition_dirs)

def check_partition_complete(partition_dir):
    logger.info(f"Checking if partition {partition_dir} has a _SUCCESS file")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    success_blob = bucket.blob(f"{partition_dir}/_SUCCESS")
    exists = success_blob.exists()
    logger.info(f"_SUCCESS file for partition {partition_dir} exists: {exists}")
    return exists

def list_parquet_files(partition_dir):
    logger.info(f"Listing Parquet files in partition directory {partition_dir}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = list(bucket.list_blobs(prefix=f"{partition_dir}/"))

    parquet_files = [
        f"gs://{GCS_BUCKET}/{blob.name}" 
        for blob in blobs 
        if blob.name.endswith('.parquet') and not blob.name.endswith('schema.parquet')
    ]

    schema_parquet_candidates = [
        f"gs://{GCS_BUCKET}/{blob.name}"
        for blob in blobs
        if blob.name.endswith("schema.parquet")
    ]

    if not schema_parquet_candidates:
        raise ValueError(f"No schema.parquet file found in {partition_dir}")

    schema_parquet_path = schema_parquet_candidates[0]
    logger.info(f"Schema parquet path: {schema_parquet_path}")
    logger.info(f"Found {len(parquet_files)} Parquet files in partition {partition_dir}")
    return parquet_files, schema_parquet_path

def load_to_bigquery(parquet_files, schema_parquet_path):
    logger.info(f"Loading {len(parquet_files)} Parquet files to BigQuery table {DESTINATION_TABLE}")
    client = bigquery.Client()
    schema = infer_bq_schema_from_parquet(schema_parquet_path)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        merge_fields=[field.name for field in schema],
        autodetect=False,
        schema=schema
    )
    table_ref = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{DESTINATION_TABLE}"
    logger.info(f"Starting BigQuery load job for table {table_ref}")
    load_job = client.load_table_from_uri(parquet_files, table_ref, job_config=job_config)
    load_job.result()
    table = client.get_table(table_ref)
    logger.info(f"Loaded {table.num_rows} rows to {table_ref}")
    return table.num_rows

def infer_bq_schema_from_parquet(parquet_path):
    table = pq.read_table(parquet_path)
    schema = []
    for field in table.schema:
        bq_type = map_pyarrow_to_bq_type(field)
        schema.append(bigquery.SchemaField(field.name, bq_type))
    return schema

def map_pyarrow_to_bq_type(field):
    pa_type = str(field.type)
    if pa_type.startswith("int") or pa_type == "uint64":
        return "STRING"
    elif pa_type.startswith("float") or "decimal" in pa_type:
        return "STRING"
    elif pa_type == "bool":
        return "STRING"
    elif "timestamp" in pa_type:
        return "STRING"
    elif "date" in pa_type:
        return "STRING"
    elif "binary" in pa_type:
        return "STRING"
    elif "string" in pa_type or "utf8" in pa_type:
        return "STRING"
    elif pa_type.startswith("list"):
        return "REPEATED"
    else:
        return "STRING"

def main():
    logger.info("Starting ETL loader script")
    for var in [SOURCE_TABLE, DESTINATION_TABLE, GCS_BUCKET, BIGQUERY_PROJECT, BIGQUERY_DATASET]:
        if not var:
            logger.error(f"Required environment variable is missing.")
            sys.exit(1)

    if not setup_gcp_credentials():
        sys.exit(1)

    try:
        partition_dirs = list_partition_dirs()
        if not partition_dirs:
            logger.error(f"No partition directories found for {SOURCE_TABLE}")
            sys.exit(1)

        incomplete_partitions = [p for p in partition_dirs if not check_partition_complete(p)]
        if incomplete_partitions:
            logger.error(f"Some partitions are incomplete: {incomplete_partitions}")
            sys.exit(1)

        if len(partition_dirs) != 1:
            logger.error("Expected exactly one partition directory for this load job")
            sys.exit(1)

        parquet_files, schema_parquet_path = list_parquet_files(partition_dirs[0])
        if not parquet_files:
            logger.error(f"No Parquet files found in partition {partition_dirs[0]}")
            sys.exit(1)

        total_rows = load_to_bigquery(parquet_files, schema_parquet_path)
        logger.info(f"Successfully loaded {total_rows} rows to BigQuery")

    except Exception as e:
        logger.error(f"Error during loading: {e}")
    finally:
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            try:
                os.unlink(cred_path)
                logger.info("Cleaned up temporary GCP credentials file")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary credentials file: {e}")

if __name__ == "__main__":
    main()

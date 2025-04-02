#!/usr/bin/env python3
import os
import time
import logging
import yaml
import psycopg2
from google.cloud import bigquery
import kubernetes.client
from kubernetes.client.rest import ApiException
from string import Template
from prometheus_client import start_http_server, Counter, Gauge
from google.cloud import storage
import base64
import json
import tempfile
import threading

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('etl-controller')
# Environment variables
NAMESPACE = os.environ.get('NAMESPACE', 'di-kubernets-staging')
logger.info(f"Using namespace: {NAMESPACE}")
MAX_CONCURRENT_EXTRACTIONS = int(os.environ.get('MAX_CONCURRENT_EXTRACTIONS', '10'))
logger.info(f"Maximum concurrent extractions: {MAX_CONCURRENT_EXTRACTIONS}")
MAX_CONCURRENT_LOADS = int(os.environ.get('MAX_CONCURRENT_LOADS', '5'))
logger.info(f"Maximum concurrent loads: {MAX_CONCURRENT_LOADS}")
DB_CONNECTION_STRING = os.environ.get('DB_CONNECTION_STRING')
DB_CONNECTION_STRING = "postgresql://diuser_prod:stripe-pipeline-secret@billingdb.postgres.ams.cfdata.org:5432/prod_entitlements"
logger.info("Database connection established")
BIGQUERY_PROJECT = os.environ.get('BIGQUERY_PROJECT', 'cloudflare-datainsights')
logger.info(f"BigQuery project: {BIGQUERY_PROJECT}")
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'prod_entitlements_stage')
logger.info(f"BigQuery dataset: {BIGQUERY_DATASET}")
GCS_BUCKET = os.environ.get('GCS_BUCKET', 'cf-it-di-us-west1-bkt-stripe-stage')
GCP_CREDENTIALS_BASE64 = "ewogICAgInR5cGUiOiAic2VydmljZV9hY2NvdW50IiwKICAgICJwcm9qZWN0X2lkIjogImNsb3VkZmxhcmUtZGF0YWluc2lnaHRzIiwKICAgICJwcml2YXRlX2tleV9pZCI6ICI2ZjUzMDZlYjFlOGVhNWYzMDMwMmY4NzVmNmUwYjMyMjg2N2JlZGI2IiwKICAgICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRFJ4bmpXaEdKdjlZSVZcbktOb2pxd292Um5LRVRDd3FWU3d1S3BXLzNiZDQ2aUpvNlVlY0NLSG1VUXNZb3o1bFNQQ0JGdmtkbGxSa2ZYcDlcbjFuTFNxbGRpTWFqbXdQdExJV0RLUVg2SWtRS0lhbU4zRHkzSkFTcE5LbmhzeUp5dlhGY05qZjdXK3RsTVZ2LzNcbnhNVHpkR0dNMEMvZmo3Z0JHeWtWdEpvWUFsUUFXTjdDeG5IcnBYZ2x5VHc4ZC9NWlB6cEpPQzBacjRsckh1cVhcbmxLOHo5SjVkYnhlNFVpV3ptdXlOd0NsL1J6NDE4cUx2M3Jzb0pkQzV2d3ZNaU9rZ0t4SkZ4Q2lxczJZaGp2K1FcbmxSSzdCN3lVWXlMeWQralV4YThoT0J4SlkzK0xwQzZUbVNjaERkQmpYM3BMMUNTZzBqRThGWGd3UmpxNForNlZcbmNMMjFoQnBaQWdNQkFBRUNnZ0VBQ1Nuc2ZUdE1wcjdQSHp0UFUyTDFTVDB6c0JBRnlrbWdBOENzU0pzWC96YnFcblc3SkpIanp3VE5GNkd4MVNlU2szYjVkbW54djFXNTdZQitkd0JITkw2VlRwYkhKWDUrd3hRRlNkNTd1TGl1RnJcbnJ4NFVoOWcrZklWNXU5eHFiSi9ldTZrbXpheGNPRlJuYUZZanJNMjZOWmc3ellzSGw5UDZBMklHQm5CRi8xU2JcbkRvZTZOalN5NXp2RGZmR3NHMEwwVXF6MEljRVQwb2t2akdLNVdtZUJucHFvbUVsUStqa3JTM2ZkN1BNVzExdlpcbmE4R05ZWnA0OXV5RExQTG9uL3hvN29JSUZvRUlVNFF4VjdKZkxMRHBXZEdFNVB0azB1S2VSOUtHekNHb0hWR1FcbkRkTm40QzN3aUg5d2paaStFanUyNm1rY0cwMUpreXdhK2NuWW0wSklhUUtCZ1FEeUNPNi9ud2k3bkl0T1dqSmhcbmpmc1cwQng0NDcrWGZqZWg5SW9aSE1MRkZzNVFkTWl3QkFQN3VLaS9FLy90UmI2RGlESU9SNUUzak1KZmQ3VmpcbjZFL09uRHhicDlSM0NXZXA2am9LdXdJVHd4NjFkRW9iRUpGblNmSTB4bC9lR0xWVEhFWWdQV2FJdVdyeWJFc3Vcbi80bzEyQVB5eWVrdkxWbmRXMWdCaFM1R25RS0JnUURkNFFsa2E2cXorY25sVTJIUHpkTW9jM29UczQvMndMei9cbm5vVG92U0o3K1d6SDVRbCtocVlvaStucEZ2YWJUZkY0a0tQZmdIVlNlTWdEamc1c2JuWktOTWMrbTJJcDdaKzNcbkYyOUN0U0FVU2RmNmF4MmIvUC83WnJYMFhYclhUd2h6b2dJTVZoVW1JTXZyc0hTektGQzVEcGxFalRMeXhNQVpcbkRUM2ZJalUzN1FLQmdRQzZ6UVFmWnNuaHFzZWxtRjJzQjEzVVZKaTFBT245TFZBWXNsam5XdGhFTHY3YS85ZG9cbmFpRDg1Wjl2b2lyellOSFNTSXFCbE9EU1k5UWN4Sko5NG0yK0E3MWQ3Q0ZDSWtNYzFBY3FBdjF1YlRqRlNWUnNcbm9SUG9DUjFqZC92RHVZUXcrZkJ4cjVIMFVrN2xmWWxsWTVxelJkNStRekd5MUtpMy9Ham0rM2drelFLQmdHZ0lcbkEyZGFOQWQrcnZNZlRWVXBwRC9ySk9ubjN6QkszbExiK0dWSGlNdW4veUVhZW9FZ2tQZWg0bUt2cWFEWHdzaE1cbnlRa2FLZWdwU1Y2aFZKSHltUFBpR3lsM29XY1hTek9GZ1YwOW4zVGp5cWhYWU9LcWxUdEN4VDEyTndxTXUrRzdcbnJFb3NjbTRXSW9hM2pZQVMvd1pGbWdkU1RQa1cxeHJuNG9La2YxOWhBb0dCQU4rRjRwZFRlbW1UT1FKNm82d1Jcbi85VUE3c3drTDBjMG1oeEZteGg0UklmZUJzRHZvcHBmUDN6K2IzZEdwRUwzU0ZCcUVLb0lwOS8wVmZrSlFYVVFcbi9KakRtQU5KQVE1c0Q4R204cXBBb1llUUtYVDFPRUJQa2hMdEFjS3YxYjRRUVprNXJneGx0WDBIVXZQNWY3YWxcbjE3V1UzODBpeFdFZTYrSFpaUzdScXZibFxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAgICJjbGllbnRfZW1haWwiOiAiaXQtZGF0YWluc2lnaHRzLWFkbWluLWNsb3VkZmxhQGNsb3VkZmxhcmUtZGF0YWluc2lnaHRzLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAgICJjbGllbnRfaWQiOiAiMTA0NDU0MDU1MTk1MDM1Mjg5NTgyIiwKICAgICJhdXRoX3VyaSI6ICJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsCiAgICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAgICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAgICJjbGllbnRfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9yb2JvdC92MS9tZXRhZGF0YS94NTA5L2l0LWRhdGFpbnNpZ2h0cy1hZG1pbi1jbG91ZGZsYSU0MGNsb3VkZmxhcmUtZGF0YWluc2lnaHRzLmlhbS5nc2VydmljZWFjY291bnQuY29tIgogICAgfQ=="
logger.info(f"GCS bucket: {GCS_BUCKET}")
TEST_MODE = os.environ.get('TEST_MODE', 'false').lower() == 'true'
logger.info(f"Test mode enabled: {TEST_MODE}")
TEST_TABLES = os.environ.get('TEST_TABLES', 'feature').split(',')
logger.info(f"Test tables: {TEST_TABLES}")
# Prometheus metrics
JOBS_CREATED = Counter('etl_jobs_created', 'Number of ETL jobs created', ['job_type', 'table'])
JOBS_COMPLETED = Counter('etl_jobs_completed', 'Number of ETL jobs completed', ['job_type', 'table'])
JOBS_FAILED = Counter('etl_jobs_failed', 'Number of ETL jobs failed', ['job_type', 'table'])
ACTIVE_JOBS = Gauge('etl_active_jobs', 'Number of active ETL jobs', ['job_type'])
logger.info("Prometheus metrics initialized")
# Initialize Kubernetes client
kubernetes.config.load_incluster_config() #uncomment for prod
logger.info("Kubernetes client initialized")
logging.basicConfig(level=logging.DEBUG)
batch_v1 = kubernetes.client.BatchV1Api()
core_v1 = kubernetes.client.CoreV1Api()


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

def get_config_from_bq():
    """Get config from bq  cloudflare-datainsights.prod_entitlements_stage.data_pipeline_registry """
    sql="""
        WITH A1 AS (
        SELECT 
            *,
            ROW_NUMBER() OVER(PARTITION BY pipeline_id ORDER BY date_of_refresh) AS RNK 
        FROM cloudflare-datainsights.prod_entitlements_stage.data_pipeline_registry
        WHERE skip_table = 'N'
        ),
        MAX_ID_TB AS (
        SELECT 
            pipeline_id,
            MAX(max_id) AS max_id 
        FROM A1 
        WHERE 
            RNK < 7 
        GROUP BY 
            pipeline_id
        ),
        A2 AS (
        SELECT  
            * EXCEPT(max_id) 
        FROM A1 
        WHERE 
            RNK = 1
        )
        SELECT 
        MAX_ID_TB.*,
        A2.*
        FROM MAX_ID_TB
        LEFT JOIN A2
        ON MAX_ID_TB.pipeline_id = A2.pipeline_id
        """
    client = bigquery.Client()
    query_job = client.query(sql)
    results = []
    for row in query_job:
        results.append(dict(row))
    logger.info(f"Retrieved {results} rows from BigQuery for pipeline configuration")   
    return results

def update_pipeline_status(pipeline_id, status):
    """Update the status of a pipeline in the BigQuery table."""
    logger.info(f"Updating status for pipeline {pipeline_id} to {status}")
    sql = f"""
        UPDATE cloudflare-datainsights.prod_entitlements_stage.data_pipeline_registry
        SET status = '{status}'
        WHERE pipeline_id = '{pipeline_id}'
    """
    client = bigquery.Client()
    query_job = client.query(sql)
    query_job.result()  # Wait for the query to complete
    logger.info(f"Status updated for pipeline {pipeline_id} to {status}")

def insert_new_pipeline_record(pipeline_id, new_values):
    """Insert a new record for the pipeline with updated values."""
    logger.info(f"Inserting new record for pipeline {pipeline_id}")
    sql = f"""
        INSERT INTO cloudflare-datainsights.prod_entitlements_stage.data_pipeline_registry
        (pipeline_id, run_date, next_refresh_date, refresh_timestamp, min_id, max_id)
        VALUES (
            '{pipeline_id}',
            '{new_values['run_date']}',
            '{new_values['next_refresh_date']}',
            '{new_values['refresh_timestamp']}',
            {new_values['min_id']},
            {new_values['max_id']}
        )
    """
    client = bigquery.Client()
    query_job = client.query(sql)
    query_job.result()  # Wait for the query to complete
    logger.info(f"New record inserted for pipeline {pipeline_id}")

def delete_old_pipeline_records(pipeline_id):
    """Delete records older than 8 runs for the given pipeline ID."""
    logger.info(f"Deleting old records for pipeline {pipeline_id}")
    sql = f"""
        DELETE FROM cloudflare-datainsights.prod_entitlements_stage.data_pipeline_registry
        WHERE pipeline_id = '{pipeline_id}'
        AND ROW_NUMBER() OVER(PARTITION BY pipeline_id ORDER BY date_of_refresh DESC) > 8
    """
    client = bigquery.Client()
    query_job = client.query(sql)
    query_job.result()  # Wait for the query to complete
    logger.info(f"Old records deleted for pipeline {pipeline_id}")

def get_db_connection():
    """Create a connection to the PostgreSQL database"""
    try:
        logger.info("Attempting to establish database connection")
        conn = psycopg2.connect(DB_CONNECTION_STRING)
        conn.set_client_encoding('UTF8')
        logger.info("Database connection successful")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.info(f"Connection string used: {DB_CONNECTION_STRING}")
        return None

def get_tables_to_process(table):
    """Get list of tables to extract from PostgreSQL with their sizes"""
    logger.info("Retrieving tables to process from database")
    conn = get_db_connection()
    if not conn:
        logger.error("Could not establish database connection for table retrieval")
        return []
    
    try:
        with conn.cursor() as cur:
            if table['refresh_type'] == 'INCREMENTAL':
                query = f"""
                    WITH table_size AS (
                        SELECT 
                            relname AS table_name,
                            pg_total_relation_size(relid) AS total_size_bytes
                        FROM 
                            pg_stat_user_tables
                        WHERE 
                            schemaname = 'public' AND 
                            relname = '{table['source_table']}'
                    ),
                    total_rows AS (
                        SELECT 
                            c.relname AS table_name,
                            c.reltuples AS estimated_row_count
                        FROM 
                            pg_class c
                        JOIN 
                            pg_namespace n ON n.oid = c.relnamespace
                        WHERE 
                            n.nspname = 'public' AND 
                            c.relname = '{table['source_table']}'
                    ),
                    chunk_rows AS (
                        SELECT 
                            COUNT(*)::float AS chunk_row_count
                        FROM {table['source_table']}
                        WHERE {table['primary_key_column']} > {table['max_id']}
                    )
                    SELECT table_name, estimated_chunk_size_gb AS size_gb FROM (
                        SELECT 
                            ts.table_name,
                            ts.total_size_bytes / (1024 * 1024 * 1024.0) AS total_size_gb,
                            tr.estimated_row_count,
                            cr.chunk_row_count,
                            (cr.chunk_row_count / NULLIF(tr.estimated_row_count, 0)) * 
                                (ts.total_size_bytes / (1024 * 1024 * 1024.0)) AS estimated_chunk_size_gb
                        FROM table_size ts
                        JOIN total_rows tr ON ts.table_name = tr.table_name,
                            chunk_rows cr
                    ) AS subq 
                """
            else:
                query = f"""
                    SELECT 
                        table_name,
                        pg_total_relation_size(quote_ident(table_name)) / (1024 * 1024 * 1024.0) as size_gb
                    FROM 
                        information_schema.tables
                    WHERE 
                        table_schema = 'public' AND
                        table_type = 'BASE TABLE'
                        AND table_name = '{table_name}'
                    ORDER BY 
                        size_gb DESC
                """
            
            logger.info("Executing query to get table sizes")
            cur.execute(query)
            results = cur.fetchall()
            tables = [{"name": row[0], "estimated_size_gb": row[1]} for row in results]
            logger.info(f"Found {len(tables)} tables to process")
            for table_info in tables:
                logger.info(f"Table {table_info['name']}: {table_info['estimated_size_gb']:.2f} GB")
            
            return tables[0]['estimated_size_gb'] if tables else 0
            
    except Exception as e:
        logger.error(f"Failed to get tables: {e}")
        logger.info("Exception details:", exc_info=True)
        return 0
    finally:
        logger.info("Closing database connection")
        conn.close()

def get_primary_key(table_name):
    """Get primary key column for a table"""
    logger.info(f"Getting primary key for table: {table_name}")
    conn = get_db_connection()
    if not conn:
        logger.warning(f"Using default primary key 'id' for table {table_name} due to connection failure")
        return "id"
    
    try:
        with conn.cursor() as cur:
            logger.info(f"Executing query to get primary key for table {table_name}")
            cur.execute("""
                    SELECT a.attname
                    FROM   pg_index i
                    JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                         AND a.attnum = ANY(i.indkey)
                    WHERE  i.indrelid = %s::regclass
                    AND    i.indisprimary
            """, (table_name,))
            result = cur.fetchone()
            primary_key = result[0] if result else "id"
            logger.info(f"Primary key for table {table_name}: {primary_key}")
            return primary_key
    except Exception as e:
        logger.error(f"Error getting primary key for {table_name}: {e}")
        logger.info("Exception details:", exc_info=True)
        return "id"
    finally:
        conn.close()

def get_destination_table_name(source_table):
    """Convert source table name to destination table name format"""
    dest_table = f"{source_table}_processed"
    logger.info(f"Destination table name for {source_table}: {dest_table}")
    return dest_table

def load_job_template(template_name):
    """Load job template from ConfigMap mounted volume"""
    template_path = f"/app/templates/{template_name}.yaml"
    logger.info(f"Loading job template: {template_name}")
    try:
        with open(template_path, 'r') as f:
            template_content = f.read()
            logger.info(f"Successfully loaded template {template_name}")
            return template_content
    except Exception as e:
        logger.info(f"Failed to load template {template_name}: {e}")
        logger.info(f"Template path attempted: {template_path}")
        logger.info("Exception details:", exc_info=True)
        return None

def count_active_jobs(label_selector):
    """Count number of active jobs matching the label selector"""
    logger.info(f"Counting active jobs with selector: {label_selector}")
    try:
        jobs = batch_v1.list_namespaced_job(
            namespace=NAMESPACE,
            label_selector=label_selector
        )
        active_count = 0
        for job in jobs.items:
            if job.status.active is not None and job.status.active > 0:
                active_count += 1
        logger.info(f"Found {active_count} active jobs for selector {label_selector}")
        return active_count
    except ApiException as e:
        logger.error(f"Error counting active jobs: {e}")
        logger.info("Exception details:", exc_info=True)
        return 0

def create_extraction_job(table_name, partition_id, total_partitions, primary_key, primary_key_val):
    """Create a Kubernetes Job for data extraction"""
    job_name = f"extract-{table_name}-{partition_id}-{int(time.time())}"
    job_name = job_name.replace('_','-')
    logger.info(f"Creating extraction job: {job_name}")
    
    template_str = load_job_template("extraction-job")
    if not template_str:
        logger.error(f"Failed to create job {job_name}: template loading failed")
        return None
    
    template = Template(template_str)
    logger.info(f"Substituting variables for job {job_name}")
    job_manifest_str = template.substitute(
        TABLE_NAME=table_name,
        PARTITION_ID=str(partition_id),
        TOTAL_PARTITIONS=str(total_partitions),
        PRIMARY_KEY=primary_key,
        GCS_BUCKET=GCS_BUCKET,
        PRIMARY_KEY_VAL=primary_key_val
    )
    
    job_manifest = yaml.safe_load(job_manifest_str)
    logger.info(f"Job manifest created for {job_name}")
    logger.debug(f"Initial job manifest: {job_manifest}")
    
    job_manifest["metadata"]["name"] = job_name
    logger.debug(f"Job manifest after name update: {job_manifest}")
    
    if "labels" not in job_manifest["metadata"]:
        job_manifest["metadata"]["labels"] = {}
    logger.debug(f"Job manifest after labels check: {job_manifest}")
    if "primary_key_val" not in job_manifest["metadata"]:
        job_manifest["metadata"]["primary_key_val"] = primary_key_val
    
    job_manifest["metadata"]["labels"].update({
        "app": "etl-extractor",
        "table": table_name,
        "partition": str(partition_id),
        "created-by": "etl-controller",
        "primary-key-val": str(primary_key_val)
    })
    logger.debug(f"Final job manifest after labels update: {job_manifest}")
    
    try:
        batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
        logger.info(f"Successfully created extraction job {job_name} for table {table_name}, partition {partition_id}")
        JOBS_CREATED.labels(job_type="extraction", table=table_name).inc()
        ACTIVE_JOBS.labels(job_type="extraction").inc()
        return job_name
    except Exception as e:
        logger.error(f"Failed to create extraction job {job_name}: {e}")
        logger.info("Exception details:", exc_info=True)
        return None

def create_loading_job(table_name):
    """Create a Kubernetes Job for data loading to BigQuery"""
    job_name = f"load-{table_name}-{int(time.time())}"
    job_name = job_name.replace('_','-')
    logger.info(f"Creating loading job: {job_name}")
    
    destination_table = get_destination_table_name(table_name)
    logger.info(f"Destination table for {job_name}: {destination_table}")
    
    template_str = load_job_template("loading-job")
    if not template_str:
        logger.error(f"Failed to create job {job_name}: template loading failed")
        return None
    
    template = Template(template_str)
    logger.info(f"Substituting variables for job {job_name}")
    job_manifest_str = template.substitute(
        TABLE_NAME=table_name,
        DESTINATION_TABLE=destination_table,
        BIGQUERY_PROJECT=BIGQUERY_PROJECT,
        BIGQUERY_DATASET=BIGQUERY_DATASET,
        GCS_BUCKET=GCS_BUCKET
    )
    
    job_manifest = yaml.safe_load(job_manifest_str)
    logger.info(f"Job manifest created for {job_name}")
    
    job_manifest["metadata"]["name"] = job_name
    
    if "labels" not in job_manifest["metadata"]:
        job_manifest["metadata"]["labels"] = {}
    
    job_manifest["metadata"]["labels"].update({
        "app": "etl-loader",
        "table": table_name,
        "created-by": "etl-controller"
    })
    
    try:
        batch_v1.create_namespaced_job(namespace=NAMESPACE, body=job_manifest)
        logger.info(f"Successfully created loading job {job_name} for table {table_name}")
        JOBS_CREATED.labels(job_type="loading", table=table_name).inc()
        ACTIVE_JOBS.labels(job_type="loading").inc()
        return job_name
    except Exception as e:
        logger.error(f"Failed to create loading job {job_name}: {e}")
        logger.info("Exception details:", exc_info=True)
        return None

def check_extraction_complete(table_name):
    """Check if all extraction jobs for a table are complete using GCS"""
    logger.info(f"Checking extraction completion for table: {table_name}")
    cfg = kubernetes.client.Configuration.get_default_copy()
    logger.info(cfg.host)
    try:
        jobs = batch_v1.list_namespaced_job(
            namespace=NAMESPACE,
            label_selector=f"app=etl-extractor,table={table_name}"
        )
        logger.info(f"Found {len(jobs.items)} extraction jobs for table {table_name}")
        
        for job in jobs.items:
            if job.status.active is not None and job.status.active > 0:
                logger.info(f"Job {job.metadata.name} is still active")
                return False
            
            if job.status.failed is not None and job.status.failed > 0:
                if job.spec.backoff_limit is not None and job.status.failed > job.spec.backoff_limit:
                    logger.error(f"Extraction job {job.metadata.name} failed after {job.status.failed} attempts")
                    JOBS_FAILED.labels(job_type="extraction", table=table_name).inc()
                    return False
        
        if not jobs.items:
            logger.info(f"No extraction jobs found for table {table_name}")
            return False
        
        logger.info("Getting GCP credentials")
        credentials_json = get_gcp_credentials()
        if not credentials_json:
            logger.error("Failed to get GCP credentials")
            return False
        
        with tempfile.NamedTemporaryFile(mode='w+', delete=True) as temp:
            temp.write(credentials_json)
            temp.flush()
            logger.info("Temporary credentials file created")
            
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp.name
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(GCS_BUCKET)
            logger.info(f"Connected to GCS bucket: {GCS_BUCKET}")
            
            partitions = set()
            for job in jobs.items:
                partition = job.metadata.labels.get("partition")
                if partition:
                    partitions.add(partition)
            
            for partition in partitions:
                success_blob = bucket.blob(f"{table_name}/{partition}/_SUCCESS")
                if not success_blob.exists():
                    logger.warning(f"Success file not found for {table_name} partition {partition}")
                    return False
        
        logger.info(f"All extraction jobs for {table_name} completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error checking extraction completion: {e}")
        return False

def calculate_partitions(table_size_gb):
    """Calculate optimal number of partitions based on table size"""
    return max(1, min(50, int(table_size_gb / 5) + 1))

def process_table(table):
    """Process a single table (extraction and loading)."""
    table_name = table['source_table']
    pipeline_id = table['pipeline_id']
    table_size = get_tables_to_process(table)
    refresh_type = table['refresh_type']
    primary_key = table['primary_key_column']
    primary_key_val = table['max_id'] if refresh_type == 'INCREMENTAL' else 0

    update_pipeline_status(pipeline_id, "In Progress")

    if check_extraction_complete(table_name):
        update_pipeline_status(pipeline_id, "Loader In Progress")
        loading_jobs = batch_v1.list_namespaced_job(
            namespace=NAMESPACE,
            label_selector=f"app=etl-loader,table={table_name}"
        )
        if not loading_jobs.items:
            active_loads = count_active_jobs(f"app=etl-loader,table={table_name}")
            if active_loads < MAX_CONCURRENT_LOADS:
                create_loading_job(table_name)
            else:
                logger.info(f"Too many active loading jobs, will try {table_name} later")
        return
    
    extraction_jobs = batch_v1.list_namespaced_job(
        namespace=NAMESPACE,
        label_selector=f"app=etl-extractor,table={table_name}"
    )
    
    if not extraction_jobs.items:
        update_pipeline_status(pipeline_id, "Extract In Progress")
        active_extractions = count_active_jobs(f"app=etl-extractor,table={table_name}")
        if active_extractions < MAX_CONCURRENT_EXTRACTIONS:
            total_partitions = calculate_partitions(table_size)
            for partition_id in range(total_partitions):
                logger.info(f"Creating extraction job for partition {partition_id}")    
                create_extraction_job(table_name, partition_id, total_partitions, primary_key, primary_key_val)
                time.sleep(30)
        else:
            logger.info(f"Too many active extraction jobs, will try {table_name} later")

def cleanup_completed_jobs():
    """Delete completed jobs that are older than 24 hours"""
    try:
        jobs = batch_v1.list_namespaced_job(namespace=NAMESPACE)
        current_time = time.time()
        
        for job in jobs.items:
            if job.status.active is not None and job.status.active > 0:
                continue
                
            if job.status.completion_time is not None:
                completion_time = job.status.completion_time.timestamp()
                if (current_time - completion_time) > 5:
                    try:
                        batch_v1.delete_namespaced_job(
                            name=job.metadata.name,
                            namespace=NAMESPACE,
                            body=kubernetes.client.V1DeleteOptions(
                                propagation_policy="Background"
                            )
                        )
                        logger.info(f"Deleted completed job {job.metadata.name}")
                    except ApiException as e:
                        logger.error(f"Error deleting job {job.metadata.name}: {e}")
    except ApiException as e:
        logger.error(f"Error listing jobs for cleanup: {e}")

def update_metrics_for_table(table_name):
    """Update Prometheus metrics and check job status for a specific table"""
    try:
        extraction_jobs = batch_v1.list_namespaced_job(
            namespace=NAMESPACE,
            label_selector=f"app=etl-extractor,table={table_name}"
        )
        
        active_extractions = 0
        for job in extraction_jobs.items:
            if job.status.active is not None and job.status.active > 0:
                active_extractions += 1
            elif job.status.succeeded is not None and job.status.succeeded > 0:
                table = job.metadata.labels.get("table", "unknown")
                JOBS_COMPLETED.labels(job_type="extraction", table=table).inc()
                batch_v1.delete_namespaced_job(
                    name=job.metadata.name,
                    namespace=NAMESPACE,
                    body=kubernetes.client.V1DeleteOptions(propagation_policy="Background")
                )
                logger.info(f"Deleted completed extraction job {job.metadata.name}")
        
        ACTIVE_JOBS.labels(job_type="extraction").set(active_extractions)
        
        loading_jobs = batch_v1.list_namespaced_job(
            namespace=NAMESPACE,
            label_selector=f"app=etl-loader,table={table_name}"
        )
        
        active_loads = 0
        for job in loading_jobs.items:
            if job.status.active is not None and job.status.active > 0:
                active_loads += 1
            elif job.status.succeeded is not None and job.status.succeeded > 0:
                table = job.metadata.labels.get("table", "unknown")
                JOBS_COMPLETED.labels(job_type="loading", table=table).inc()
                batch_v1.delete_namespaced_job(
                    name=job.metadata.name,
                    namespace=NAMESPACE,
                    body=kubernetes.client.V1DeleteOptions(propagation_policy="Background")
                )
                logger.info(f"Deleted completed loading job {job.metadata.name}")
        
        ACTIVE_JOBS.labels(job_type="loading").set(active_loads)
        
        return active_extractions == 0 and active_loads == 0
    except ApiException as e:
        logger.error(f"Error updating metrics for table {table_name}: {e}")
        return False

def get_gcp_credentials():
    """Get GCP credentials from Kubernetes secret"""
    try:
        credentials_json = base64.b64decode(GCP_CREDENTIALS_BASE64).decode('utf-8')
        return credentials_json
    except Exception as e:
        logger.error(f"Error getting GCP credentials: {e}")
        return None

def run_table(table):
    """Run the ETL pipeline for a single table."""
    pipeline_id = table['pipeline_id']
    logger.info(f"Running ETL pipeline for table {pipeline_id}")
    while True:
        try:
            logger.info(f"Running ETL pipeline for table {pipeline_id}")
            process_table(table)
            cleanup_completed_jobs()
            if update_metrics_for_table(table['source_table']):
                new_values = {
                    "run_date": "CURRENT_DATE",
                    "next_refresh_date": "DATE_ADD(CURRENT_DATE, INTERVAL 1 DAY)",
                    "refresh_timestamp": "CURRENT_TIMESTAMP",
                    "min_id": 0,
                    "max_id": 1000,
                }
                insert_new_pipeline_record(pipeline_id, new_values)
                delete_old_pipeline_records(pipeline_id)
                logger.info(f"All jobs completed for table {pipeline_id}, exiting loop")
                return
            logger.info("Sleeping for 1 min before next check")
            time.sleep(60)
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(60)

def main():
    """Main controller loop"""
    start_http_server(8000)
    logger.info("ETL controller started")
    if not setup_gcp_credentials():
        logger.info("Failed to setup GCP credentials")
    table_config = get_config_from_bq()
    threads = []
    for table in table_config:
        logger.info(f"Pipeline configuration for table {table['pipeline_id']}: {table}")
        thread = threading.Thread(target=run_table, args=(table,))
        threads.append(thread)
        thread.start()
        logger.info(f"Started thread for table {table['pipeline_id']}")
    
    for thread in threads:
        logger.info(f"Waiting for thread {thread.name} to complete...")
        thread.join()
        logger.info(f"Thread {thread.name} completed")
    logger.info("All table processing threads completed")

if __name__ == "__main__":
    main()
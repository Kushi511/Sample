#!/usr/bin/env python3
import os
import sys
import logging
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import io
import base64
import tempfile
from google.cloud import storage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('etl-extractor')

# Get environment variables
TABLE_NAME = os.environ.get("TABLE_NAME")
PARTITION_ID = int(os.environ.get("PARTITION_ID", "0"))
TOTAL_PARTITIONS = int(os.environ.get("TOTAL_PARTITIONS", "1"))
PRIMARY_KEY = os.environ.get("PRIMARY_KEY", "id")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "250000"))
COMPLEX_CHUNK_SIZE = int(os.environ.get("COMPLEX_CHUNK_SIZE", "10000"))
GCS_BUCKET = os.environ.get("GCS_BUCKET", 'cf-it-di-us-west1-bkt-stripe-stage')
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", f"{TABLE_NAME}/{PARTITION_ID}")
COMPRESSION_LEVEL = int(os.environ.get("COMPRESSION_LEVEL", "9"))
POSTGRES_CONNECTION = "postgresql://diuser_prod:stripe-pipeline-secret@billingdb.postgres.ams.cfdata.org:5432/prod_entitlements"
GCP_CREDENTIALS_BASE64 = "ewogICAgInR5cGUiOiAic2VydmljZV9hY2NvdW50IiwKICAgICJwcm9qZWN0X2lkIjogImNsb3VkZmxhcmUtZGF0YWluc2lnaHRzIiwKICAgICJwcml2YXRlX2tleV9pZCI6ICI2ZjUzMDZlYjFlOGVhNWYzMDMwMmY4NzVmNmUwYjMyMjg2N2JlZGI2IiwKICAgICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRRFJ4bmpXaEdKdjlZSVZcbktOb2pxd292Um5LRVRDd3FWU3d1S3BXLzNiZDQ2aUpvNlVlY0NLSG1VUXNZb3o1bFNQQ0JGdmtkbGxSa2ZYcDlcbjFuTFNxbGRpTWFqbXdQdExJV0RLUVg2SWtRS0lhbU4zRHkzSkFTcE5LbmhzeUp5dlhGY05qZjdXK3RsTVZ2LzNcbnhNVHpkR0dNMEMvZmo3Z0JHeWtWdEpvWUFsUUFXTjdDeG5IcnBYZ2x5VHc4ZC9NWlB6cEpPQzBacjRsckh1cVhcbmxLOHo5SjVkYnhlNFVpV3ptdXlOd0NsL1J6NDE4cUx2M3Jzb0pkQzV2d3ZNaU9rZ0t4SkZ4Q2lxczJZaGp2K1FcbmxSSzdCN3lVWXlMeWQralV4YThoT0J4SlkzK0xwQzZUbVNjaERkQmpYM3BMMUNTZzBqRThGWGd3UmpxNForNlZcbmNMMjFoQnBaQWdNQkFBRUNnZ0VBQ1Nuc2ZUdE1wcjdQSHp0UFUyTDFTVDB6c0JBRnlrbWdBOENzU0pzWC96YnFcblc3SkpIanp3VE5GNkd4MVNlU2szYjVkbW54djFXNTdZQitkd0JITkw2VlRwYkhKWDUrd3hRRlNkNTd1TGl1RnJcbnJ4NFVoOWcrZklWNXU5eHFiSi9ldTZrbXpheGNPRlJuYUZZanJNMjZOWmc3ellzSGw5UDZBMklHQm5CRi8xU2JcbkRvZTZOalN5NXp2RGZmR3NHMEwwVXF6MEljRVQwb2t2akdLNVdtZUJucHFvbUVsUStqa3JTM2ZkN1BNVzExdlpcbmE4R05ZWnA0OXV5RExQTG9uL3hvN29JSUZvRUlVNFF4VjdKZkxMRHBXZEdFNVB0azB1S2VSOUtHekNHb0hWR1FcbkRkTm40QzN3aUg5d2paaStFanUyNm1rY0cwMUpreXdhK2NuWW0wSklhUUtCZ1FEeUNPNi9ud2k3bkl0T1dqSmhcbmpmc1cwQng0NDcrWGZqZWg5SW9aSE1MRkZzNVFkTWl3QkFQN3VLaS9FLy90UmI2RGlESU9SNUUzak1KZmQ3VmpcbjZFL09uRHhicDlSM0NXZXA2am9LdXdJVHd4NjFkRW9iRUpGblNmSTB4bC9lR0xWVEhFWWdQV2FJdVdyeWJFc3Vcbi80bzEyQVB5eWVrdkxWbmRXMWdCaFM1R25RS0JnUURkNFFsa2E2cXorY25sVTJIUHpkTW9jM29UczQvMndMei9cbm5vVG92U0o3K1d6SDVRbCtocVlvaStucEZ2YWJUZkY0a0tQZmdIVlNlTWdEamc1c2JuWktOTWMrbTJJcDdaKzNcbkYyOUN0U0FVU2RmNmF4MmIvUC83WnJYMFhYclhUd2h6b2dJTVZoVW1JTXZyc0hTektGQzVEcGxFalRMeXhNQVpcbkRUM2ZJalUzN1FLQmdRQzZ6UVFmWnNuaHFzZWxtRjJzQjEzVVZKaTFBT245TFZBWXNsam5XdGhFTHY3YS85ZG9cbmFpRDg1Wjl2b2lyellOSFNTSXFCbE9EU1k5UWN4Sko5NG0yK0E3MWQ3Q0ZDSWtNYzFBY3FBdjF1YlRqRlNWUnNcbm9SUG9DUjFqZC92RHVZUXcrZkJ4cjVIMFVrN2xmWWxsWTVxelJkNStRekd5MUtpMy9Ham0rM2drelFLQmdHZ0lcbkEyZGFOQWQrcnZNZlRWVXBwRC9ySk9ubjN6QkszbExiK0dWSGlNdW4veUVhZW9FZ2tQZWg0bUt2cWFEWHdzaE1cbnlRa2FLZWdwU1Y2aFZKSHltUFBpR3lsM29XY1hTek9GZ1YwOW4zVGp5cWhYWU9LcWxUdEN4VDEyTndxTXUrRzdcbnJFb3NjbTRXSW9hM2pZQVMvd1pGbWdkU1RQa1cxeHJuNG9La2YxOWhBb0dCQU4rRjRwZFRlbW1UT1FKNm82d1Jcbi85VUE3c3drTDBjMG1oeEZteGg0UklmZUJzRHZvcHBmUDN6K2IzZEdwRUwzU0ZCcUVLb0lwOS8wVmZrSlFYVVFcbi9KakRtQU5KQVE1c0Q4R204cXBBb1llUUtYVDFPRUJQa2hMdEFjS3YxYjRRUVprNXJneGx0WDBIVXZQNWY3YWxcbjE3V1UzODBpeFdFZTYrSFpaUzdScXZibFxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAgICJjbGllbnRfZW1haWwiOiAiaXQtZGF0YWluc2lnaHRzLWFkbWluLWNsb3VkZmxhQGNsb3VkZmxhcmUtZGF0YWluc2lnaHRzLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAgICJjbGllbnRfaWQiOiAiMTA0NDU0MDU1MTk1MDM1Mjg5NTgyIiwKICAgICJhdXRoX3VyaSI6ICJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20vby9vYXV0aDIvYXV0aCIsCiAgICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAgICJhdXRoX3Byb3ZpZGVyX3g1MDlfY2VydF91cmwiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vb2F1dGgyL3YxL2NlcnRzIiwKICAgICJjbGllbnRfeDUwOV9jZXJ0X3VybCI6ICJodHRwczovL3d3dy5nb29nbGVhcGlzLmNvbS9yb2JvdC92MS9tZXRhZGF0YS94NTA5L2l0LWRhdGFpbnNpZ2h0cy1hZG1pbi1jbG91ZGZsYSU0MGNsb3VkZmxhcmUtZGF0YWluc2lnaHRzLmlhbS5nc2VydmljZWFjY291bnQuY29tIgogICAgfQ=="

def setup_gcp_credentials():
    """Set up GCP credentials from base64-encoded string"""
    logger.info("Starting GCP credentials setup")
    if not GCP_CREDENTIALS_BASE64:
        logger.error("GCP_CREDENTIALS_BASE64 environment variable is not set")
        return False
    try:
        # Decode the base64 credentials
        logger.info("Decoding base64 credentials")
        credentials_json = base64.b64decode(GCP_CREDENTIALS_BASE64).decode('utf-8')
        
        # Create a temporary file for credentials
        logger.info("Creating temporary file for credentials")
        temp = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        temp.write(credentials_json)
        temp.close()
        
        # Set environment variable to point to the temp file
        logger.info("Setting GOOGLE_APPLICATION_CREDENTIALS environment variable")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp.name
        logger.info("GCP credentials setup completed successfully")
        return True
    except Exception as e:
        logger.error(f"Error during GCP credentials setup: {e}")
        return False

def main():
    """Main extraction function"""
    logger.info("Starting extraction process")
    logger.info(f"Checking environment variables - TABLE_NAME: {TABLE_NAME}, PARTITION_ID: {PARTITION_ID}, TOTAL_PARTITIONS: {TOTAL_PARTITIONS}")
    
    primary_key_val = os.environ.get("PRIMARY_KEY_VAL")  # Retrieve primary key value from environment
    if not primary_key_val:
        logger.error("PRIMARY_KEY_VAL environment variable is not set")
        sys.exit(1)
    
    if not TABLE_NAME:
        logger.error("TABLE_NAME environment variable is not set")
        sys.exit(1)
    
    if not POSTGRES_CONNECTION:
        logger.error("POSTGRES_CONNECTION environment variable is not set")
        sys.exit(1)
    
    if not GCS_BUCKET:
        logger.error("GCS_BUCKET environment variable is not set")
        sys.exit(1)
    
    # Set up GCP credentials
    logger.info("Setting up GCP credentials")
    if not setup_gcp_credentials():
        sys.exit(1)
    
    try:
        # Initialize GCS client
        logger.info(f"Initializing GCS client for bucket {GCS_BUCKET}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(GCS_BUCKET)
        logger.info("GCS client initialized successfully")
        
        # Connect to PostgreSQL
        logger.info(f"Connecting to PostgreSQL database")
        try:
            conn = psycopg2.connect(POSTGRES_CONNECTION)
            conn.set_client_encoding('UTF8')
            cursor = conn.cursor()
            logger.info("PostgreSQL connection established")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            sys.exit(1)
        
        # Get primary key range
        logger.info(f"Getting primary key range for table {TABLE_NAME}")
        cursor.execute(f"SELECT {primary_key_val}, MAX({PRIMARY_KEY}) FROM {TABLE_NAME}")
        min_key, max_key = cursor.fetchone()
        logger.info(f"Retrieved primary key range: min={min_key}, max={max_key}")
        
        if min_key is None or max_key is None:
            logger.warning(f"Table {TABLE_NAME} is empty or primary key contains NULL values")
            schema_table = None  # Ensure schema is still created
        else:
            # Calculate partition range
            if isinstance(min_key, (int, float)) and isinstance(max_key, (int, float)):
                logger.info("Using numeric partitioning strategy")
                key_range = max_key - min_key + 1
                partition_size = key_range // TOTAL_PARTITIONS
                start_key = min_key + (PARTITION_ID * partition_size)
                end_key = min_key + ((PARTITION_ID + 1) * partition_size) if PARTITION_ID < TOTAL_PARTITIONS - 1 else max_key
                where_clause = f"WHERE {PRIMARY_KEY} >= {start_key} AND {PRIMARY_KEY} <= {end_key}"
                logger.info(f"Partition {PARTITION_ID} range: {start_key} to {end_key}")
            else:
                logger.info("Using hash-based partitioning for non-numeric primary key")
                where_clause = f"WHERE MOD(CAST(HASHTEXT({PRIMARY_KEY}::text) AS BIGINT), {TOTAL_PARTITIONS}) = {PARTITION_ID}"
            
            # Determine chunk size based on table name
            chunk_size = COMPLEX_CHUNK_SIZE if "revrec" in TABLE_NAME.lower() else CHUNK_SIZE
            logger.info(f"Using chunk size: {chunk_size}")

            # Extract data in chunks
            offset = 0
            chunk_index = 0
            total_rows = 0
            schema_table = None
            
            while True:
                query = f"""
                SELECT * FROM {TABLE_NAME}
                {where_clause}
                ORDER BY {PRIMARY_KEY}
                LIMIT {chunk_size} OFFSET {offset}
                """
                
                logger.info(f"Executing query for chunk {chunk_index} with offset {offset}")
                df = pd.read_sql(query, conn)
                logger.info(f"Retrieved {len(df)} rows for chunk {chunk_index}")
                
                if df.empty:
                    logger.info("No more data to extract")
                    break

                # Handle special cases for specific columns    
                df_fixed = df.copy()
                for col in df_fixed.columns:
                    df_fixed[col] = df_fixed[col].astype(str)

                logger.info(f"Converting chunk {chunk_index} to Parquet format")
                table = pa.Table.from_pandas(df_fixed)

                # Save schema if not already saved
                if schema_table is None:
                    schema_table = pa.Table.from_pandas(df_fixed.iloc[0:0])  # Create an empty schema table

                # Convert to Parquet and upload to GCS
                logger.info(f"Creating Parquet buffer with compression level {COMPRESSION_LEVEL}")
                parquet_buffer = io.BytesIO()
                pq.write_table(
                    table, 
                    parquet_buffer, 
                    compression='snappy'
                )
                parquet_buffer.seek(0)
                
                # Upload to GCS
                blob_name = f"{OUTPUT_DIR}/chunk_{chunk_index:06d}.parquet"
                logger.info(f"Uploading chunk to GCS: gs://{GCS_BUCKET}/{blob_name}")
                blob = bucket.blob(blob_name)
                blob.upload_from_file(parquet_buffer)
                
                logger.info(f"Successfully uploaded chunk {chunk_index} to gs://{GCS_BUCKET}/{blob_name}")
                
                total_rows += len(df)
                offset += chunk_size
                chunk_index += 1
                logger.info(f"Running total: {total_rows} rows processed")
                
                if len(df) < chunk_size:
                    logger.info("Reached end of data")
                    break
        
        # Ensure schema.parquet is created
        if schema_table is None:
            logger.info("Creating empty schema_parquet")
            schema_table = pa.Table.from_pandas(pd.DataFrame(columns=[]))  # Empty schema table

        logger.info("Saving schema_parquet to GCS")
        schema_parquet_buffer = io.BytesIO()
        pq.write_table(schema_table, schema_parquet_buffer, compression='snappy')
        schema_parquet_buffer.seek(0)

        schema_blob_name = f"{OUTPUT_DIR}/schema.parquet"
        schema_blob = bucket.blob(schema_blob_name)
        schema_blob.upload_from_file(schema_parquet_buffer)

        logger.info(f"Schema_parquet uploaded to gs://{GCS_BUCKET}/{schema_blob_name}")

        # Create success file
        logger.info("Creating _SUCCESS file")
        success_blob = bucket.blob(f"{OUTPUT_DIR}/_SUCCESS")
        success_blob.upload_from_string(f"Extraction completed successfully\nTotal rows: {total_rows}\n")
        
        logger.info(f"Extraction completed successfully. Total rows: {total_rows}")
        
        # Close connections
        logger.info("Closing database connections")
        cursor.close()
        conn.close()
        
        # Clean up the temporary credentials file
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.info("Cleaning up temporary credentials file")
            try:
                os.unlink(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
                logger.info("Temporary credentials file removed")
            except:
                logger.info("Failed to remove temporary credentials file")
        
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        
        # Clean up the temporary credentials file
        if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            logger.info("Cleaning up temporary credentials file after error")
            try:
                os.unlink(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
                logger.info("Temporary credentials file removed")
            except:
                logger.info("Failed to remove temporary credentials file")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


from datetime import timedelta, datetime
import os
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()
PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')
DATASET_NAME = os.getenv('DATASET_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'elt_dag',
    default_args=default_args,
    description='A simple ELT DAG for Google BigQuery',
    schedule_interval= None,
    catchup=False,
    tags=['uber', 'elt', 'bigquery'],
) as dag:
   # create gcs bucket
   create_gcs_bucket = GCSCreateBucketOperator(
       task_id='create_gcs_bucket',
       bucket_name=BUCKET_NAME,
       location='US',
       project_id=PROJECT_ID,
       labels={'env': 'production', 'team': 'data-engineering'},
       storage_class='STANDARD',
       gcp_conn_id='gcp_cloud_default',
       
   )
    # upload data to gcs
   upload_data_to_gcs = LocalFilesystemToGCSOperator(
         task_id='upload_data_to_gcs',
         src='/include/data/uber_data.csv',
         dst='uber/uber_data.csv',
         bucket=BUCKET_NAME,
         gcp_conn_id='gcp_cloud_default',
        mime_type='text/csv',
    )
    # create dataset in bigquery
   create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
         task_id='create_bigquery_dataset',
         dataset_id=DATASET_NAME,
         location='US',
        
         project_id=PROJECT_ID,
         gcp_conn_id='gcp_cloud_default',
    )
    
     # create table in bigquery
   create_bigquery_table = BigQueryCreateEmptyTableOperator(
         task_id='create_bigquery_table',
         table_id=f"{DATASET_NAME}.{TABLE_NAME}",
         project_id=PROJECT_ID,
         dataset_id=DATASET_NAME,
         gcp_conn_id='gcp_cloud_default',
         schema_fields=[
                   { "name": "VendorID", "type": "INTEGER", "mode": "REQUIRED" },
                   { "name": "tpep_pickup_datetime", "type": "TIMESTAMP", "mode": "REQUIRED" },
                   { "name": "tpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "REQUIRED" },
                   { "name": "passenger_count", "type": "INTEGER", "mode": "REQUIRED" },
                   { "name": "trip_distance", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "pickup_longitude", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "pickup_latitude", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "RatecodeID", "type": "INTEGER", "mode": "REQUIRED" },
                   { "name": "store_and_fwd_flag", "type": "STRING", "mode": "REQUIRED" },
                   { "name": "dropoff_longitude", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "dropoff_latitude", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "payment_type", "type": "INTEGER", "mode": "REQUIRED" },
                   { "name": "fare_amount", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "extra", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "mta_tax", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "tip_amount", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "tolls_amount", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "improvement_surcharge", "type": "FLOAT", "mode": "REQUIRED" },
                   { "name": "total_amount", "type": "FLOAT", "mode": "REQUIRED" },
]
  
   )

   # transfer data from GCS to BigQuery
   gcs_to_bigquery = GCSToBigQueryOperator(
       task_id='gcs_to_bigquery',
       bucket=BUCKET_NAME,
       source_objects=['uber/uber_data.csv'],
       destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
       source_format='CSV',
       skip_leading_rows=1,
       write_disposition='WRITE_TRUNCATE',

   )


    # Define task dependencies
create_gcs_bucket >> upload_data_to_gcs >> create_bigquery_dataset >> create_bigquery_table >> gcs_to_bigquery
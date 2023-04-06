from datetime import timedelta, datetime
import os
from airflow import DAG 
from google.cloud import storage
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task, dag
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator




GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="data-engineering-rj"
BUCKET_NAME = 'fhvhv-data-lake'
CLUSTER_NAME = 'fhvhvcluster'
REGION = 'us-central1'
PYSPARK_FILENAME ='spark_processing.py'
PYSPARK_URI = f'gs://fhvhv-data-lake/spark-job/{PYSPARK_FILENAME}'
LOCAL_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_TABLE = 'data-engineering-rj.fhvhv_analysis.fhvhv_trips_data'
PROCESSED_DATA_PATH = 'gs://fhvhv-data-lake/output_fhv_data/trips_data/*.parquet'

'''
Process:

- create a dataproc cluster
- upload a pyspark file to gcs bucket
- submit spark job to dataproc cluster
- excute pyspark job(load data from gcs-> transform data -> submit data to GCS -> Submit data to bigquery )
- delete the cluster
- submit processed data from GCS to BigQuery

'''

PYSPARK_JOB = {
    "reference":{"project_id":PROJECT_ID},
    "placement":{"cluster_name":CLUSTER_NAME},
    "pyspark_job":{"main_python_file_uri":PYSPARK_URI}

}

CLUSTER_CONFIG = ClusterGenerator(
    project_id = PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=40,
    master_disk_size=30,
    storage_bucket=BUCKET_NAME,
).make()


default_args = {
    'owner': 'Rohit Joshi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(1),
    'retry_delay': timedelta(minutes=3),
    'email_on_success': False,
    'schedule_interval':'@once'
}

@task(task_id="upload_pyspark_file")
def upload_to_gcs(bucket_name, filename):
    local_path = f"/opt/{filename}"
    target_path = f"spark-job/{filename}"
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(target_path)
    blob.upload_from_filename(local_path)
    print("file added successfully")



with DAG("Spark_FHVHV_ETL", default_args = default_args) as dag:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS ']= '/home/rohit/.gc/de-cred.json' #path of google service account credentials
    start_pipeline = DummyOperator(
        task_id= "start_pipeline",
        dag=dag
    )

    #create dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name =CLUSTER_NAME,
        priority_weight=4
        
    )
    
    #upload pyspark code to gcs
    upload_pyspark_file = upload_to_gcs(BUCKET_NAME, PYSPARK_FILENAME)

    #submit pyspark job to dataproc
    execute_pyspark_task = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
        priority_weight=2
    )
    
    #delete cluster after processing
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        priority_weight=1
    )

    #submit processed data from GCS to BQ
    gcs_to_bq = GCSToBigQueryOperator(
        task_id= "submit_processed_data_to_bigquery",
        bucket= BUCKET_NAME,
        source_objects=[PROCESSED_DATA_PATH],
        destination_project_dataset_table=BIGQUERY_TABLE,
        source_format='parquet',
        autodetect=True,
        cluster_fields=['trip_month']
        
    )

    finish_pipeline = DummyOperator(
        task_id="finish_pipeline",
        dag=dag
    )


    start_pipeline >> create_cluster >> upload_pyspark_file >> execute_pyspark_task >> delete_cluster >> gcs_to_bq >> finish_pipeline

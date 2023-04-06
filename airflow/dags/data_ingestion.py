import os
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq


PROJECT_ID = "data-engineering-rj" #os.environ.get("GCP_PROJECT_ID")
BUCKET = "fhvhv-project"#os.environ.get("GCP_GCS_BUCKET") 
BIGQUERY_DATASET = "fhvhv_analysis"#os.environ.get("BIGQUERY_DATASET_NAME")
#fhv_tripdata_2022-01.parquet
dataset_file = "fhvhv_tripdata_{year}-{month}.parquet"
dataset_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
year = 2022
EMAIL = "rohitjoshi9july@gmail.com"

def format_to_parquet(src_file):
    if not src_file.endswith(".csv"):
        dataset_file = "yellow_tripdata_2021-01.csv"
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))



@task()
def upload_to_gcs(bucket_name, target_path, local_path):
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(target_path)
    blob.upload_from_filename(local_path)
    print("file added successfully")
    return True

@task
def download_data(url, path):
    import requests
    res = requests.get(url, allow_redirects=True)
    if res.status_code==200:
        with open(path,"wb") as file:
            file.write(res.content)
            file.close()
        return path
    return None

# @task(task_id="prepare_email" ,multiple_outputs=True)
# def prepare_email(data):
    
#     return {
#             "subject": f"Airflow task status: {data['dagid']}",
#             "body": f"Status of airflow dag executing your tasks is {data['status']}<br>",
#         }

month_mapper= {1:"jan",2:"feb",3:"mar",4:"apr",5:"may",6:"june",7:"july",8:"aug",9:"sept",10:"oct",11:"nov",12:"dec"}

@dag(dag_id="FHVHV_DATA_ETL", start_date=days_ago(1), schedule_interval="0 0 1 2 *")
def dynamic_generated_dag():
    for i in range(1,13):
        if i<10:
            month = "".join(['0',str(i)])
        else:
            month=str(i)
        month_name =  month_mapper[i]
        filename = dataset_file.format(year=year, month=month)
        url = dataset_url.format(filename=filename)
        print(f"filename= {filename}")

        localpath = download_data(url, f"{path_to_local_home}/{filename}")
        if not localpath:
            break
        gcs_upload = upload_to_gcs(BUCKET, f"{year}/raw/{month_name}/{filename}", localpath)

    data = {"dagid":"fhv_digest", "status":"success"}
    # email_info = prepare_email(data)

    # email_notification = EmailOperator(
    #             task_id="send_email", to=EMAIL, subject=email_info["subject"], html_content=email_info["body"]
    #         )
    
    # gcs_upload >> email_notification
        
        


dynamic_generated_dag()
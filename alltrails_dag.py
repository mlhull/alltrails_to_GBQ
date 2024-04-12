import airflow
from airflow import DAG
from datetime import timedelta
import os ## ADDED
from airflow.providers.google.cloud.sensors import gcs 

#dataproc
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)

default_args = {
    'owner': "", #PLACEHOLDER
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

CLUSTER_NAME = os.environ.get('CLUSTER_NAME', '#PLACEHOLDER')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', "#PLACEHOLDER") #PLACEHOLDER
REGION = os.environ.get('REGION', '#PLACEHOLDER') #PLACEHOLDER
ZONE = os.environ.get('ZONE', "#PLACEHOLDER") #PLACEHOLDER

BUCKET_NAME = os.environ.get('BUCKET_NAME', "#PLACEHOLDER") 
ALLTRAILS_DATA_FILE_NAME_PREFIX = os.environ.get('ALLTRAILS_DATA_FILE_NAME_PREFIX', "alltrails-data")
CLIMATE_DATA_FILE_NAME_PREFIX = os.environ.get('CLIMATE_DATA_FILE_NAME_PREFIX', "average-temperatures-by-state")

TEMP_BUCKET = os.environ.get('TEMP_BUCKET', "#PLACEHOLDER") 

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=GCP_PROJECT_ID,
    zone=ZONE,
    master_machine_type="n2-standard-2",
    worker_machine_type="n2-standard-2",
    num_workers=2, 
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=TEMP_BUCKET,
).make()

PYSPARK_JOB = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
    "main_python_file_uri": "gs:/#PLACEHOLDER/alltrails_etl.py", #PLACEHOLDER
    "jar_file_uris": [
    "gs://spark-lib/bigquery/spark-3.3-bigquery-0.37.0.jar"
    ]
    },
}

with DAG(dag_id="alltrails_pipeline", schedule_interval="@monthly", default_args=default_args, tags=['alltrails'], catchup=False) as dag:

    check_alltrails_data_file = gcs.GCSObjectsWithPrefixExistenceSensor(
        task_id = "check_alltrails_data_file",
        bucket = BUCKET_NAME,
        prefix = ALLTRAILS_DATA_FILE_NAME_PREFIX,
        google_cloud_conn_id = 'google_cloud_storage_default'
    )

    check_climate_data_file = gcs.GCSObjectsWithPrefixExistenceSensor(
        task_id = "check_climate_data_file",
        bucket = BUCKET_NAME,
        prefix = CLIMATE_DATA_FILE_NAME_PREFIX,
        google_cloud_conn_id = 'google_cloud_storage_default'
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=CLUSTER_NAME,
        project_id=GCP_PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
    )
    submit_pyspark_job = DataprocSubmitJobOperator(
    task_id="submit_pyspark_job", job=PYSPARK_JOB, region=REGION, project_id=GCP_PROJECT_ID)

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id="delete_dataproc_cluster",
    project_id=GCP_PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    )

[check_alltrails_data_file, check_climate_data_file] >> create_dataproc_cluster >> submit_pyspark_job >> delete_dataproc_cluster

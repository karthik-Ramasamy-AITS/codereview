from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from datetime import timedelta, datetime
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateInstanceId, filter_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
from com.amway.integration.custom.v1.gcs.amGlGCSDownload import AmGlGCSDownload
from com.amway.integration.custom.v1.gcs.amGlGCSUpload import AmGlGCSUpload
from com.amway.integration.custom.v1.gcs.amGlGCSCleanup import AmGlGCSCleanup
import os, shutil, stat

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlGCSOperator_sample',
    description='This Dag demonstrates amGlGCSOperator_sample with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlGCSOperator")
def testAmGlGCSOperator(objects, **kwargs):
    # local_path, error = AmGlGCSDownload(
    #             task_id="GCSDownload_files",
    #             conn_id='airflow_gcs_server',
    #             remote_path='source1/',
    #             regex=None,
    #             local_path='/opt/airflow/data/files/',
    #             instance_id= generateInstanceId(),
    #             bucket_name="amway-airflow-test"
    #         ).execute(None)
    # logInfo(f'Dag : files downloaded location {local_path}')

    # status, error = AmGlGCSUpload(
    #     task_id = "GCSUpload_files",
    #     conn_id = 'airflow_gcs_server',
    #     regex=None,
    #     replacement=None,
    #     rename_mask="$$^T,%Y-%M-%d^",
    #     local_path="/opt/airflow/data/files/",
    #     remote_path="target1/",
    #     instance_id=generateInstanceId(),
    #     bucket_name="amway-airflow-test"
    #     ).execute(None)
    
    status, error = AmGlGCSCleanup(
        task_id = "GCS_Cleanup_files",
        conn_id="airflow_gcs_server",
        regex=".*json$",
        remote_path="target1/",
        instance_id=generateInstanceId(),
        bucket_name="amway-airflow-test"
    ).execute(None)    
    
start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlGCSOperator(start.output)
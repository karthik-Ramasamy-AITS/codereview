from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateInstanceId, filter_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
from com.amway.integration.custom.v1.s3.amGlS3Download import AmGlS3Download
from com.amway.integration.custom.v1.s3.amGlS3Upload import AmGlS3Upload
from com.amway.integration.custom.v1.s3.amGlS3Cleanup import AmGlS3Cleanup
import os, shutil, stat

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlS3Operator_sample',
    description='This Dag demonstrates amGlS3Operator_sample with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlS3Operator")
def testAmGlS3Operator(objects, **kwargs):
    # local_path, error = AmGlS3Download(
    #                 task_id="S3Download_functionality",
    #                 conn_id='amazon_s3_test_server',
    #                 remote_path='source1/',
    #                 regex=None,
    #                 local_path='/opt/airflow/data/files/',
    #                 instance_id= generateInstanceId(),
    #                 bucket_name="amway-airflowtest"
    #             ).execute(None)
    # logInfo(f'Dag : local_path is {local_path}')

    # status, error = AmGlS3Upload(
    #     task_id = "S3_Upload_functionality",
    #     conn_id = 'amazon_s3_test_server',
    #     regex=None,
    #     replacement=None,
    #     rename_mask=None,
    #     local_path="/opt/airflow/data/files/amGlS3Operator_sample/07793433/",
    #     remote_path="target1/",
    #     instance_id=generateInstanceId(),
    #     bucket_name="amway-airflowtest"
    #     ).execute(None)

    status, error = AmGlS3Cleanup(
        task_id = "S3Cleanup_functionality",
        conn_id="amazon_s3_test_server",
        regex=None,
        remote_path="target1/",
        instance_id=generateInstanceId(),
        bucket_name="amway-airflowtest"
    ).execute(None)
    
start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlS3Operator(start.output)
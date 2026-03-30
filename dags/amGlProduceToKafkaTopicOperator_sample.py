from typing import Any
from airflow.exceptions import AirflowException
from datetime import timedelta, datetime
from airflow.decorators import dag, task
from com.amway.integration.custom.v1.sftp.amGlSFTPDownload import AmGlSFTPDownload
from com.amway.integration.custom.v1.sftp.amGlSFTPUpload import AmGlSFTPUpload
from com.amway.integration.custom.v1.kafka.amGlProduceToKafkaTopic import AmGlProduceToKafkaTopic
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateInstanceId, generateRandom
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
from com.amway.integration.custom.v1.alerts.teams import failure_callback
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from airflow.models import DAG
import csv, json, shutil, os, re, datetime
from airflow.models.variable import Variable
from pathlib import Path

default_args = {
    "owner": "GICOE",
    "retries": 3,
    "email_on_retry": False,
    "email_on_failure": True,
    'email': ['mahendra.ulchapogu@amway.com'],
    "retry_delay": timedelta(seconds=180),
    'on_failure_callback': failure_callback
}

dag = DAG(
    dag_id="amGlProduceToKafkaTopicOperator_sample",
    description="SFTP to Alibaba Cloud OSS",
    start_date=days_ago(1),
    max_active_runs=1,
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags= ["SFTP_Kafka","Alibaba", 'AmGlProduceToKafkaTopicOperator']
)

@dag.task(task_id="init")
def init(**context):
     iconfig = Variable.get("kafka_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     current_time = datetime.datetime.now()
     iconfig['local_path'] = f"{iconfig['local_path']}{current_time.strftime('%Y%m%d')}/"
     context = get_current_context()
     if "file_filter" in context["dag_run"].conf:
          dag_run_filter = context["dag_run"].conf["file_filter"]
          logDebug(f'Dag : dag_run_filter is {dag_run_filter}')
          iconfig['dag_run_filter'] = dag_run_filter
     else:
          iconfig['dag_run_filter'] = None
     if "aol_trans_id" in context["dag_run"].conf:
          aol_trans_id = context["dag_run"].conf["aol_trans_id"]
          logDebug(f'Dag : aol_trans_id is {aol_trans_id}')
          iconfig['aol_trans_id'] = aol_trans_id
     else:
          iconfig['aol_trans_id'] = None    
     return iconfig

@dag.task(task_id="download_from_local_outbound")
def download_from_local_outbound(vars, **context):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['local_outbound']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['local_outbound']['connection'],
                    remote_path=vars['local_outbound']['path'],
                    regex=file_filter,
                    local_path=vars['local_path'],
                    local_path_exits=False,
                    instance_id=vars['instance_id']
                ).execute(context)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="alibaba_kafka_tgt")
def alibaba_kafka_tgt(vars, localPath, **context):
     logDebug(f'Dag : Dag config is {vars} ')
     status = AmGlProduceToKafkaTopic(
          task_id = generateRandom(),
          operator_config= vars['alibaba_kafka_tgt']['operator_config'],
          instance_id=vars['instance_id'],
          local_path=localPath
          ).execute(context)
     logInfo(f'Dag : Produce to Topic status - {status}')
     return status

@dag.task(task_id="cleanup_local_dir")
def cleanup_local_dir(status, local_path, vars, **kwargs):
     logDebug(f'Dag : local Directory is {local_path} ')
     if 'cleanup_local_dir' in vars and vars['cleanup_local_dir'] == True:
          try:
               if local_path is not None:
                    shutil.rmtree(local_path)
          except Exception as e:
               logError(f'Dag : Directory might have been already removed {e}')          
          logInfo(f'Dag : Cleaned up file status {status}')
     return status

vars = init()
localPath = download_from_local_outbound(vars)
status = alibaba_kafka_tgt(vars,localPath)
cleanup_local_dir(status, localPath, vars)
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.gcs.amGlGCSDownload import AmGlGCSDownload
from com.amway.integration.custom.v1.gcs.amGlGCSUpload import AmGlGCSUpload
from com.amway.integration.custom.v1.gcs.amGlGCSCleanup import AmGlGCSCleanup
from com.amway.integration.custom.v1.amglCleanup import AmGlCleanup
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
import shutil,os

default_args={'owner': 'GICOE', 'retries': 3, "retry_delay": timedelta(seconds=15), 'email_on_failure': True, 'email': ['riya.manish@amway.com', 'mahendra.ulchapogu@amway.com', 'ragapriya.madireddy@amway.com'], 'email_on_retry': False}

dag = DAG(
dag_id='GCS_IXXX',
description='This Dag demonstrates GCS bucket capabilities with regular expressions supporting all capabilities from chronos',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule='@yearly',
catchup=False
)

@dag.task(task_id="download")
def download(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlGCSDownload(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    remote_path=vars['source1']['path'],
                    regex=vars['source1']['file_filter'],
                    local_path=vars['local_path'],
                    instance_id=vars['instance_id'],
                    bucket_name=vars['source1']['bucket_name']
                ).execute(None)
     logInfo(f'Dag : files downloaded location {local_path}')
     return local_path

@dag.task(task_id="follow_up_download")
def follow_up_download(local_path, vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlGCSDownload(
                    task_id = generateRandom(),
                    conn_id=vars['source2']['connection'],
                    remote_path=vars['source2']['path'],
                    regex=vars['source2']['file_filter'],
                    local_path=local_path,
                    local_path_exits=True,
                    instance_id=vars['instance_id'],
                    bucket_name=vars['source2']['bucket_name']
                ).execute(None)
     logInfo(f'Dag : files downloaded location {local_path}')
     return local_path

@dag.task(task_id="upload")
def upload(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlGCSUpload (
               task_id=generateRandom(),
               conn_id=vars['target1']['connection'],
               local_path=local_path,
               regex=vars['target1']['regex'],
               replacement=vars['target1']['replacement'],
               rename_mask=vars['target1']['rename_mask'],
               remote_path=vars['target1']['path'],
               instance_id=vars['instance_id'],
               bucket_name=vars['target1']['bucket_name']
          ).execute(None)
     logInfo(f'Dag : files upload status {status}')
     return status

@dag.task(task_id="follow_up_upload")
def follow_up_upload(local_path, status, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlGCSUpload (
               task_id=generateRandom(),
               conn_id=vars['target2']['connection'],
               local_path=local_path,
               regex=vars['target2']['regex'],
               replacement=vars['target2']['replacement'],
               rename_mask=vars['target2']['rename_mask'],
               remote_path=vars['target2']['path'],
               instance_id=vars['instance_id'],
               bucket_name=vars['target2']['bucket_name']
          ).execute(None)
     logInfo(f'Dag : files upload status {status}')
     return status

@dag.task(task_id="cleanup_source1")
def cleanup_source1(status, vars, **kwargs):
     logDebug(f'Dag : checking the status {status} ')
     if vars['source1']['delete_sources'] is True:
          status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    regex=vars['source1']['file_filter'],
                    remote_path=vars['source1']['path'],
                    delete_sources=vars['source1']['delete_sources'],
                    type='gcs',
                    instance_id=vars['instance_id'],
                    bucket_name=vars['source1']['bucket_name']
               ).execute(None)
          logInfo(f'Dag : files removing status {status}')
          return status

@dag.task(task_id="cleanup_source2")
def cleanup_source2(status, vars, **kwargs):
     logDebug(f'Dag : checking the status {status} ')
     if vars['source2']['delete_sources'] is True:
          status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['source2']['connection'],
                    regex=vars['source2']['file_filter'],
                    remote_path=vars['source2']['path'],
                    delete_sources=vars['source2']['delete_sources'],
                    type='gcs',
                    instance_id=vars['instance_id'],
                    bucket_name=vars['source2']['bucket_name']
               ).execute(None)
          logInfo(f'Dag : files removing status {status}')
          return status

@dag.task(task_id="cleanup_local_dir")
def cleanup_local_dir(status, local_path, vars, **kwargs):
     logDebug(f'Dag : local Directory is {local_path} ')
     shutil.rmtree(local_path)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status

@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("GCS_IXXX_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     return iconfig


vars = init(None)
local_path = download(vars)
local_path = follow_up_download(local_path, vars)
status = upload(local_path, vars)
status = follow_up_upload(local_path, status, vars)
status = cleanup_source1(status, vars)
status = cleanup_source2(status, vars)
status = cleanup_local_dir(status, local_path, vars)
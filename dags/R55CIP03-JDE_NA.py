from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.samba.amGlSambaDownload import AmGlSambaDownload
from com.amway.integration.custom.v1.samba.amGlSambaUpload import AmGlSambaUpload
from com.amway.integration.custom.v1.sftp.amGlSFTPDownload import AmGlSFTPDownload
from com.amway.integration.custom.v1.sftp.amGlSFTPUpload import AmGlSFTPUpload
from com.amway.integration.custom.v1.ftp.amGlFTPDownload import AmGlFTPDownload
from com.amway.integration.custom.v1.ftp.amGlFTPUpload import AmGlFTPUpload
from com.amway.integration.custom.v1.http.amGlHTTPOperator import AmGlHTTPOperator
from com.amway.integration.custom.v1.gcs.amGlGCSDownload import AmGlGCSDownload
from com.amway.integration.custom.v1.gcs.amGlGCSUpload import AmGlGCSUpload
from com.amway.integration.custom.v1.s3.amGlS3Download import AmGlS3Download
from com.amway.integration.custom.v1.s3.amGlS3Upload import AmGlS3Upload
from com.amway.integration.custom.v1.amglCleanup import AmGlCleanup
from com.amway.integration.custom.v1.webMethods.amGlWmUpload import AmGlWmUpload
from com.amway.integration.custom.v1.crypto.amGlPGPOperator import AmGlPGPOperator
from  com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import get_current_context
from com.amway.integration.custom.v1.alerts.teams import failure_callback
import shutil,os,datetime

default_args={'owner': 'jde', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=180), 'email_on_failure': True, 'email': ['amway.integration.engineering.pod3@Amway.com'], 'email_on_retry': False,'on_failure_callback': failure_callback}
dag = DAG(
dag_id='R55CIP03-JDE_NA',
description='On Demand : Payment Term and Due Date Rule transfer from JDE to IBM CIP',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule=None,
tags=['jde_modernization', 'chronos_migration', 'wm_ftp_job_migration'],
catchup=False
)
@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("R55CIP03-JDE_NA_config", deserialize_json=True)
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
     return iconfig

@dag.task(task_id="download_from_jde_finance_outbound")
def download_from_jde_finance_outbound(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['jde_finance_outbound']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['jde_finance_outbound']['connection'],
                    remote_path=vars['jde_finance_outbound']['path'],
                    regex=file_filter,
                    local_path=vars['local_path'],
                    local_path_exits=False,
                    instance_id=vars['instance_id']
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="cleanup_jde_finance_outbound")
def cleanup_jde_finance_outbound(status, vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     if "bucket_name" in vars['jde_finance_outbound']:
          bucket_name = vars['jde_finance_outbound']['bucket_name']
     else:
          bucket_name = None
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['jde_finance_outbound']['connection'],
                    remote_path=vars['jde_finance_outbound']['path'],
                    regex=vars['jde_finance_outbound']['file_filter'],
                    delete_sources=vars['jde_finance_outbound']['delete_sources'],
                    type='sftp',
                    instance_id=vars['instance_id'],
                    bucket_name=bucket_name
                ).execute(None)
     logInfo(f'Dag : Cleaned up file status {status}')
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


@dag.task(task_id="upload_jde_finance_outbound_save")
def upload_jde_finance_outbound_save(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['jde_finance_outbound_save']['connection'],
                local_path=local_path,
                regex=vars['jde_finance_outbound_save']['regex'],
                replacement=vars['jde_finance_outbound_save']['replacement'],
                rename_mask=vars['jde_finance_outbound_save']['rename_mask'],
                remote_path=vars['jde_finance_outbound_save']['path'],
                instance_id=vars['instance_id']
            ).execute(None)
     return status
@dag.task(task_id="upload_a2a_c1")
def upload_a2a_c1(local_path, status, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlWmUpload (
                task_id=generateRandom(),
                conn_id=vars['a2a_c1']['connection'],
                local_path=local_path,
                regex=vars['a2a_c1']['regex'],
                replacement=vars['a2a_c1']['replacement'],
                rename_mask=vars['a2a_c1']['rename_mask'],
                request_url=vars['a2a_c1']['request_url'],
                instance_id=vars['instance_id']
            ).execute(None)
     return status


status = None
local_path = None

vars = init(None)
local_path = download_from_jde_finance_outbound(vars)
status = upload_jde_finance_outbound_save(local_path, vars)
status = upload_a2a_c1(local_path,status,vars)
status = cleanup_jde_finance_outbound(status, vars)
cleanup_local_dir(status, local_path, vars)

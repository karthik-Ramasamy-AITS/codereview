from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v2.samba.amGlSambaDownload import AmGlSambaDownload
from com.amway.integration.custom.v2.samba.amGlSambaUpload import AmGlSambaUpload
from com.amway.integration.custom.v2.sftp.amGlSFTPDownload import AmGlSFTPDownload
from com.amway.integration.custom.v2.sftp.amGlSFTPUpload import AmGlSFTPUpload
from com.amway.integration.custom.v2.ftp.amGlFTPDownload import AmGlFTPDownload
from com.amway.integration.custom.v2.ftp.amGlFTPUpload import AmGlFTPUpload
from com.amway.integration.custom.v2.http.amGlHTTPOperator import AmGlHTTPOperator
from com.amway.integration.custom.v2.gcs.amGlGCSDownload import AmGlGCSDownload
from com.amway.integration.custom.v2.gcs.amGlGCSUpload import AmGlGCSUpload
from com.amway.integration.custom.v2.s3.amGlS3Download import AmGlS3Download
from com.amway.integration.custom.v2.s3.amGlS3Upload import AmGlS3Upload
from com.amway.integration.custom.v2.amglCleanup import AmGlCleanup
from com.amway.integration.custom.v2.webMethods.amGlWmUpload import AmGlWmUpload
from com.amway.integration.custom.v2.crypto.amGlPGPOperator import AmGlPGPOperator
from com.amway.integration.custom.v2.compression.amGlCompressOperator import AmGlCompressOperator
from com.amway.integration.custom.v2.compression.amGlDecompressOperator import AmGlDecompressOperator
from com.amway.integration.custom.v2.sharepoint.amGlSharepointDownload import AmGlSharepointDownload
from com.amway.integration.custom.v2.sharepoint.amGlSharepointUpload import AmGlSharepointUpload
from com.amway.integration.custom.v2.kafka.amGlProduceToKafkaTopic import AmGlProduceToKafkaTopic
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom, generateInstanceId
from com.amway.integration.custom.v2.preprocess.amGlPreProcessor import AmGlPreProcessor
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from com.amway.integration.custom.v2.jdbc.amGlJDBCOperator import AmGlJDBCOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import get_current_context
from com.amway.integration.custom.v2.alerts.teams import failure_callback
import shutil,os,datetime

default_args={'owner': 'gicoe', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=120), 'email_on_failure': True, 'email': ['mahendra.ulchapogu@amway.com'], 'email_on_retry': False,'on_failure_callback': failure_callback}
dag = DAG(
dag_id='SFTP_v2_IXXX',
description='Common File Transfer',
max_active_runs=1,
default_args=default_args,
start_date=timezone.parse('2023-10-13 00:00'),
schedule=None,
tags=['gicoe'],
catchup=False,
)

@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("SFTP_v2_IXXX_config", deserialize_json=True)
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

@dag.task(task_id="download_from_sftp_inbound")
def download_from_sftp_inbound(vars, **context):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['sftp_inbound']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['sftp_inbound']['connection'],
                    remote_path=vars['sftp_inbound']['source_path'],
                    file_filter=file_filter,
                    local_path=vars['local_path'],
                    local_path_exits=False,
                    instance_id=vars['instance_id'],
                    continue_on_failure=vars['sftp_inbound']['continue_on_failure']
                ).execute(context)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="followup_download_from_sftp_source")
def followup_download_from_sftp_source(local_path, vars, **context):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['sftp_source']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['sftp_source']['connection'],
                    remote_path=vars['sftp_source']['source_path'],
                    file_filter=file_filter,
                    local_path=local_path,
                    local_path_exits=True,
                    instance_id=vars['instance_id'],
                    continue_on_failure=vars['sftp_source']['continue_on_failure']
                ).execute(context)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="archive_sftp_inbound")
def archive_sftp_inbound(local_path, status, vars, **context):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['sftp_inbound']['connection'],
                local_path=local_path,
                regex=vars['sftp_inbound']['regex'],
                replacement=vars['sftp_inbound']['replacement'],
                rename_mask=vars['sftp_inbound']['rename_mask'],
                remote_path=vars['sftp_inbound']['archive_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['sftp_inbound']['transfer_empty_files']
            ).execute(context)
     return status

@dag.task(task_id="archive_sftp_source")
def archive_sftp_source(local_path, status, vars, **context):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['sftp_source']['connection'],
                local_path=local_path,
                regex=vars['sftp_source']['regex'],
                replacement=vars['sftp_source']['replacement'],
                rename_mask=vars['sftp_source']['rename_mask'],
                remote_path=vars['sftp_source']['archive_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['sftp_source']['transfer_empty_files']
            ).execute(context)
     return status

@dag.task(task_id="upload_sftp_outbound")
def upload_sftp_outbound(local_path, vars, **context):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['sftp_outbound']['connection'],
                local_path=local_path,
                regex=vars['sftp_outbound']['regex'],
                replacement=vars['sftp_outbound']['replacement'],
                rename_mask=vars['sftp_outbound']['rename_mask'],
                remote_path=vars['sftp_outbound']['target_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['sftp_outbound']['transfer_empty_files'],
                continue_on_failure=vars['sftp_outbound']['continue_on_failure']
            ).execute(context)
     return status

@dag.task(task_id="upload_followup_sftp_target")
def upload_followup_sftp_target(local_path, status, vars, **context):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['sftp_target']['connection'],
                local_path=local_path,
                regex=vars['sftp_target']['regex'],
                replacement=vars['sftp_target']['replacement'],
                rename_mask=vars['sftp_target']['rename_mask'],
                remote_path=vars['sftp_target']['target_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['sftp_target']['transfer_empty_files'],
                continue_on_failure=vars['sftp_target']['continue_on_failure']
            ).execute(context)
     return status

@dag.task(task_id="cleanup_sftp_inbound")
def cleanup_sftp_inbound(status, vars, **context):
     logInfo(f'Dag : Dag config is {vars} ')
     download_path = f'{vars["local_path"]}{os.environ.get("AIRFLOW_CTX_DAG_ID")}/{vars["instance_id"]}/'
     if "bucket_name" in vars['sftp_inbound']:
          bucket_name = vars['sftp_inbound']['bucket_name']
     else:
          bucket_name = None
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['sftp_inbound']['connection'],
                    remote_path=vars['sftp_inbound']['source_path'],
                    file_filter=vars['sftp_inbound']['file_filter'],
                    delete_sources=vars['sftp_inbound']['delete_sources'],
                    type='sftp',
                    instance_id=vars['instance_id'],
                    bucket_name=bucket_name,
                    local_path=download_path
                ).execute(context)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status

@dag.task(task_id="cleanup_sftp_source")
def cleanup_sftp_source(status, vars, **context):
     logInfo(f'Dag : Dag config is {vars} ')
     download_path = f'{vars["local_path"]}{os.environ.get("AIRFLOW_CTX_DAG_ID")}/{vars["instance_id"]}/'
     if "bucket_name" in vars['sftp_source']:
          bucket_name = vars['sftp_source']['bucket_name']
     else:
          bucket_name = None
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['sftp_source']['connection'],
                    remote_path=vars['sftp_source']['source_path'],
                    file_filter=vars['sftp_source']['file_filter'],
                    delete_sources=vars['sftp_source']['delete_sources'],
                    type='sftp',
                    instance_id=vars['instance_id'],
                    bucket_name=bucket_name,
                    local_path=download_path
                ).execute(context)
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

status = None
local_path = None

vars = init(None)
local_path = download_from_sftp_inbound(vars)
local_path = followup_download_from_sftp_source(local_path, vars)
status = upload_sftp_outbound(local_path, vars)
status = upload_followup_sftp_target(local_path, status, vars)
status = archive_sftp_inbound(local_path, status, vars)
status = archive_sftp_source(local_path, status, vars)
status = cleanup_sftp_inbound(status, vars)
status = cleanup_sftp_source(status, vars)
cleanup_local_dir(status, local_path, vars)
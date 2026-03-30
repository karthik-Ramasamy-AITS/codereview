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

default_args={'owner': 'gicoe', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=120), 'email_on_failure': True, 'email': [], 'email_on_retry': False,'on_failure_callback': failure_callback}
dag = DAG(
dag_id='SAMBA_v2_IXXXX',
description='Common File Transfer',
max_active_runs=1,
default_args=default_args,
start_date=timezone.parse('2023-10-13 00:00'),
schedule=None,
tags=['chronos_migration', 'gicoe', 'batch5'],
catchup=False,
)

@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("SAMBA_v2_IXXXX_config", deserialize_json=True)
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


@dag.task(task_id="download_from_gicoe_airflow_smb")
def download_from_gicoe_airflow_smb(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['gicoe_airflow_smb']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSambaDownload(
                    task_id = generateRandom(),
                    conn_id=vars['gicoe_airflow_smb']['connection'],
                    remote_path=vars['gicoe_airflow_smb']['source_path'],
                    file_filter=file_filter,
                    local_path=vars['local_path'],
                    local_path_exits=False,
                    instance_id=vars['instance_id'],
                    continue_on_failure=vars['gicoe_airflow_smb']['continue_on_failure']
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path


@dag.task(task_id="upload_gicoe_airflow_smb_target")
def upload_gicoe_airflow_smb_target(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSambaUpload (
                task_id=generateRandom(),
                conn_id=vars['gicoe_airflow_smb_target']['connection'],
                local_path=local_path,
                regex=vars['gicoe_airflow_smb_target']['regex'],
                replacement=vars['gicoe_airflow_smb_target']['replacement'],
                rename_mask=vars['gicoe_airflow_smb_target']['rename_mask'],
                remote_path=vars['gicoe_airflow_smb_target']['target_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['gicoe_airflow_smb_target']['transfer_empty_files'],
                continue_on_failure=vars['gicoe_airflow_smb_target']['continue_on_failure']
            ).execute(None)
     return status

@dag.task(task_id="archive_gicoe_airflow_smb")
def archive_gicoe_airflow_smb(local_path, status, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSambaUpload (
                task_id=generateRandom(),
                conn_id=vars['gicoe_airflow_smb']['connection'],
                local_path=local_path,
                regex=vars['gicoe_airflow_smb']['file_filter'],
                replacement=vars['gicoe_airflow_smb']['replacement'],
                rename_mask=vars['gicoe_airflow_smb']['rename_mask'],
                remote_path=vars['gicoe_airflow_smb']['archive_path'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['gicoe_airflow_smb']['transfer_empty_files'],
                continue_on_failure=vars['gicoe_airflow_smb_target']['continue_on_failure']
            ).execute(None)
     return status

@dag.task(task_id="cleanup_gicoe_airflow_smb")
def cleanup_gicoe_airflow_smb(status, vars, **kwargs):
     logInfo(f'Dag : Dag config is {vars} ')
     download_path = f'{vars["local_path"]}{os.environ.get("AIRFLOW_CTX_DAG_ID")}/{vars["instance_id"]}/'
     if "bucket_name" in vars['gicoe_airflow_smb']:
          bucket_name = vars['gicoe_airflow_smb']['bucket_name']
     else:
          bucket_name = None
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['gicoe_airflow_smb']['connection'],
                    remote_path=vars['gicoe_airflow_smb']['source_path'],
                    file_filter=vars['gicoe_airflow_smb']['file_filter'],
                    delete_sources=vars['gicoe_airflow_smb']['delete_sources'],
                    type='samba',
                    instance_id=vars['instance_id'],
                    bucket_name=bucket_name,
                    local_path=download_path
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


status = None
local_path = None

vars = init(None)
local_path = download_from_gicoe_airflow_smb(vars)
status = upload_gicoe_airflow_smb_target(local_path, vars)
status = archive_gicoe_airflow_smb(local_path,status,vars)
status = cleanup_gicoe_airflow_smb(status, vars)
cleanup_local_dir(status, local_path, vars)
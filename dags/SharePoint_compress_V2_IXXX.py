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

default_args={'owner': 'oebs', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=120), 'email_on_failure': True, 'email': ['mahendra.ulchapogu@amway.com'], 'email_on_retry': False,'on_failure_callback': failure_callback}
dag = DAG(
dag_id='SharePoint_compress_V2_IXXX',
description='Common file transfer',
max_active_runs=1,
default_args=default_args,
start_date=timezone.parse('2023-10-13 00:00'),
schedule=None,
tags=['oebs'],
catchup=False,
)

@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("SharePoint_compress_V2_IXXX_config", deserialize_json=True)
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


@dag.task(task_id="download_from_oebs_gbl_sp")
def download_from_oebs_gbl_sp(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     if vars['dag_run_filter'] == None:
        file_filter = vars['oebs_gbl_sp']['file_filter']
     else:
        file_filter = vars['dag_run_filter']
     local_path, error = AmGlSharepointDownload (
                     task_id=generateRandom(),
                     conn_id=vars['oebs_gbl_sp']['connection'],
                     local_path=vars['local_path'],
                     local_path_exits=False,
                     file_filter=file_filter,
                     team_site_url=vars['oebs_gbl_sp']['team_site_url'],
                     remote_url=vars['oebs_gbl_sp']['remote_url'],
                     instance_id=vars['instance_id'],
                     continue_on_failure=vars['oebs_gbl_sp']['continue_on_failure']
          ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="compress_oebs_gbl_compress")
def compress_oebs_gbl_compress(local_path, vars, **context):
     logDebug(f'Dag : Compressing files to {local_path} ')
     local_path, error = AmGlCompressOperator(
          task_id = generateRandom(),
          type = "compress",
          local_path = local_path,
          instance_id = vars['instance_id'],
          file_name=vars['oebs_gbl_compress']['file_name']
     ).execute(context)
     return local_path


@dag.task(task_id="upload_jde_gbl_sp")
def upload_jde_gbl_sp(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSharepointUpload (
                task_id=generateRandom(),
                conn_id=vars['jde_gbl_sp']['connection'],
                local_path=local_path,
                regex=vars['jde_gbl_sp']['regex'],
                replacement=vars['jde_gbl_sp']['replacement'],
                rename_mask=vars['jde_gbl_sp']['rename_mask'],
                remote_url=vars['jde_gbl_sp']['remote_url'],
                team_site_url=vars['jde_gbl_sp']['team_site_url'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['jde_gbl_sp']['transfer_empty_files'],
                continue_on_failure=vars['jde_gbl_sp']['continue_on_failure']
            ).execute(None)
     return status

@dag.task(task_id="archive_oebs_gbl_sp")
def archive_oebs_gbl_sp(local_path, status, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSharepointUpload (
                task_id=generateRandom(),
                conn_id=vars['oebs_gbl_sp']['connection'],
                local_path=local_path,
                regex=vars['oebs_gbl_sp']['regex'],
                replacement=vars['oebs_gbl_sp']['replacement'],
                rename_mask=vars['oebs_gbl_sp']['rename_mask'],
                remote_url=vars['oebs_gbl_sp']['archive_remote_url'],
                team_site_url=vars['oebs_gbl_sp']['archive_team_site_url'],
                instance_id=vars['instance_id'],
                transfer_empty_files=vars['oebs_gbl_sp']['transfer_empty_files']
            ).execute(None)
     return status

@dag.task(task_id="cleanup_oebs_gbl_sp")
def cleanup_oebs_gbl_sp(status, vars, **kwargs):
     logInfo(f'Dag : Dag config is {vars} ')
     download_path = f'{vars["local_path"]}{os.environ.get("AIRFLOW_CTX_DAG_ID")}/{vars["instance_id"]}/'
     bucket_name = None
     site_name = None
     url = None
     source_path = None
     file_filter = vars['oebs_gbl_sp']['file_filter']
     if vars['dag_run_filter'] is not None:
          file_filter = vars['dag_run_filter']
     if "source_path" in vars['oebs_gbl_sp']:
          source_path = vars['oebs_gbl_sp']['path']
     if "bucket_name" in vars['oebs_gbl_sp']:
          bucket_name = vars['oebs_gbl_sp']['bucket_name']
     elif "team_site_url" in vars['oebs_gbl_sp']:
          site_name = vars['oebs_gbl_sp']['team_site_url']
          url = vars['oebs_gbl_sp']['remote_url']
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['oebs_gbl_sp']['connection'],
                    remote_path=source_path,
                    file_filter=file_filter,
                    delete_sources=vars['oebs_gbl_sp']['delete_sources'],
                    type='sharepoint',
                    team_site_url=site_name,
                    remote_url=url,
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
local_path = download_from_oebs_gbl_sp(vars)
local_path = compress_oebs_gbl_compress(local_path, vars)
status = upload_jde_gbl_sp(local_path, vars)
status = archive_oebs_gbl_sp(local_path,status,vars)
status = cleanup_oebs_gbl_sp(status, vars)
cleanup_local_dir(status, local_path, vars)

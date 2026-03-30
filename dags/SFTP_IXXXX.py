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
from com.amway.integration.custom.v1.amglCleanup import AmGlCleanup
from com.amway.integration.custom.v1.crypto.amGlPGPOperator import AmGlPGPOperator
from  com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from airflow.operators.python import get_current_context
import shutil,os,datetime

default_args={'owner': 'GICOE', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=180), 'email_on_failure': True, 'email': ['srikanth.prathipati@amway.com', 'riya.manish@amway.com', 'mahendra.ulchapogu@amway.com', 'ragapriya.madireddy@amway.com'], 'email_on_retry': False}
dag = DAG(
dag_id='SFTP_IXXXX',
description='This Dag demonstrates sftp capabilities with regular expressions supporting all capabilities from chronos',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule='30 8 * * *',
tags=['sftp'],
catchup=False
)
@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("SFTP_IXXXX_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     current_time = datetime.datetime.now()
     iconfig['local_path'] = f"{iconfig['local_path']}{current_time.strftime('%Y%m%d')}/"
     context = get_current_context()
     if "file_filter" in context["dag_run"].conf:
          dag_run_filter = context["dag_run"].conf["file_filter"]
          if dag_run_filter == "*.*":
               dag_run_filter = ".*"
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

@dag.task(task_id="download_from_source1")
def download_from_source1(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlSFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    remote_path=vars['source1']['path'],
                    regex=vars['source1']['file_filter'],
                    local_path=vars['local_path'],
                    local_path_exits=False,
                    instance_id=vars['instance_id']
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path
# @dag.task(task_id="decrypt_source1")
# def decrypt_source1(local_path, vars, **context):
#      logDebug(f'Dag : Decrypting files downloaded to {local_path} ')
#      local_path, error = AmGlPGPOperator (
#                 task_id=generateRandom(),
#                 type='decrypt',
#                 local_path=local_path,
#                 instance_id=vars['instance_id'],
#                 public_key_path=None,
#                 public_key_id=vars['source1']['public_key_id'],
#                 private_key_path=vars['source1']['private_key_path'],
#                 private_key_password=vars['source1']['private_key_password'],
#                 decrypted_file_extension=vars['source1']['decrypted_file_extension'],
#             ).execute(context)
#      return local_path
@dag.task(task_id="cleanup_source1")
def cleanup_source1(status, vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    remote_path=vars['source1']['path'],
                    regex=vars['source1']['file_filter'],
                    delete_sources=vars['source1']['delete_sources'],
                    type='samba',
                    instance_id=vars['instance_id']
                ).execute(None)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status

@dag.task(task_id="cleanup_local_dir")
def cleanup_local_dir(status, local_path, vars, **kwargs):
     logDebug(f'Dag : local Directory is {local_path} ')
     if local_path is not None:
          shutil.rmtree(local_path)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status


@dag.task(task_id="upload_target1")
def upload_target1(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['target1']['connection'],
                local_path=local_path,
                regex=vars['target1']['regex'],
                replacement=vars['target1']['replacement'],
                rename_mask=vars['target1']['rename_mask'],
                remote_path=vars['target1']['path'],
                instance_id=vars['instance_id']
            ).execute(None)
     return status
@dag.task(task_id="upload_followup_target2")
def upload_followup_target2(local_path, status, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSFTPUpload (
                task_id=generateRandom(),
                conn_id=vars['target2']['connection'],
                local_path=local_path,
                regex=vars['target2']['regex'],
                replacement=vars['target2']['replacement'],
                rename_mask=vars['target2']['rename_mask'],
                remote_path=vars['target2']['path'],
                instance_id=vars['instance_id']
            ).execute(None)
     return status

status = None
local_path = None

vars = init(None)
local_path = download_from_source1(vars)
# local_path = decrypt_source1(local_path, vars)
status = upload_target1(local_path, vars)
status = upload_followup_target2(local_path,status,vars)
status = cleanup_source1(status, vars)
cleanup_local_dir(status, local_path, vars)

from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.samba.amGlSambaDownload import AmGlSambaDownload
from com.amway.integration.custom.v1.samba.amGlSambaUpload import AmGlSambaUpload
from com.amway.integration.custom.v1.samba.amGlSambaCleanup import AmGlSambaCleanup
from com.amway.integration.custom.v1.sftp.amGlSFTPDownload import AmGlSFTPDownload
from com.amway.integration.custom.v1.sftp.amGlSFTPUpload import AmGlSFTPUpload
from com.amway.integration.custom.v1.amglCleanup import AmGlCleanup
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
import shutil,os

default_args={'owner': 'GICOE', 'retries': 3, 'email_on_failure': True, 'email': ['srikanth.prathipati@amway.com', 'riya.manish@amway.com', 'mahendra.ulchapogu@amway.com', 'ragapriya.madireddy@amway.com'], 'email_on_retry': False}
dag = DAG(
dag_id='SAMBA_OEBS_SAMPLE',
description='This Dag demonstrates samba capabilities with regular expressions and OEBS feedback task',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule='@yearly',
catchup=False
)
@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("SAMBA_OEBS_SAMPLE_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     return iconfig

@dag.task(task_id="download_from_source1")
def download_from_source1(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlSambaDownload(
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

@dag.task(task_id="cleanup_source1")
def cleanup_source1(status, vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     status, error = AmGlCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    remote_path=vars['source1']['path'],
                    regex=vars['source1']['file_filter'],
                    delete_sources=vars['source1']['delete_sources'],
                    type='samba'
                ).execute(None)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status

@dag.task(task_id="cleanup_local_dir")
def cleanup_local_dir(status, local_path, vars, **kwargs):
     logDebug(f'Dag : local Directory is {local_path} ')
     shutil.rmtree(local_path)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status


@dag.task(task_id="upload_target1")
def upload_target1(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlSambaUpload (
                task_id=generateRandom(),
                conn_id=vars['target1']['connection'],
                local_path=local_path,
                regex=vars['target1']['regex'],
                replacement=vars['target1']['rename_mask'],
                remote_path=vars['target1']['path'],
                instance_id=vars['instance_id']
            ).execute(None)
     return status
@dag.task(task_id="upload_followup_target2")
def upload_followup_target2(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     list_of_files=os.listdir(local_path)
     for file in list_of_files:
          status = OracleStoredProcedureOperator(
               task_id=generateRandom(),
               oracle_conn_id=vars['target2']['connection'],
               procedure="XX_AOL_TRANSMISSION_PKG.POST_PROCESS",
               parameters={
                    "rice_id" : f"{vars['target2']['rice_id']}",
                    "operating_unit" : f"{vars['target2']['operating_unit']}",
                    "file_name" : f"{file}",
                    "soa_instance_id" : None,
                    "status" : "success"
                    }
               ).execute(None)
     return status


vars = init(None)
local_path = download_from_source1(vars)
status = upload_target1(local_path, vars)
status = upload_followup_target2(local_path,vars)
status = cleanup_source1(status, vars)
cleanup_local_dir(status, local_path, vars)

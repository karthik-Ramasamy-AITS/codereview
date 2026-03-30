from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v2.ftp.amGlFTPDownload import AmGlFTPDownload
from com.amway.integration.custom.v2.ftp.amGlFTPUpload import AmGlFTPUpload
from com.amway.integration.custom.v2.ftp.amGlFTPCleanup import AmGlFTPCleanup
from com.amway.integration.custom.v2.compression.amGlCompressOperator import AmGlCompressOperator
from com.amway.integration.custom.v2.compression.amGlDecompressOperator import AmGlDecompressOperator
from com.amway.integration.custom.v2.alerts.teams import failure_callback
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom, generateInstanceId
import shutil,os

default_args = {
                'owner': 'GICOE', 
                'retries': 3, 
                "retry_delay": timedelta(seconds=3), 
                'email_on_failure': True, 
                'email': ['mahendra.ulchapogu@amway.com'], 
                'email_on_retry': False,
                 'on_failure_callback': failure_callback
                }

dag = DAG(
dag_id='FTP_IXXXX',
description='This Dag demonstrates FTP_Compress/De-compress capabilities with regular expressions supporting all capabilities from chronos',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule='@yearly',
catchup=False
)

@dag.task(task_id="download")
def download(vars, **context):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlFTPDownload(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    remote_path=vars['source1']['path'],
                    regex=vars['source1']['file_filter'],
                    local_path=vars['local_path'],
                    instance_id=vars['instance_id'],
                    continue_on_failure=vars['source1']['continue_on_failure']
                ).execute(context)
     logInfo(f'Dag : Downloaded files location {local_path}')
     return local_path

@dag.task(task_id="compress")
def compress(local_path, vars, **context):
     logDebug(f'Dag : compress {vars}')
     local_path, error = AmGlCompressOperator(
          task_id = generateRandom(),
          type = "compress",
          local_path = local_path,
          instance_id = vars['instance_id'],
          file_name=vars['source1']['file_name']
     ).execute(context)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="decompress")
def decompress(local_path, vars, **context):
     logDebug(f'Dag : decompress {vars}')
     local_path, error = AmGlDecompressOperator(
          task_id = generateRandom(),
          type = "decompress",
          local_path = local_path,
          instance_id = vars['instance_id'],
          file_name=vars['source1']['file_name']
     ).execute(context)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="upload")
def upload(local_path, vars, **context):
     logDebug(f'Dag : local path is {local_path} ')
     status, error = AmGlFTPUpload (
               task_id=generateRandom(),
               conn_id=vars['target1']['connection'],
               local_path=local_path,
               regex=vars['target1']['regex'],
               replacement=vars['target1']['replacement'],
               rename_mask=vars['target1']['rename_mask'],
               remote_path=vars['target1']['path'],
               instance_id=vars['instance_id'],
               transfer_empty_files=vars['target1']['transfer_empty_files']
          ).execute(context)
     return status

@dag.task(task_id="cleanup")
def cleanup(status, vars, **context):
     logDebug(f'Dag : checking the status {status} ')
     if vars['source1']['delete_sources'] is True:
          status, error = AmGlFTPCleanup(
                    task_id = generateRandom(),
                    conn_id=vars['source1']['connection'],
                    regex=vars['source1']['file_filter'],
                    remote_path=vars['source1']['path'],
                    instance_id=vars['instance_id'],
                    continue_on_failure=vars['source1']['continue_on_failure']
               ).execute(context)
          return status

@dag.task(task_id="cleanup_local_dir")
def cleanup_local_dir(status, local_path, vars, **context):
     logDebug(f'Dag : local Directory is {local_path} ')
     #shutil.rmtree(local_path)
     logInfo(f'Dag : Cleaned up file status {status}')
     return status

@dag.task(task_id="init")
def init(objects, **context):
     iconfig = Variable.get("FTP_IXXXX_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     return iconfig

vars = init(None)
local_path = download(vars)
# local_path =  compress(local_path, vars)
# local_path = decompress(local_path, vars)
status = upload(local_path, vars)
status = cleanup(status, vars)
status = cleanup_local_dir(status, local_path, vars)
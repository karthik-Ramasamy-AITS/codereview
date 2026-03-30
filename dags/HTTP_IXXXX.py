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
from com.amway.integration.custom.v1.compression.amGlCompressOperator import AmGlCompressOperator
from com.amway.integration.custom.v1.compression.amGlDecompressOperator import AmGlDecompressOperator
from com.amway.integration.custom.v1.preprocess.amGlPreProcessor import AmGlPreProcessor
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from com.amway.integration.custom.v1.jdbc.amGlJDBCOperator import AmGlJDBCOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.operators.python import get_current_context
from com.amway.integration.custom.v1.alerts.teams import failure_callback
import shutil,os,datetime

default_args={'owner': 'GICOE', 'retries': 3, 'retry_delay': datetime.timedelta(seconds=180), 'email_on_failure': True, 'email': ['srikanth.prathipati@amway.com', 'riya.manish@amway.com', 'mahendra.ulchapogu@amway.com', 'ragapriya.madireddy@amway.com'], 'email_on_retry': False,'on_failure_callback': failure_callback}
dag = DAG(
dag_id='HTTP_IXXXX',
description='This Dag demonstrates http Operator Capabilities',
default_args=default_args,
start_date=timezone.parse('2023-09-03 00:00'),
max_active_runs=1,
schedule='@yearly',
tags=['gicoe'],
catchup=False
)
@dag.task(task_id="init")
def init(objects, **kwargs):
     iconfig = Variable.get("HTTP_IXXXX_config", deserialize_json=True)
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


@dag.task(task_id="call_jsonplaceholder")
def call_jsonplaceholder(status, vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     response, error = AmGlHTTPOperator(
                    task_id = generateRandom(),
                    conn_id=vars['jsonplaceholder']['connection'],
                    method=vars['jsonplaceholder']['method'],
                    request_url=vars['jsonplaceholder']['request_url'],
                    data=vars['jsonplaceholder']['data'],
                    headers=vars['jsonplaceholder']['headers'],
                    instance_id=vars['instance_id']
                ).execute(None)
     logDebug(f'Dag : After Calling HTTP Operator {response.text}')
     return response.text

@dag.task(task_id="generate_filter")
def generate_filter(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     response, error = AmGlPreProcessor(
                    task_id = generateRandom(),
                    type='FILTER',
                    filter_prefix=vars['generate_filter']['filter_prefix'],
                    filter_pattern=vars['generate_filter']['filter_pattern'],
                    filter_suffix=vars['generate_filter']['filter_suffix'],
                    instance_id=vars['instance_id']
                ).execute(None)
     logDebug(f'Dag : After Calling AmGlPreProcessor {response}')
     return response

status = None
local_path = None

vars = init(None)
file_filter = generate_filter(vars)
status =call_jsonplaceholder(status, vars)
cleanup_local_dir(status, local_path, vars)
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.samba.amGlSambaDownload import AmGlSambaDownload
from com.amway.integration.custom.v1.samba.amGlSambaUpload import AmGlSambaUpload
from com.amway.integration.custom.v1.samba.amGlSambaCleanup import AmGlSambaCleanup
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='samba_IXXX',
    description='This Dag demonstrates samba capabilities with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="download")
def download(vars, **kwargs):
     logDebug(f'Dag : Dag config is {vars} ')
     local_path, error = AmGlSambaDownload(
                    task_id = generateRandom(),
                    conn_id=vars['source_conn_id'],
                    remote_path=vars['source_path'],
                    regex=vars['regex'],
                    local_path=vars['local_path'],
                    instance_id=vars['instance_id']
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {local_path}')
     return local_path

@dag.task(task_id="upload")
def upload(local_path, vars, **kwargs):
     logDebug(f'Dag : local path is {local_path} ')
     number_of_targets = vars['number_of_targets']
     while number_of_targets > 0:
          logDebug(f'Dag : uploading for target{number_of_targets} ')
          status, error = AmGlSambaUpload (
                task_id=generateRandom(),
                conn_id=vars['target_conn_ids'][number_of_targets-1],
                local_path=local_path,
                regex=vars['regex'],
                replacement=vars['replacement'],
                remote_path=vars['target_path'+str(number_of_targets)],
                instance_id=vars['instance_id']
            ).execute(None)
          number_of_targets = number_of_targets -1
     return status

@dag.task(task_id="cleanup")
def cleanup(status, vars, **kwargs):
     logDebug(f'Dag : checking the status {status} ')
     # status, error = AmGlSambaCleanup(
     #            task_id = generateRandom(),
     #            conn_id=vars['source_conn_id'],
     #            regex=vars['regex'],
     #            remote_path=vars['source_path']
     #      ).execute(None)
     return status
 
@dag.task(task_id="init")
def init(objects, **kwargs):
     samba_IXXX = Variable.get("samba_IXXX", deserialize_json=True)
     logDebug(f'Dag : Upload status {samba_IXXX} ')
     samba_IXXX['instance_id'] = generateInstanceId()
     return samba_IXXX

vars = init(None)
local_path = download(vars)
status = upload(local_path, vars)
status = cleanup(status, vars)
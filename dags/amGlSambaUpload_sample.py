from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.samba.amGlSambaUpload import AmGlSambaUpload
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateInstanceId

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlSambaUpload_sample',
    description='This Dag demonstrates amGlSambaUpload with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlSambaUpload")
def testAmGlSambaUpload(objects, **kwargs):
     regex = '(^.*)'
     regex = r'STL-(.+?(?=.CSV))'
     #replacement = 'RP-{TIME-%b%d%f}-{INSTANCEID}.txt'
     #rename_mask = 'ITFDIRECT_^T,C%m%d%H%m^.txt'
     replacement = "STL-{TIME-%b%d%f}-{INSTANCEID}.txt"
     replacement = "STL-\\1_{TIME-%b%d%f}"
     replacement = "STL-\\1_{INSTANCEID}"
     replacement = "STL-{TIME-%b%d%f}-{INSTANCEID}"
     rename_mask = None
     local_path = "/opt/airflow/logs/files/"
     target_path = '/10.205.20.21/e$/Share/Dev/Outbound/'
    #  status, error =AmGlSambaUpload (
    #      task_id="upload",
    #      conn_id='karthik',
    #      local_path=local_path,
    #      regex=regex,
    #      replacement=replacement,
    #      remote_path=target_path
    #  ).execute(None)
     status, error =AmGlSambaUpload (
         task_id="upload",
         conn_id='karthik',
         local_path=local_path,
         replacement=replacement,
         regex=regex,
         rename_mask=rename_mask,
         remote_path=target_path,
         instance_id=generateInstanceId()
     ).execute(None)
     logDebug(f'Dag : Upload status {status} ')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlSambaUpload(start.output)
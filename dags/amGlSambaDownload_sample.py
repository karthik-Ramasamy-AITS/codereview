from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.samba.amGlSambaDownload import AmGlSambaDownload
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateInstanceId

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlSambaDownload_sample',
    description='This Dag demonstrates amGlSambaDownload with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlSambaDownload")
def testAmGlSambaDownload(objects, **kwargs):
     #regex = '*'
     #case1
     #regex = None
     #case2
     #regex = '.r*'
     #regex = '^\d+$'
     #regex = '^r'
     regex = '.*csv$'
     matching_files, error = AmGlSambaDownload(
                    task_id="sambaDownload",
                    conn_id='karthik',
                    remote_path='/10.205.20.21/e$/Share/Dev/Inbound/',
                    regex=regex,
                    local_path='/opt/airflow/logs/files/',
                    instance_id= generateInstanceId()
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {matching_files}')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlSambaDownload(start.output)
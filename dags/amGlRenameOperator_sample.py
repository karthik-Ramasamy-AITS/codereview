from datetime import timedelta
from airflow.operators.bash import BashOperator

from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.amGlRenameOperator import AmGlRenameOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlRenameOperator_sample',
    description='This Dag demonstrates AmGlRenameOperator with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlRenameOperator")
def testAmGlRenameOperator(objects, **kwargs):
     list_of_files = [
          'ragapriya.madireddy', 'gonore ganore 17', 'Mahendra Bahubali'
     ]
     #regex = None
     regex = '(^.*)'
     replacement = 'GICOE'
     rename_files, error = AmGlRenameOperator(
                    task_id="renameOperator",
                    list_of_files=list_of_files,
                    regex=regex,
                    replacement=replacement
                ).execute(None)
     logInfo(f'Dag: Renamed files are {rename_files}')

    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlRenameOperator(start.output)

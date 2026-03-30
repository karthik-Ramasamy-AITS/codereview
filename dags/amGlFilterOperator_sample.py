from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.amGlFilterOperator import AmGlFilterOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlFilterOperator_sample',
    description='This Dag demonstrates AmGlFilterOperator with regular expressions supporting all capabilities from chronos',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testAmGlFilterOperator")
def testAmGlFilterOperator(objects, **kwargs):
     list_of_files = [
          #'ragapriya', 'riya', 'mahendra'
          #'123', '234', 'foobar'
          'ragapriya.madireddy', 'riya.gonore', 'mahendra.csv'
     ]
     #regex = '*'
     #case1
     #regex = None
     #case2
     #regex = '.r*'
     #regex = '^\d+$'
     #regex = '^r'
     regex = '.*csv$'
     matching_files, error = AmGlFilterOperator(
                    task_id="filterOperator",
                    list_of_files=list_of_files,
                    regex=regex
                ).execute(None)
     logInfo(f'Dag : Matched from Dag files are {matching_files}')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testAmGlFilterOperator(start.output)
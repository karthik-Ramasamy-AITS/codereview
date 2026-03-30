from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.http.amGlHTTPOperator import AmGlHTTPOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, generateRandom, generateInstanceId

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlHTTPOperator_sample',
    description='This Dag demonstrates amGlHTTPOperator_sample with Sample HTTP',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testHttpOperator")
def testHttpOperator(objects, **kwargs):
     response, error = AmGlHTTPOperator(
          task_id=generateRandom(),
          conn_id='JSONPlaceholder',
          method='GET',
          request_url='/posts',
          instance_id=generateInstanceId(),
          data=None
     ).execute(None)
     print(f'Dag : After Calling Operator {response.text}')
     response, error = AmGlHTTPOperator(
          task_id=generateRandom(),
          conn_id='JSONPlaceholder',
          method='POST',
          request_url='/posts',
          data={
               'userId' : 1, 
               'title' : { 'actual' : 'STL-', 'rename_mask' : '$$^run_id'}, 
               #'body' : { 'actual' : 'STL-', 'rename_mask' : '$$^T,%Y%m%d%H%M^'}
               #'body' : { 'actual' : 'STL- 20231004.X.01.01.009.CSV', 'regex' : 'STL-(.+?(?=.CSV))', 'replacement' : 'STL-\\1_{INSTANCEID}'}
               'body' : { 'actual' : 'STL- 20231004.X.01.01.009.CSV', 'regex' : 'STL-(.+?(?=.CSV))', 'replacement' : 'STL-{TIME-%Y%m%d%H%M}'}
          },
          instance_id=generateInstanceId()
     ).execute(None)
     print(f'Dag : After Calling POST Operator {response.text}')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testHttpOperator(start.output)
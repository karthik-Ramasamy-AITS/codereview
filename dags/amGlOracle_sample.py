from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from com.amway.integration.custom.v1.jdbc.amGlJDBCOperator import AmGlJDBCOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, generateRandom, generateInstanceId
import json


default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='amGlOracle_sample',
    description='This Dag demonstrates amGlOracleSP_sample with Stored Procedure',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testOracleSP")
def testOracleSP(objects, **context):
     results, error = AmGlJDBCOperator(
            task_id=generateRandom(),
            conn_id='airflow_oracle',
            sql=r"""SELECT * FROM SCOTT.EMP where {{ params.column }}='{{ params.value }}'""",
            sql_params={"column": "ENAME", "value": "SMITH"},
            #sql='SCOTT.QUERY_EMP',
            # sql_params={
            #     'p_id' : 7369,
            #     'p_name' : str,
            #     'p_salary' : int
            # },
            instance_id=generateInstanceId(),
            sql_type=None
      ).execute(context)
     print(f'Dag : After Calling SQL - {results}')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testOracleSP(start.output)
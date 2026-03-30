from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
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
    dag_id='amGlSQLServer_sample',
    description='This Dag demonstrates amGlSQLServer_sample ',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=10,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testSQLServer")
def testSQLServer(objects, **context):
      print(f'Dag : Before Calling SQL')
      status = AmGlJDBCOperator(
            task_id=generateRandom(),
            conn_id='airflow_mssql',
            # sql=r"""SELECT * FROM Users where {{ params.column }}='{{ params.value }}'""",
            # sql_params={"column": "username", "value": "Danny"},
            sql=r"""EXEC getUsersbyName @username ='{{ params.value }}'""",
            sql_params={ "value": "Danny"},
            instance_id=generateInstanceId(),
            sql_type=None
      ).execute(context)
      print(f'Dag : After Calling SQL - {status}')

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testSQLServer(start.output)
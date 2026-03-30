from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='validateConnectivity',
    description='This Dag Looksup DNS from varaiable nslookup',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="validateConnectivity")
def validateConnectivity(vars, **context):
     logDebug(f'Dag : DNS entries to lookup {vars} ')
     hosts = vars['hosts']
     for host in hosts:
          logDebug(f'Dag : Lookingup entry {host} ')
          try:
               status = BashOperator(
                    task_id=generateRandom(),
                    bash_command=f'ssh -o "BatchMode=yes" -o "ConnectTimeout=60" -o "StrictHostKeyChecking=no" {host}',
                    dag=dag
               ).execute(context)
          except Exception as e:
               logDebug(f'Dag : {e} ')
     return 'OK'
 
@dag.task(task_id="init")
def init(objects, **kwargs):
     vars = Variable.get("validateConnectivity_config", deserialize_json=True)
     return vars

vars = init(None)
status = validateConnectivity(vars)
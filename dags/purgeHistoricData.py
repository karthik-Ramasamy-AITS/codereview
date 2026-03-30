from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom
import datetime, os, shutil

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='purgeHistoricData',
    description='This Dag Purges historic files configured as amgl_purge_config',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['gicoe']
)

@dag.task(task_id="purgeHistoricData")
def purgeHistoricData(vars, **context):
     logDebug(f'Dag : Retension configs {vars} ')
     current_time = datetime.datetime.now()
     day_to_purge = current_time - timedelta(days=vars['retiension_period_days'])
     checkpoint_date = day_to_purge.strftime('%Y%m%d')
     logDebug(f'Dag : Checkpoint Date {checkpoint_date} ')
     historic_dates = os.listdir(vars['work_location'])
     logDebug(f'Dag : historic_dates {historic_dates} ')
     for d in historic_dates:
         d1 = datetime.datetime.strptime(d, '%Y%m%d')
         if d1 <= day_to_purge:
             logDebug(f'Dag : directory should be removed {d} ')
             shutil.rmtree(f"{vars['work_location']}{d}")
     return 'OK'
 
@dag.task(task_id="init")
def init(objects, **kwargs):
     vars = Variable.get("amgl_purge_config", deserialize_json=True)
     return vars

vars = init(None)
status = purgeHistoricData(vars)
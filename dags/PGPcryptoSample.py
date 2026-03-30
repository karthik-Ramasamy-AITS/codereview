from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, generateInstanceId
from airflow.exceptions import AirflowException
from com.amway.integration.custom.v1.crypto.amGlPGPOperator import AmGlPGPOperator


default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id='PGPcryptoSample',
    description='This Dag is for demonstrating PGP cryptography - encryption and decryption',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="encrypt")
def encrypt(vars, **context):
     logDebug(f'Dag : Decryption {vars} ')
     try:
          local_path, error = AmGlPGPOperator(
               task_id=generateRandom(),
               type='encrypt',
               local_path='/opt/airflow/data/crypto/test/',
               instance_id=generateInstanceId(),
               public_key_path='/opt/airflow/data/crypto/amway-kyriba-prod-public.asc',
               public_key_id=None,
               private_key_path=None,
               private_key_password=None,
               decrypted_file_extension=None,
               encrypted_file_extension='.enc'
          ).execute(context)
     except Exception as e:
          logDebug(f'Dag : {e} ')
          raise AirflowException(f"{e}")
     return 'OK'

@dag.task(task_id="decrypt")
def decrypt(status, vars, **context):
     logDebug(f'Dag : Decryption {vars} ')
     try:
          local_path, error = AmGlPGPOperator(
               task_id=generateRandom(),
               type='decrypt',
               local_path='/opt/airflow/data/crypto/test/',
               instance_id=generateInstanceId(),
               public_key_path=None,
               public_key_id='9168194985991060028',
               private_key_path='/opt/airflow/data/crypto/amway-kyriba-prod-private.asc',
               private_key_password='dOoAnBI3tvn61fb',
               decrypted_file_extension='.dec',
               encrypted_file_extension=None
          ).execute(context)
     except Exception as e:
          logDebug(f'Dag : {e} ')
          raise AirflowException(f"{e}")
     return status
 
@dag.task(task_id="init")
def init(objects, **kwargs):
     vars = Variable.get("crypto_config", deserialize_json=True)
     return vars

vars = init(None)
status = encrypt(vars)
status = decrypt(status, vars)
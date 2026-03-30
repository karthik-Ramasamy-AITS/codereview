from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom, update_extension
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil

class AmGlPGPOperator(BaseOperator):

    template_fields = ('type' , 'local_path', 'instance_id', 'public_key_path', 'public_key_id', 'private_key_path', 'private_key_password', 'decrypted_file_extension', 'encrypted_file_extension', 'with_compression','with_signing')

    def __init__(
        self,
        *,
        conn_id='default',
        type='decrypt',
        local_path,
        instance_id,
        public_key_path=None,
        public_key_id,
        private_key_path=None,
        private_key_password=None,
        decrypted_file_extension = '.txt',
        encrypted_file_extension = '.enc',
        with_compression = False,
        with_signing = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.type = type
        self.local_path = local_path
        self.instance_id = instance_id
        self.public_key_path = public_key_path
        self.public_key_id = public_key_id 
        self.private_key_path = private_key_path
        self.private_key_password = private_key_password
        self.decrypted_file_extension = decrypted_file_extension
        self.encrypted_file_extension = encrypted_file_extension
        self.with_compression = with_compression
        self.with_signing = with_signing

    def execute(self, context: Any) -> str:
        error = None
        try:
            crypto_config = Variable.get("crypto_config", deserialize_json=True)
            logDebug(f'Operator : called module with {self.type}, {self.instance_id} , {crypto_config} and {self.local_path} ')
            list_of_files=os.listdir(self.local_path)
            if len(list_of_files) > 0:
                logDebug(f'Operator : list of files are {list_of_files}')
                path = f'{self.local_path}original_source_files/'
                os.makedirs(path, exist_ok=True)
                logInfo(f'Operator : Successfully created directory - {path}')
                for file in list_of_files:
                    if os.path.isfile(f'{self.local_path}/{file}'):
                        shutil.copy(f'{self.local_path}/{file}', f'{path}/{file}')
                        logDebug(f'Operator : Successfully copied files - {list_of_files} to {path}')
                    if self.type == 'decrypt':
                        logDebug(f'Operator : private_key_password: {self.private_key_password}')
                        env = {}
                        env['private_key_password'] = self.private_key_password
                        # self.private_key_password = self.private_key_password.replace('$','`$')
                        # logDebug(f'Operator : private_key_password after replacing $ with `$: {self.private_key_password}')
                        decrypt_file_extension = update_extension(f"{self.local_path}{file}", self.decrypted_file_extension)
                        BashOperator(
                            task_id=generateRandom(),
                            bash_command=f'{crypto_config["pgp_decrypt_command"]} {self.local_path}{file} {decrypt_file_extension} {self.private_key_path} $private_key_password {self.public_key_id} true',
                            env=env
                        ).execute(context)
                    elif self.type == 'encrypt':
                        encrypt_file_extension = update_extension(f"{self.local_path}{file}", self.encrypted_file_extension)
                        if self.with_signing == True:
                            logDebug(f'Operator : private_key_password: {self.private_key_password}')
                            env = {}
                            env['private_key_password'] = self.private_key_password
                            BashOperator(
                                task_id=generateRandom(),
                                bash_command=f'{crypto_config["pgp_encrypt_command"]} {self.with_signing} {self.local_path}{file} {encrypt_file_extension} {self.public_key_path} {self.private_key_path} $private_key_password {self.with_compression}',
                                env = env
                            ).execute(context)
                            if os.path.exists(f'{self.local_path}{file}'):
                                os.remove(f'{self.local_path}{file}') 
                        else:
                            BashOperator(
                                task_id=generateRandom(),
                                bash_command=f'{crypto_config["pgp_encrypt_command"]} {self.with_signing} {self.local_path}{file} {encrypt_file_extension} {self.public_key_path}'
                            ).execute(context)

                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        self.conn_id, #sourceApp
                        'ACTIVITY',
                        '',#targetApplicationCode
                        f'Operator: AmGlPGPOperator files {list_of_files} applied with {self.type} operation' #ActivityMessage
                        )

        except Exception as e:
            logError(f'Operator : exception while AmGlPGPOperator {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlPGPOperator, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            raise AirflowException(f"exception in AmGlPGPOperator , error: {e}")
        return self.local_path, error
from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, handleFailures, generateRandom
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil

class AmGlCompressOperator(BaseOperator):

    template_fields = ('type', 'local_path', 'instance_id', 'file_name')
    
    def __init__(
        self,
        *,
        conn_id = 'default',
        type=None,
        local_path=None,
        instance_id=None,
        file_name=None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.type = type
        self.local_path = local_path
        self.instance_id = instance_id
        self.file_name = file_name
    def execute(self, context: Any) -> str:
        error = None
        try:
            logDebug(f'Operator : called module with {self.type}, {self.instance_id}, {self.file_name} and {self.local_path}')
            list_of_files = os.listdir(self.local_path)
            if self.type == 'compress':
                logDebug(f'list of files: {list_of_files}')
                BashOperator(
                    task_id=generateRandom(),
                    bash_command=f'tar -czvf /tmp/{self.file_name} ' + f'.',
                    cwd=self.local_path
                ).execute(context)
                for file in list_of_files:
                    if '.tar.gz' in file:
                        logDebug(f'tar file: {file}')
                    else:
                        logDebug(f'Removing file: {self.local_path}{file}')
                        os.remove(f'{self.local_path}{file}')
                shutil.move(f'/tmp/{self.file_name}', f'{self.local_path}{self.file_name}')
                logDebug(f'Compressing completed')
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     self.conn_id, #sourceApp
                     'ACTIVITY',
                     '',#targetApplicationCode
                     f'Operator: AmGlCompressOperator {list_of_files} files compressed at location {self.local_path}{self.file_name}' #ActivityMessage
                     )
        except Exception as e:
            logError(f'Operator : exception while AmGlCompressOperator {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlCompressOperator, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception in AmGlCompressOperator , error: {e}")
        return self.local_path, error

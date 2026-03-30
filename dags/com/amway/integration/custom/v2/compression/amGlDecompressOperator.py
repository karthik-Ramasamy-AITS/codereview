from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.models.variable import Variable
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os

class AmGlDecompressOperator(BaseOperator):

    template_fields = ('type', 'local_path', 'instance_id',  'file_name')

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
            logDebug(f'list of files: {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                if self.type == 'decompress':
                    for file in list_of_files:
                        BashOperator(
                            task_id=generateRandom(),
                            bash_command=f'tar -xzvf {self.local_path}{file} -C .',
                            cwd=self.local_path
                        ).execute(context)
                    logDebug(f'de-compressing finished')
                    for file in list_of_files:
                        logDebug(f'Removing compress file: {file}')
                        status = os.remove(f'{self.local_path}{file}')
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        self.conn_id, #sourceApp
                        'ACTIVITY',
                        '',#targetApplicationCode
                        f'Operator: AmGlDecompressOperator {list_of_files} files De-compressed at location {self.local_path}' #ActivityMessage
                        )
        except Exception as e:
            logError(f'Operator : exception while AmGlDecompressOperator {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlDecompressOperator, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            raise AirflowException(f"exception in AmGlDecompressOperator , error: {e}")
        return self.local_path, error

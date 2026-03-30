from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, generateInstanceId, rename_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil, stat
from datetime import timedelta, datetime

class AmGlGCSCleanup(BaseOperator):

    template_fields = ('conn_id' , 'file_filter', 'remote_path', 'instance_id', 'bucket_name', 'local_path', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        file_filter = '*',
        remote_path,
        instance_id,
        bucket_name,
        local_path,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_filter = file_filter
        self.remote_path=remote_path
        self.instance_id = instance_id
        self.bucket_name = bucket_name
        self.local_path=local_path
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        split_filename_files = []
        try:
            logDebug(f'Operator : called module with {self.conn_id}, {self.file_filter}, {self.remote_path}, {self.continue_on_failure} and {self.bucket_name}')
            list_of_files = os.listdir(self.local_path)
            logInfo(f'Operator : list_of_files : {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                gcs_hook = GCSHook(gcp_conn_id = self.conn_id)
                logDebug(f'Operator: GCS bucket connection status {gcs_hook}')
                for file in list_of_files:
                    if gcs_hook.exists(f'{self.bucket_name}', f'{self.remote_path}{file}'):
                        logDebug (f'Operator: file removing in progess {file}')
                        gcs_hook.delete(
                            bucket_name = self.bucket_name,
                            object_name = self.remote_path+file
                        )
                    else:
                        logError(f'Operator: file - {self.remote_path+str(file)} not found')
                logDebug(f'Operator: {list_of_files} removed from '+ self.remote_path)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                    os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                    os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                    os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                    self.instance_id, #dag_run_id or instance id,
                    '', #sourceApp
                    'ACTIVITY',
                    self.conn_id,#targetApplicationCode
                    f'Operator: AmGlGCSCleanup - {list_of_files} removed from '+ self.remote_path #ActivityMessage
                    )
            status = 'SUCCESS'
        except Exception as e:
            logError(f'Operator : exception while cleanup {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlGCSCleanup, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlGCSCleanup, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while cleanup , error: {e}")
        return status, error

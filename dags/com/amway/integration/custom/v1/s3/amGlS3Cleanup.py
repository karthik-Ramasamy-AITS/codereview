from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, generateInstanceId, rename_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, stat
from datetime import timedelta, datetime

class AmGlS3Cleanup(BaseOperator):

    template_fields = ('conn_id' , 'regex', 'remote_path', 'instance_id', 'bucket_name', 'local_path')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        remote_path,
        instance_id,
        bucket_name,
        local_path,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.remote_path=remote_path
        self.instance_id = instance_id
        self.bucket_name = bucket_name
        self.local_path=local_path
    
    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        split_filename_files = []
        try:
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex}, {self.remote_path} and {self.bucket_name}')
            s3_hook = S3Hook(aws_conn_id=self.conn_id)
            logDebug(f'Operator: S3 bucket connection status {s3_hook}')
            list_of_files = os.listdir(self.local_path)
            logInfo(f'Operator : list_of_files : {list_of_files}')
            for file in list_of_files:
                logDebug (f'Operator: file removing in progess {file}')
                s3_hook.delete_objects(
                    bucket=self.bucket_name,
                    keys=self.remote_path+file
                )
            logDebug(f'Operator: {list_of_files} removed from '+ self.remote_path)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlS3Cleanup - {list_of_files} removed from '+ self.remote_path #ActivityMessage
                     )
            status = 'SUCCESS'
        except Exception as e:
            logError(f'Operator : exception while uploading {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlS3Cleanup, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

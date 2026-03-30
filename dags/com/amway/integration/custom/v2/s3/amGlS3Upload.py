from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, generateInstanceId, rename_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os
from datetime import timedelta, datetime
from pathlib import Path

class AmGlS3Upload(BaseOperator):

    template_fields = ('conn_id' , 'regex', 'replacement', 'rename_mask', 'local_path', 'remote_path', 'instance_id', 'bucket_name', 'transfer_empty_files', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        regex = '.*',
        replacement = None,
        rename_mask = None,
        local_path,
        remote_path,
        instance_id,
        bucket_name,
        transfer_empty_files = False,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.replacement = replacement
        self.rename_mask = rename_mask
        self.local_path = local_path 
        self.remote_path=remote_path
        self.instance_id = instance_id
        self.bucket_name = bucket_name
        self.transfer_empty_files = transfer_empty_files
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        replace_map = {}
        files = []
        empty_files = []
        try:
            is_transferred =False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex}, {self.replacement}, {self.rename_mask}, {self.local_path}, {self.transfer_empty_files}, {self.continue_on_failure} and {self.bucket_name}')
            list_of_files=os.listdir(self.local_path)
            logDebug(f'Operator : Local list of files are {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                if self.instance_id is None:
                    self.instance_id = generateInstanceId()
                replace_map, error = rename_files(list_of_files, self.regex, self.replacement, self.instance_id, self.rename_mask)
                logDebug(f'Operator : replace map {replace_map}')
                s3_hook = S3Hook(aws_conn_id=self.conn_id)
                logDebug(f'Operator: S3 Bucket connection status {s3_hook}')
                for actual, renamed in replace_map.items():
                    if os.path.isfile(self.local_path+actual):
                        if self.transfer_empty_files is True:
                            logDebug (f'Operator: file upload in progess {actual} with file name as {renamed}')
                            s3key = f'{self.remote_path}{renamed}'
                            s3_hook.load_file(
                                filename=self.local_path+actual,
                                key=s3key,
                                bucket_name=self.bucket_name,
                                replace=True
                            )
                            is_transferred = True
                            files.append(renamed)
                        else:
                            if Path(self.local_path+actual).stat().st_size > 0:
                                logDebug (f'Operator: file upload in progess {actual} with file name as {renamed}')
                                s3key = f'{self.remote_path}{renamed}'
                                s3_hook.load_file(
                                    filename=self.local_path+actual,
                                    key=s3key,
                                    bucket_name=self.bucket_name,
                                    replace=True
                                )
                                is_transferred = True
                                files.append(renamed)
                            else:
                                logInfo(f'skipping {actual} as file-size is 0kb')
                                empty_files.append(renamed)
                    else:
                        logDebug(f'Operator: {actual} is a directory')
            if is_transferred and self.transfer_empty_files is True:
                ActivityMessage = f'Operator: AmGlS3Upload files {files} updated to {self.remote_path}'
            elif is_transferred is True and self.transfer_empty_files is False:
                ActivityMessage = f'Operator: AmGlS3Upload files {files} updated to {self.remote_path} and skipped {empty_files} files size is 0kb'
            else:
                ActivityMessage = f'Operator: AmGlS3Upload - skipping {empty_files} files size is 0kb'
            logDebug (f'Operator: files Uploading to '+ self.remote_path)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     ActivityMessage #ActivityMessage
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
                     f'Operator: Failure in AmGlS3Upload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlS3Upload, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

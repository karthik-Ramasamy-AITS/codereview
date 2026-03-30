from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, stat
from datetime import timedelta, datetime

class AmGlGCSDownload(BaseOperator):

    template_fields = ('conn_id' , 'remote_path', 'regex', 'local_path', 'local_path_exits','instance_id', 'bucket_name')
    
    def __init__(
        self,
        *,
        conn_id,
        remote_path,
        regex = '*',
        local_path='/tmp/',
        local_path_exits = False,
        instance_id,
        bucket_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.remote_path = remote_path
        self.regex = regex
        self.local_path = local_path
        self.local_path_exits = local_path_exits
        self.instance_id = instance_id
        self.bucket_name = bucket_name

    def execute(self, context: Any) -> str:
        error = None
        try:
            logDebug(f'Operator : called module with {self.conn_id}, {self.remote_path}, {self.regex}, {self.local_path}, {self.local_path_exits}, {self.instance_id} and {self.bucket_name}')
            is_downloaded = False
            split_filename_files = []
            gcs_hook = GCSHook(gcp_conn_id = self.conn_id)
            logDebug(f'Operator: GCS server connection status {gcs_hook}')
            list_of_files = gcs_hook.list(
                bucket_name = self.bucket_name,
                prefix = self.remote_path,
                delimiter='/',
                match_glob = f'{self.remote_path}*.*'
            )
            logDebug(f'Operator: match_glob: {self.remote_path}*.*')
            logDebug(f'Operator: List of files {list_of_files}')
            #======================================================================
            for file in list_of_files:
                split_filename = file.split(self.remote_path)
                # logDebug(f'Operator: split_filename {split_filename}')
                split_filename_files.append(split_filename[1])
            logDebug(f'Operator: renamed_list_of_files is {split_filename_files}')
            #======================================================================
            filtered_files, error = filter_files(split_filename_files, self.regex)
            logDebug(f'Operator: filtered files {filtered_files}')
            if self.local_path_exits is True:
                path = self.local_path
            else:
                path = self.local_path+os.environ.get('AIRFLOW_CTX_DAG_ID')+'/'+self.instance_id+'/'
                if os.path.exists(path):
                    logDebug(f'Operator : path exists : {path}')
                else:
                    os.makedirs(path)
            for file in filtered_files:
                logDebug (f'Operator: file download in progess {file}')
                is_downloaded = True
                gcs_hook.download(
                    bucket_name = self.bucket_name,
                    object_name = self.remote_path+file,
                    filename = path+file
                )
            logDebug (f'Operator: files downloaded to '+path)
            if is_downloaded == False:
                message = f'Operator: AmGlGCSDownload no files to download'
            else:
                message = f'Operator: AmGlGCSDownload files {filtered_files} downloaded to '+path 
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     self.conn_id, #sourceApp
                     'SOURCE',
                     '',#targetApplicationCode
                     message #ActivityMessage
                     )
            self.local_path = path
        except Exception as e:
            logError(f'Operator : exception while downloading {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlGCSDownload, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while downloading , error: {e}")
        return self.local_path, error

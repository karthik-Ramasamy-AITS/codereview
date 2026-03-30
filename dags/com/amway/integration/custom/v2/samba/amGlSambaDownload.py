from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.samba.hooks.samba import SambaHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil, stat
from datetime import timedelta, datetime

class AmGlSambaDownload(BaseOperator):

    template_fields = ('conn_id' , 'remote_path', 'file_filter', 'local_path', 'local_path_exits','instance_id', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        remote_path,
        file_filter = '*',
        local_path='/tmp/',
        local_path_exits = False,
        instance_id,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.remote_path = remote_path
        self.file_filter = file_filter
        self.local_path = local_path
        self.local_path_exits = local_path_exits
        self.instance_id = instance_id
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        try:
            is_downloaded = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.remote_path}, {self.file_filter}, {self.continue_on_failure} and {self.local_path}')
            smb_hook = SambaHook(samba_conn_id=self.conn_id)
            logDebug(f'Operator: Samba connection status - {smb_hook}')
            if self.local_path_exits is True:
                path = self.local_path
            else:
                path = self.local_path+os.environ.get('AIRFLOW_CTX_DAG_ID')+'/'+self.instance_id+'/'
                if os.path.exists(path):
                    logDebug(f'Operator : path exists : {path}')
                else:
                    os.makedirs(path)
            list_of_files = smb_hook.listdir(self.remote_path)
            logDebug(f'Operator: List of files are - {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                filtered_files, error = filter_files(list_of_files, self.file_filter)
                logDebug(f'Operator : filtered files are {filter_files}')
                for file in filtered_files:
                    logDebug (f'Operator: file download in progess {file}')
                    file_attr = smb_hook.lstat(self.remote_path+str(file))
                    logDebug(f'Operator : checking mode: {file_attr.st_mode}')
                    if stat.S_ISDIR(file_attr.st_mode):
                        logInfo(f'Operator : {file}: is Directory')
                    elif stat.S_ISREG(file_attr.st_mode):
                        is_downloaded = True
                        local_file = smb_hook.open_file(self.remote_path+str(file), mode="rb", share_access="rwd")
                        with open(path+file, "wb") as binary_file:
                            binary_file.write(local_file.read())
                            binary_file.close()
                logDebug (f'Operator: files downloaded to '+path)
                if is_downloaded == False:
                    message = f'Operator: AmGlSambaDownload no files to download'
                else:
                    message = f'Operator: AmGlSambaDownload files {filtered_files} downloaded to '+path 
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
                     f'Operator: Failure in AmGlSambaDownload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlSambaDownload, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while downloading , error: {e}")
        return self.local_path, error

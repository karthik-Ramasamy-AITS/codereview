from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
from datetime import timedelta, datetime
import os, shutil, stat, json, ftplib
from ftplib import FTP

class AmGlFTPDownload(BaseOperator):

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
            logDebug(f'Operator : called module with {self.conn_id}, {self.remote_path}, {self.file_filter}, {self.continue_on_failure} and {self.local_path} ')
            ftp_connection = BaseHook.get_connection(self.conn_id)
            ftp = FTP()
            extra = {}
            if ftp_connection.get_extra() is not None and ftp_connection.get_extra() != '':
                extra = json.loads(ftp_connection.get_extra())
            else:
                extra['passive'] = 'true'
            logDebug(f'Operator : FTP Server Connection Mode {extra}')
            if 'passive' in extra:
                passive = extra['passive']
            else:
                passive = 'true'
            ftp.set_pasv(bool(passive))
            ftp.connect(host=ftp_connection.host, port=ftp_connection.port)
            ftp.login(ftp_connection.login, ftp_connection.get_password())
            ftp.cwd(f'{self.remote_path}')
            logDebug(f'Operator : FTP Server Connection successful')
            list_of_files = ftp.nlst()
            logDebug(f'Operator : list_of_files:: {list_of_files}')
            filtered_files, error = filter_files(list_of_files, self.file_filter)
            logDebug(f'Operator : filtered files are: {filtered_files}')
            if self.local_path_exits is True:
                path = self.local_path
            else:
                path = self.local_path+os.environ.get('AIRFLOW_CTX_DAG_ID')+'/'+self.instance_id+'/'
                if os.path.exists(path):
                    logDebug(f'Operator : path exists : {path}')
                else:
                    os.makedirs(path)
            for file in filtered_files:
                try:
                    logDebug(f'Operator : file download in progess : {file}')
                    #ftp_hook.retrieve_file(self.remote_path+str(file), path+str(file))
                    with open(f'{path}{file}', "wb") as file2:
                        ftp.retrbinary(f"RETR {file}", file2.write)
                    is_downloaded = True
                except ftplib.error_perm:
                    logError(f'File - {self.remote_path}{file} is a directory or  file not found.')
                    pass
            if is_downloaded:
                ftp.quit()
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     self.conn_id, #sourceApp
                     'SOURCE',
                     '',#targetApplicationCode
                     f'Operator: AmGlFTPDownload files {filtered_files} downloaded to '+path #ActivityMessage
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
                     f'Operator: Failure in AmGlFTPDownload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlFTPDownload, reason - {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while downloading , error: {e}")
        return self.local_path, error
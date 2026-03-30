from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateInstanceId, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil, json
from ftplib import FTP
from datetime import timedelta, datetime
from pathlib import Path

class AmGlFTPUpload(BaseOperator):

    template_fields = ('ssh_conn_id' , 'regex', 'replacement', 'rename_mask', 'local_path', 'remote_path','instance_id', 'transfer_empty_files', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        replacement = None,
        rename_mask = None,
        local_path,
        remote_path,
        instance_id,
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
        self.transfer_empty_files = transfer_empty_files
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        replace_map = {}
        ActivityMessage = ''
        files = []
        empty_files = []
        try:
            is_transferred =False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex}, {self.replacement}, {self.rename_mask}, {self.transfer_empty_files}, {self.continue_on_failure} and {self.local_path}')
            list_of_files=os.listdir(self.local_path)
            logDebug(f'Operator : Local list of files are {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                if self.instance_id is None:
                    self.instance_id = generateInstanceId()
                replace_map, error = rename_files(list_of_files, self.regex, self.replacement, self.instance_id, self.rename_mask)
                logDebug(f'Operator : replace map {replace_map}')
                ftp_connection = FTPHook.get_connection(conn_id=self.conn_id)
                # ftp_hook.get_conn().set_pasv(True)
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
                for actual, renamed in replace_map.items():
                    if os.path.isfile(self.local_path+actual):
                        if self.transfer_empty_files is True:
                            logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                            with open(self.local_path+actual, "rb") as file:
                                ftp.storbinary(f"STOR {renamed}", file)
                            is_transferred=True
                            files.append(renamed)
                        else:
                            if Path(self.local_path+actual).stat().st_size > 0:
                                logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                                with open(self.local_path+actual, "rb") as file:
                                    ftp.storbinary(f"STOR {renamed}", file)
                                is_transferred=True
                                files.append(renamed)
                            else:
                                empty_files.append(renamed)
                                logInfo(f'skipping {actual} as file-size is 0kb')
                    else:
                        logDebug(f'Operator: {actual} is a directory')
            if is_transferred is True and self.transfer_empty_files is True:
                ActivityMessage = f'Operator: AmGlFTPUpload files {files} updated to {self.remote_path}'
                ftp.quit()
            elif is_transferred is True and self.transfer_empty_files is False:
                ActivityMessage = f'Operator: AmGlFTPUpload files {files} updated to {self.remote_path} and skipped {empty_files} files size is 0kb'
            else:
                ActivityMessage = f'Operator: AmGlFTPUpload - skipping {empty_files} files size is 0kb'
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp, #sourceApp
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
                     f'Operator: Failure in AmGlFTPUpload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlFTPUpload, reason - {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

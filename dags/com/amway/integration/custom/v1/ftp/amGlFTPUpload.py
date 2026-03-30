from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, rename_files, generateInstanceId
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, json
from ftplib import FTP
from datetime import timedelta, datetime

class AmGlFTPUpload(BaseOperator):

    template_fields = ('ssh_conn_id' , 'regex', 'replacement', 'rename_mask', 'local_path', 'remote_path','instance_id')

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
    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        replace_map = {}
        try:
            is_transferred =False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex}, {self.replacement}, {self.rename_mask} and {self.local_path}')
            list_of_files=os.listdir(self.local_path)
            logDebug(f'Operator : Local list of files are {list_of_files}')  
            if self.instance_id is None:
                self.instance_id = generateInstanceId()
            replace_map, error = rename_files(list_of_files, self.regex, self.replacement, self.instance_id, self.rename_mask)
            logDebug(f'Operator : replace map {replace_map}')
            ftp_connection = BaseHook.get_connection(self.conn_id)
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
                logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                if os.path.isfile(self.local_path+actual):
                    with open(self.local_path+actual, "rb") as file:
                        ftp.storbinary(f"STOR {renamed}", file)
                else:
                    logDebug(f'Operator: {actual} is a directory')
                is_transferred=True
            if is_transferred:
                ftp.quit()
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlFTPUpload files {replace_map} updated to '+self.remote_path #ActivityMessage
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
                     f'Operator: Failure in AmGlFTPUpload, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

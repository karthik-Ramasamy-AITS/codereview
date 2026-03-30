from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil, ftplib, json
from ftplib import FTP
from datetime import timedelta, datetime

class AmGlFTPCleanup(BaseOperator):

    template_fields = ('conn_id' , 'file_filter', 'remote_path', 'instance_id', 'local_path', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        file_filter = '*',
        remote_path,
        instance_id,
        local_path,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_filter = file_filter
        self.remote_path=remote_path
        self.instance_id = instance_id
        self.local_path=local_path
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        try:
            is_deleted = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.file_filter}, {self.continue_on_failure} and {self.remote_path}')
            list_of_files = os.listdir(self.local_path)
            logInfo(f'Operator : list_of_files : {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
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
                for file in list_of_files:
                    try:
                        ftp.delete(self.remote_path+str(file))
                        is_deleted = True
                    except ftplib.error_perm:
                        logError(f'File - {self.remote_path}{file} not found.')
                        pass
            if is_deleted:
                ftp.quit()
            logDebug(f'Operator: {list_of_files} removed from '+ self.remote_path)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlFTPCleanup - {list_of_files} removed from '+ self.remote_path #ActivityMessage
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
                     f'Operator: Failure in AmGlFTPCleanup, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlFTPCleanup, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while cleanup , error: {e}")
        return status, error

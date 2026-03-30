from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
from datetime import timedelta, datetime
import os, shutil,stat, pysftp, json

class AmGlSFTPDownload(BaseOperator):

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
            connection = SSHHook.get_connection(conn_id=self.conn_id)
            # temp = connection.get_extra()
            # logDebug(f'Operator : Connection Details {temp} and {len(temp)}')
            if connection.get_extra() is not None and len(connection.get_extra()) > 0:
                extra = json.loads(connection.get_extra())
            else:
                extra = json.loads('{}')
            logDebug(f'Operator : Connection extra {extra}')
            use_native = False
            sftp_client = None
            conn = None
            if 'use_native' in extra and extra['use_native'] == 'true':
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None
                conn = pysftp.Connection(host=connection.host,
                                         port=connection.port,
                                         username=connection.login,
                                         password='', 
                                         cnopts=cnopts)
                list_of_files = conn.listdir(self.remote_path)
                use_native = True
                logDebug(f'Operator : going with passwordless authentication')
            else:
                ssh_hook = SSHHook(ssh_conn_id=self.conn_id)
                logDebug(f'Operator : SFTP Server Connection Status {ssh_hook}')
                sftp_client = ssh_hook.get_conn().open_sftp()
                list_of_files = sftp_client.listdir(self.remote_path)
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
                logDebug(f'Operator : file download in progess : {file}')
                if use_native:
                    isdir = conn.isdir(self.remote_path+str(file))
                    if isdir is not True:
                        conn.get(self.remote_path+str(file), path+str(file))
                        is_downloaded = True
                    else:
                        logInfo(f'Operator : {self.remote_path+str(file)}: is Directory')
                else:
                    file_attr = sftp_client.lstat(self.remote_path+str(file))
                    logDebug(f'Operator : checking mode: {file_attr.st_mode}')
                    if stat.S_ISDIR(file_attr.st_mode):
                        logInfo(f'Operator : {file}: is Directory')
                    elif stat.S_ISREG(file_attr.st_mode):
                        sftp_client.get(self.remote_path+str(file), path+str(file))
                        is_downloaded = True
            if is_downloaded:
                if sftp_client is not None:
                    sftp_client.close()
                if conn is not None:
                    conn.close()
            if is_downloaded == False:
                message = f'Operator: AmGlSFTPDownload no files to download'
            else:
                message = f'Operator: AmGlSFTPDownload files {filtered_files} downloaded to '+path    
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
                     f'Operator: Failure in AmGlSFTPDownload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlSFTPDownload, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while downloading , error: {e}")
        return self.local_path, error
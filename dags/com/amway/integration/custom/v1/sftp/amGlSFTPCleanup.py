from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, rename_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, stat, pysftp, json
from datetime import timedelta, datetime
from pathlib import Path

class AmGlSFTPCleanup(BaseOperator):

    template_fields = ('conn_id' , 'regex', 'remote_path', 'instance_id', 'local_path')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        remote_path,
        instance_id,
        local_path,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.remote_path=remote_path
        self.instance_id = instance_id
        self.local_path = local_path            

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        try:
            is_deleted = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex} and {self.remote_path}')
            list_of_files = os.listdir(self.local_path)
            if list_of_files is not None and len(list_of_files) > 0:
                connection = SSHHook.get_connection(conn_id=self.conn_id)
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
                    #list_of_files = conn.listdir(self.remote_path)
                    use_native = True
                    logDebug(f'Operator : going with passwordless authentication')
                else:
                    ssh_hook = SSHHook(ssh_conn_id=self.conn_id)
                    logDebug(f'Operator : SFTP Server Connection Status {ssh_hook}')
                    sftp_client = ssh_hook.get_conn().open_sftp()
                    #list_of_files = sftp_client.listdir(self.remote_path)
                logInfo(f'Operator : list_of_files : {list_of_files}')
                for file in list_of_files:
                    if use_native:
                        isdir = conn.isdir(self.remote_path+str(file))
                        if isdir is not True:
                            if conn.exists(self.remote_path+str(file)):
                                logDebug(f'Operator: {file} removing from '+ self.remote_path)
                                conn.remove(self.remote_path+str(file))
                                is_deleted = True
                            else:
                                logError(f'Operator: file - {self.remote_path+str(file)} not found')
                    else:
                        try:
                            file_attr = sftp_client.lstat(self.remote_path+str(file))
                            logDebug(f'Operator : checking mode: {file_attr.st_mode}')
                            if stat.S_ISDIR(file_attr.st_mode):
                                logInfo(f'Operator : {file}: is Directory')
                            elif stat.S_ISREG(file_attr.st_mode):
                                logDebug(f'Operator: {file} removing from '+ self.remote_path)
                                sftp_client.remove(self.remote_path+str(file))
                                is_deleted = True
                        except FileNotFoundError:
                            logError(f'File - {self.remote_path}{file} not found.')
                            pass
                logDebug(f'Operator: {list_of_files} removed from '+ self.remote_path)
                if is_deleted:
                    if sftp_client is not None:
                        sftp_client.close()
                    if conn is not None:
                        conn.close()
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        '', #sourceApp
                        'ACTIVITY',
                        self.conn_id,#targetApplicationCode
                        f'Operator: AmGlSFTPCleanup - {list_of_files} removed from '+ self.remote_path #ActivityMessage
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
                     f'Operator: Failure in AmGlSFTPCleanup, reason {e}' #ActivityMessage
                     )	
            raise AirflowException(f"exception while cleanup , error: {e}")
        return status, error

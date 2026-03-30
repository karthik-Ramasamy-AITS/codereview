from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateInstanceId, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, shutil, pysftp, json
from datetime import timedelta, datetime
from pathlib import Path

class AmGlSFTPUpload(BaseOperator):

    template_fields = ('ssh_conn_id' , 'regex', 'replacement', 'rename_mask', 'local_path', 'remote_path', 'instance_id', 'transfer_empty_files', 'continue_on_failure')

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
        files = []
        empty_files = []
        try:
            is_uploaded = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.regex}, {self.replacement}, {self.rename_mask}, {self.transfer_empty_files}, {self.continue_on_failure} and {self.local_path}')
            list_of_files=os.listdir(self.local_path)
            logDebug(f'Operator : Local list of files are {list_of_files}')
            if list_of_files is not None and len(list_of_files) > 0:
                if self.instance_id is None:
                    self.instance_id = generateInstanceId()
                replace_map, error = rename_files(list_of_files, self.regex, self.replacement, self.instance_id, self.rename_mask)
                logDebug(f'Operator : replace map {replace_map}')
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
                    use_native = True
                    logDebug(f'Operator : going with passwordless authentication')
                else:
                    ssh_hook = SSHHook(ssh_conn_id=self.conn_id)
                    logDebug(f'Operator :  SFTP server is connected {ssh_hook}')
                    sftp_client = ssh_hook.get_conn().open_sftp()
                    logDebug(f'Operator :  SFTP server is connected {sftp_client}')
                for actual, renamed in replace_map.items():
                    if os.path.isfile(self.local_path+actual):
                        if self.transfer_empty_files is True:
                            logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                            if use_native:
                                conn.put(self.local_path+actual,
                                        self.remote_path+renamed)
                            else:
                                sftp_client.put(self.local_path+actual,
                                                self.remote_path+renamed)
                            is_uploaded = True
                            files.append(renamed)
                        else:
                            if Path(self.local_path+actual).stat().st_size > 0:
                                logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                                if use_native:
                                    conn.put(self.local_path+actual,
                                            self.remote_path+renamed)
                                else:
                                    sftp_client.put(self.local_path+actual,
                                                    self.remote_path+renamed)
                                is_uploaded = True
                                files.append(renamed)
                            else:
                                logInfo(f'skipping {actual} as file-size is 0kb')
                                empty_files.append(renamed)
                    else:
                        logDebug(f'Operator: {actual} is a directory')
            if is_uploaded is True:
                if sftp_client is not None:
                    sftp_client.close()
                if conn is not None:
                    conn.close()
                if self.transfer_empty_files is True:
                    ActivityMessage = f'Operator: AmGlSFTPUpload files {files} uploaded to {self.remote_path}'
                else:
                    ActivityMessage = f'Operator: AmGlSFTPUpload files {files} uploaded to {self.remote_path} and skipped {empty_files} files size is 0kb'
            else:
                ActivityMessage = f'Operator: AmGlSFTPUpload - skipping {empty_files} files size is 0kb'
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
                     f'Operator: Failure in AmGlSFTPUpload, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGlSFTPUpload, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, generateInstanceId, rename_files
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
from datetime import timedelta, datetime
import os, re, stat, json
from pathlib import PurePath
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File

class AmGlSharepointUpload(BaseOperator):

    template_fields = ('conn_id', 'regex', 'rename_mask', 'replacement', 'local_path', 'local_path_exits', 'remote_url', 'team_site_url', 'instance_id')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        replacement = None,
        rename_mask = None,
        local_path,
        team_site_url,
        remote_url,
        instance_id,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.local_path = local_path
        self.replacement = replacement
        self.rename_mask = rename_mask
        self.remote_url = remote_url
        self.team_site_url = team_site_url
        self.instance_id = instance_id
    
    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        replace_map = {}
        try:
            is_uploaded = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.rename_mask}, {self.replacement}, {self.regex}, {self.team_site_url} and {self.local_path}')

            #getting the list from the local path
            list_of_files = os.listdir(self.local_path)
            logDebug(f'Operator : Local list of files are {list_of_files}') 

            if list_of_files is not None and len(list_of_files) > 0:

                #getting connection info
                conn = BaseHook.get_connection(conn_id=self.conn_id)
                if conn.get_extra() is not None and len(conn.get_extra()) > 0:
                    extra = json.loads(conn.get_extra())
                else:
                    extra = json.loads('{}')
                logDebug(f'Operator : Connection extra {extra}')
                
                if self.instance_id is None:
                    self.instance_id = generateInstanceId()

                temp_url = self.remote_url.split("?", 1)[0]
                if not temp_url.endswith("/"):
                    temp_url += "/"

                base_url = self.team_site_url.replace("/:f:/r", "")
                folder_path = temp_url.replace(self.team_site_url, "")

                if 'thumbprint' in extra and 'certificate_path' in extra:
                    #setting auth parameters
                    cert_credentials = {
                        "tenant": conn.host,
                        "client_id": conn.login,
                        "thumbprint": extra['thumbprint'],
                        "cert_path": extra['certificate_path'],
                        "passphrase": conn.password,
                    }
                else:
                    raise AirflowException(f'Missing required fields in connection extra configuration: "thumbprint" and/or "certificate_path"')
                
                #creating connection
                ctx = ClientContext(base_url).with_client_certificate(**cert_credentials)
                
                file_list= {}
                
                #Filtering based on regex
                filtered_files, error = filter_files(list_of_files, self.regex)
                logDebug(f'Operator : filtered files are: {filtered_files}')

                if self.instance_id is None:
                    self.instance_id = generateInstanceId()
                
                #renaming the local files
                replace_map, error = rename_files(filtered_files, self.regex, self.replacement, self.instance_id, self.rename_mask)
                logDebug(f'Operator : replace map {replace_map}')

                #mapping every file with its local path
                for item in filtered_files:
                    item_path = PurePath(self.local_path,item)
                    if os.path.isfile(item_path):
                        file_list[item] = item_path
                
                #uploading each file
                for file_name, file_path in file_list.items():
                    with open(file_path, 'rb') as f:
                        file_data = f.read()
                    
                    if file_name in replace_map:
                        replacement = replace_map[file_name]
                    folder = ctx.web.get_folder_by_server_relative_url(folder_path)
                    response = folder.upload_file(replacement, file_data).execute_query()
                    is_uploaded = True
                    logDebug("Operator: file has been uploaded from: {0}".format(response.serverRelativeUrl))

                if is_uploaded == False:
                    message = f'Operator: AmGISharepointUpload no files to upload'
                    logDebug(message)
                else:
                    message = f'Operator: AmGISharepointUpload files {replace_map} uploaded to ' + self.remote_url 
                    logDebug(message)
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        '', #sourceApp
                        'ACTIVITY',
                        self.conn_id,#targetApplicationCode
                        message #ActivityMessage
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
                     f'Operator: Failure in AmGISharepointUpload, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while uploading , error: {e}")
        return status , error

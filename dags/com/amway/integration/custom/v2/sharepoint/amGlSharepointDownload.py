from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
from datetime import timedelta, datetime
import os, re, stat, json
from pathlib import PurePath
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

class AmGlSharepointDownload(BaseOperator):

    template_fields = ('conn_id', 'file_filter', 'local_path', 'local_path_exits', 'remote_url', 'team_site_url', 'instance_id', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        file_filter = '*',
        local_path='/tmp/',
        local_path_exits = False,
        remote_url,
        team_site_url,
        instance_id,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_filter = file_filter
        self.local_path = local_path
        self.local_path_exits = local_path_exits
        self.remote_url = remote_url
        self.team_site_url = team_site_url
        self.instance_id = instance_id
        self.continue_on_failure = continue_on_failure
    
    def execute(self, context: Any) -> str:
        error = None
        try:
            is_downloaded = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.remote_url}, {self.file_filter}, {self.team_site_url}, {self.continue_on_failure} and {self.local_path}')
            
            #getting connection info
            conn = BaseHook.get_connection(conn_id=self.conn_id)
            if conn.get_extra() is not None and len(conn.get_extra()) > 0:
                extra = json.loads(conn.get_extra())
            else:
                extra = json.loads('{}')
            logDebug(f'Operator : Connection extra {extra}')

            #Spliting the URL as site_url and folder path
            temp_url = self.remote_url.split("?", 1)[0]
            if not temp_url.endswith("/"):
                temp_url += "/"

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
            ctx = ClientContext(self.team_site_url).with_client_certificate(**cert_credentials)
            
            #getting the list from the source location
            root_folder = ctx.web.get_folder_by_server_relative_url(f'{folder_path}')
            root_folder.expand(["Files", "Folders"]).get().execute_query()
            files_list = root_folder.files

            #list of files
            list_of_files = []
            for file in files_list:
                list_of_files.append(file.name)
            logDebug(f'Operator : list_of_files:: {list_of_files}')

            #filtering the files based on file_filter
            filtered_files, error = filter_files(list_of_files, self.file_filter)
            logDebug(f'Operator : filtered files are: {filtered_files}')
            
            #creating local path
            if self.local_path_exits is True:
                path = self.local_path
            else:
                path = self.local_path+os.environ.get('AIRFLOW_CTX_DAG_ID')+'/'+self.instance_id+'/'
                if os.path.exists(path):
                    logDebug(f'Operator : path exists : {path}')
                else:    
                    os.makedirs(path)

            #looping through the list to download each file
            for file in files_list:
                if file.name in filtered_files:
                    file_url=file.properties["ServerRelativeUrl"]
                    file_obj = File.open_binary(ctx, file_url)
                    file_content = file_obj.content
                    file_storage_path = PurePath(path, file.name)
                    with open(file_storage_path, 'wb') as f:
                        f.write(file_content)
                    logDebug("Operator : file has been downloaded from: {0}".format(self.local_path)+file.name)
                    is_downloaded = True

            if is_downloaded == False:
                message = f'Operator: AmGlSharepointDownload no files to download'
            else:
                message = f'Operator: AmGlSharepointDownload files {filtered_files} downloaded to '+ path 
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
                     f'Operator: Failure in AmGISharepointDownload, reason {e}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGISharepointDownload, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while downloading , error: {e}")
        return self.local_path, error

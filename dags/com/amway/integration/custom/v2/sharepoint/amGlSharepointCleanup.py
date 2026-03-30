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
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File

class AmGlSharepointCleanup(BaseOperator):

    template_fields = ('conn_id' , 'remote_path', 'file_filter', 'local_path', 'local_path_exits', 'remote_url', 'team_site_url', 'instance_id', 'continue_on_failure')

    def __init__(
        self,
        *,
        conn_id,
        file_filter = '*',
        local_path,
        instance_id,
        remote_url,
        team_site_url,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_filter = file_filter
        self.local_path = local_path
        self.remote_url = remote_url
        self.team_site_url = team_site_url
        self.instance_id = instance_id
        self.continue_on_failure = continue_on_failure
    
    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        try:
            is_deleted = False
            logDebug(f'Operator : called module with {self.conn_id}, {self.remote_url}, {self.file_filter}, and {self.team_site_url}')

            #Getting the list of Files
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

                #Spliting the URL as site_url and folder path
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

                #looping through the list to delete each file
                for file in list_of_files:
                    item = ctx.web.get_file_by_server_relative_url(f'{folder_path}{file}')
                    #items = items_list.items.get().execute_query()

                    #To move the file to recycle bin
                    item.recycle().execute_query()
                    logDebug("Operator : file has been deleted from: {0}".format(self.local_path)+file)
                    #TO Permanently delete file uncomment the below line
                    #item.delete_object().execute_query()
                    is_deleted = True
                
                if is_deleted == False:
                    message = f'Operator: AmGISharepointCleanup no files to delete'
                else:
                    message = f'Operator: AmGISharepointCleanup files deleted.'
            
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
                     f'Operator: Failure in AmGISharepointCleanup, reason {e}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in AmGISharepointCleanup, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while cleanup , error: {e}")
        return status, error

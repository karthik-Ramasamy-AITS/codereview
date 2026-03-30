from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, rename_files, generateInstanceId
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, shutil, json, requests, mimetypes
from ftplib import FTP
from datetime import timedelta, datetime

class AmGlWmUpload(BaseOperator):

    template_fields = ('conn_id' , 'regex', 'replacement', 'rename_mask', 'local_path', 'request_url','instance_id')

    def __init__(
        self,
        *,
        conn_id,
        regex = '*',
        replacement = None,
        rename_mask = None,
        local_path,
        request_url,
        instance_id,           
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.regex = regex
        self.replacement = replacement
        self.rename_mask = rename_mask
        self.local_path = local_path 
        self.request_url=request_url
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
            http_connection = HttpHook.get_connection(conn_id=self.conn_id)
            url = ''
            if http_connection.schema is None or http_connection.schema == '':
                url = f'https://{http_connection.host}'
            else:
                url = f'{http_connection.schema}://{http_connection.host}'
            if http_connection.port is not None:
                url = f'{url}:{http_connection.port}'
            url = f'{url}{self.request_url}'
            auth = None
            headers = {
                    'Accept' : 'application/json'
                }
            logDebug (f'Operator: Calling endpoint - {url}')
            if http_connection.get_password() is not None:
                auth=(http_connection.login, http_connection.get_password())
            for actual, renamed in replace_map.items():
                logDebug (f'Operator: file Uploading in progess {actual} with file name as {renamed}')
                mt = mimetypes.guess_type(actual)
                files = {renamed: (self.local_path+actual,
                                open(self.local_path+actual, 'rb'),
                                mt[0],
                                {'Expires': '0'})}
                reply = requests.post(url=url, auth=auth, headers=headers, files=files, verify=False)
                logInfo (f'Operator: file upload as {renamed} status code is {reply.status_code} with response as {reply.text}')
                is_transferred=True
                kafka_message = reply.text
                if len(kafka_message) > 1500:
                    kafka_message = kafka_message[:1500]
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        '', #sourceApp
                        'ACTIVITY',
                        self.conn_id,#targetApplicationCode
                        f'Operator: AmGlWmUpload file upload as {renamed} to {self.request_url} response status code is {reply.status_code} with response as {kafka_message}' #ActivityMessage
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
                     f'Operator: Failure in AmGlWmUpload, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while uploading , error: {e}")
        return status, error

from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, generateRandom, StringMask, RegexStringMask
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import json, requests, os

class AmGlHTTPOperator(BaseOperator):

    template_fields = ('conn_id' , 'method', 'request_url', 'data', 'headers', 'instance_id')

    def __init__(
        self,
        *,
        conn_id,
        method = 'GET',
        request_url = '/',
        data = {},
        headers = {'Content-Type' : 'application/json', 'Accept' : 'application/json'},
        instance_id,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.method = method
        self.request_url = request_url
        self.data = data
        self.headers = headers
        self.instance_id = instance_id
    
    def execute(self, context: Any) -> str:
        error = None
        response = None
        try:
            logDebug(f'Operator : called AmGlHTTPOperator module with {self.conn_id}, {self.method}, {self.request_url}, {self.data} and {self.headers}')
            http_connection = HttpHook.get_connection(conn_id=self.conn_id)
            #logDebug (f'Operator: {http_connection.login}, {http_connection.get_password}, {http_connection.host} , {http_connection.schema}, {http_connection.port}')
            if self.data is not None:
                for key in self.data:
                    logDebug (f'Operator: {type(self.data[key]).__name__}')
                    if type(self.data[key]).__name__ == 'dict':
                        if 'rename_mask' in self.data[key]:
                            if '$$#T' in self.data[key]['rename_mask']:
                                s = StringMask(self.data[key]['rename_mask'])
                                self.data[key] = s.apply(self.data[key]['actual'])
                            elif '$$#run_id' in self.data[key]['rename_mask']:
                                self.data[key] = self.data[key]['actual']+os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')
                        if 'regex' in self.data[key]:
                            replace = RegexStringMask(self.data[key]['regex'], self.data[key]['replacement'], os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'))
                            self.data[key] = replace.apply(self.data[key]['actual'])
            url = ''
            if http_connection.schema is None or http_connection.schema == '':
                url = f'https://{http_connection.host}'
            else:
                url = f'{http_connection.schema}://{http_connection.host}'
            if http_connection.port is not None:
                url = f'{url}:{http_connection.port}'
            url = f'{url}{self.request_url}'
            auth = None
            logDebug (f'Operator: Calling endpoint - {url}')
            if http_connection.get_password() is not None:
                auth=(http_connection.login, http_connection.get_password())
            if self.method == 'GET':
                response = requests.get(url, auth=auth, headers=self.headers, timeout=300, verify=False)
            elif self.method == 'DELETE':
                response = requests.delete(url, auth=auth, headers=self.headers, timeout=300, verify=False)
            elif self.method == 'POST':
                data = json.dumps(self.data)
                response = requests.post(url, data, auth=auth, headers=self.headers, timeout=300, verify=False)
            elif self.method == 'PUT':
                data = json.dumps(self.data)
                response = requests.put(url, data, auth=auth, headers=self.headers, timeout=300, verify=False)
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlHTTPOperator Completed execution with request url - {url}, data - {self.data}, response - {response.text} and response status - {response.status_code}' #ActivityMessage
                     )
            logDebug (f'Operator: AmGlHTTPOperator Completed HTTP call for {url}, response is {response.status_code}')
            
        except Exception as e:
            logError(f'Operator : exception while calling AmGlHTTPOperator {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlHTTPOperator, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while calling AmGlHTTPOperator , error: {e}")
        return response, error

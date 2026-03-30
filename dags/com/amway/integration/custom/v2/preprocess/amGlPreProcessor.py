from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom, StringMask, RegexStringMask, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os

class AmGlPreProcessor(BaseOperator):

    template_fields = ('type' , 'filter_prefix', 'filter_pattern', 'filter_suffix','instance_id')

    def __init__(
        self,
        *,
        type = 'FILTER',
        filter_prefix = '',
        filter_pattern = '',
        filter_suffix = '',
        instance_id,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.type = type
        self.filter_prefix = filter_prefix
        self.filter_pattern = filter_pattern
        self.filter_suffix = filter_suffix
        self.instance_id = instance_id
    
    def execute(self, context: Any) -> str:
        error = None
        response = f'{self.filter_prefix}{self.filter_pattern}{self.filter_suffix}'
        try:
            logDebug(f'Operator : called AmGlPreProcessor module with {type}, {self.filter_pattern} and {self.instance_id}')
            if self.type == 'FILTER':
                if '$$#T' in self.filter_pattern:
                    s = StringMask(self.filter_pattern)
                    result = s.apply(self.filter_pattern)
                    result = result.replace(self.filter_pattern, '')
                    response = f'{self.filter_prefix}{result}{self.filter_suffix}'
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.type,#targetApplicationCode
                     f'Operator: AmGlPreProcessor Completed execution with the result - {response}' #ActivityMessage
                     )
            logDebug (f'Operator: AmGlPreProcessor Completed execution, response is {response}')
            
        except Exception as e:
            logError(f'Operator : exception while calling AmGlPreProcessor {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.type,#targetApplicationCode
                     f'Operator: Failure in AmGlPreProcessor, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f"exception while calling AmGlPreProcessor , error: {e}")
        return response, error

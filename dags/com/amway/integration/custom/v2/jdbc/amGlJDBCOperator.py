from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.oracle.operators.oracle import OracleStoredProcedureOperator
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateInstanceId, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os

class AmGlJDBCOperator(BaseOperator):

    template_fields = ('conn_id' , 'sql', 'sql_params', 'instance_id', 'sql_type', 'continue_on_failure')
    

    def __init__(
        self,
        *,
        conn_id,
        sql,
        sql_params = {},
        instance_id,
        sql_type,
        continue_on_failure = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.sql = sql
        self.sql_params = sql_params
        self.instance_id = instance_id
        self.sql_type = sql_type
        self.continue_on_failure = continue_on_failure

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        query_results = []
        try:
            logDebug(f'Operator : Processsing MSSQL with MSSQL DB and Execute Query with {self.sql_params}')
                #self.sql = generate_sql_query(self.sql, self.sql_params)
            if self.sql_params is None and self.sql_type != 'SP':
                logDebug(f'Operator : Skipping formatting query {self.sql}')
                query_results = SQLExecuteQueryOperator(
                    task_id=self.instance_id,
                    conn_id=self.conn_id,
                    sql=self.sql
                ).execute(context)
            elif self.sql_params is not None and self.sql_type != 'SP':
                for key, value in self.sql_params.items():
                    replace_string = '{{ params.'+key+' }}'
                    self.sql = self.sql.replace(replace_string, value)
                logDebug(f'Operator : Formatted query {self.sql}')
                query_results = SQLExecuteQueryOperator(
                    task_id=self.instance_id,
                    conn_id=self.conn_id,
                    sql=self.sql
                ).execute(context)
            else:
                parameters = []
                for key, value in self.sql_params.items():
                    parameters.append(value)
                query_results = SQLExecuteQueryOperator(
                    task_id=self.instance_id,
                    conn_id=self.conn_id,
                    sql=self.sql,
                    parameters=parameters
                ).execute(context)
            logDebug(f'Operator : results {query_results}')
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ACTIVITY',
                     self.conn_id,#targetApplicationCode
                     f'Operator: AmGlJDBCOperator executed query {self.sql}, Successfully' #ActivityMessage
                     )
            #status = 'SUCCESS'
            #status = repr(query_results)
        except Exception as e:
            logError(f'Operator : exception while executing query {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     self.conn_id,#targetApplicationCode
                     f'Operator: Failure in AmGlJDBCOperator, reason {e}, Retry count - {os.environ["AIRFLOW_CTX_TRY_NUMBER"]}' #ActivityMessage
                     )
            status = handleFailures(self.continue_on_failure)
            if status is True:
                logInfo(f'Operator: Failure in executing query, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while executing query , error: {e}")
        return query_results, error

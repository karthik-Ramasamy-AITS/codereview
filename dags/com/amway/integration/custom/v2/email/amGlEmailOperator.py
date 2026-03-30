from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.operators.email import EmailOperator
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateRandom, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os
from pathlib import Path

class AmGlEmailOperator(BaseOperator):
    template_fields = ('instance_id', 'local_path', 'operator_config')

    def __init__(
        self,
        *,
        operator_config,
        instance_id,
        local_path='/tmp/',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.operator_config = operator_config
        self.instance_id = instance_id
        self.local_path = local_path

    def execute(self, context: Any) -> str:
        error = None
        status = 'FAILED'
        logDebug(f'Operator: called module with {self.operator_config}, {self.instance_id}, and {self.local_path}')       
        try:
            list_of_files = os.listdir(f'{self.local_path}')
            logInfo(f'list of files - {list_of_files}')
            transfer_empty_files = self.operator_config['transfer_empty_files']
            if len(list_of_files) > 0:
                attachments = []
                for file in list_of_files:
                    full_file_path = os.path.join(f'{self.local_path}', f'{file}')
                    if transfer_empty_files is True:
                        attachments.append(full_file_path)
                    elif transfer_empty_files is False and Path(full_file_path).stat().st_size > 0:
                        attachments.append(full_file_path)
                status = EmailOperator(
                    task_id=self.instance_id,
                    to=self.operator_config['to'],
                    cc=self.operator_config['cc'],
                    bcc=self.operator_config['bcc'],
                    subject=self.operator_config['subject'],
                    html_content=self.operator_config['html_template'],
                    files=attachments,
                ).execute(context)
                logInfo(f'Operator: Finished sending email')
                logInfo(f'Operator: Email status - {status}')
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        'default', #sourceApp
                        'SOURCE',
                        '',#targetApplicationCode
                        f'Operator: AmGlEmailOperator Finished message production to KafkaTopic' #ActivityMessage
                        )
                status, error
            else:
                logInfo(f'Skipping the Email as no files are available to send')
        except Exception as e:
            logError(f'Operator : exception while Producing {e}')
            error = e
            logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                     os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                     os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                     self.instance_id, #dag_run_id or instance id,
                     '', #sourceApp
                     'ERROR',
                     'default',#targetApplicationCode
                     f'Operator: Failure in AmGlEmailOperator, reason {e}' #ActivityMessage
                     )
            status = handleFailures(self.operator_config['continue_on_failure'])
            if status is True:
                logInfo(f'Operator: Failure in Sending email, reason {e}, as continue_on_failure is True')
            else:
                raise AirflowException(f"exception while Sending email , error: {e}")
        return status, error

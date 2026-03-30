from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, logDebug, logError, filter_files, rename_files, generateRandom
from com.amway.integration.custom.v1.pvf.amGlTranLog import logToPVF
import os, json, csv
from pathlib import Path

def alicloud_producer_function(local_path, operator_config):
    try:
        logDebug(f'Operator_alicloud_producer_function: local_path - {local_path}')
        logDebug(f'Operator_alicloud_producer_function: operator_config - {operator_config}')
        headers = []
        list_of_files = os.listdir(f'{local_path}')
        for file in list_of_files:
            logInfo(f'Operator_alicloud_producer_function: file start processing - {file}')
            full_file_path = os.path.join(f'{local_path}', f'{file}')
            if Path(full_file_path).stat().st_size > 0:
                with open(full_file_path, mode='r', newline=operator_config["newline"]) as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter=operator_config["delimiter"])
                    line_count = 0
                    for row in csv_reader:
                        if line_count == 0:  # row 1 as keys
                            logInfo(f'Operator_alicloud_producer_function: Header Column names are {row}')
                            if len(row) < 2:
                                raise Exception(f'{file} is not a valid CSV')
                            headers.extend(row)
                        else:  # other rows as data to be published to kafka
                            if len(row) < 2:
                                raise Exception(f'{file} is not a valid CSV')
                            data = {}
                            for column_index in range(len(row)):
                                data[headers[column_index]] = row[column_index]
                            logDebug(f'Operator_alicloud_producer_function: Object {line_count} : {json.dumps(data)}')
                            yield (
                                json.dumps(line_count),
                                json.dumps(data),
                            )
                        line_count += 1
                    logInfo(f'Operator_alicloud_producer_function: Processed {line_count} lines.')
                    logInfo(f'Operator_alicloud_producer_function: Publishing message to Kafka')
                    csvfile.close()
            else:
                logInfo(f'Operator_alicloud_producer_function: skipping {file} as file-size is 0kb')
    except Exception as e:
        logError(f'Operator_alicloud_producer_function: Exception while Producing {e}')
        raise AirflowException(f"Exception while Producing, error: {e}")

class AmGlProduceToKafkaTopic(BaseOperator):
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
            logInfo(f'Operator_alicloud_producer_function: list of files - {list_of_files}')
            if len(list_of_files) > 0:
                # Map yield_function_name to the actual function reference
                yield_function = globals().get(self.operator_config['yield_function_name'])
                if yield_function is None:
                    raise AirflowException(f"Yield function {self.operator_config['yield_function_name']} not found.")
                status = ProduceToTopicOperator(
                    task_id=generateRandom(),
                    kafka_config_id=self.operator_config['connection'],
                    topic=self.operator_config['topic'],
                    producer_function=yield_function,
                    producer_function_args=[self.local_path, self.operator_config['custom_args']],
                    poll_timeout=10,
                ).execute(context)
                logInfo(f'Operator: Finished message production')
                logInfo(f'Operator: Kafka Producer status - {status}')
                logToPVF(os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #key
                        os.environ.get('AIRFLOW_CTX_DAG_RUN_ID'), #transactionEventID
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #transactionSourceObject
                        os.environ.get('AIRFLOW_CTX_DAG_ID'), #dag_id
                        self.instance_id, #dag_run_id or instance id,
                        self.operator_config['connection'], #sourceApp
                        'SOURCE',
                        '',#targetApplicationCode
                        f'Operator: AmGlProduceToKafkaTopic Finished message production to KafkaTopic' #ActivityMessage
                        )
                status, error
            else:
                logInfo(f'Skipping the Kafka connection as no files are available to produce data to Kafka Topic')
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
                     self.operator_config['connection'],#targetApplicationCode
                     f'Operator: Failure in AmGlProduceToKafkaTopic, reason {e}' #ActivityMessage
                     )
            raise AirflowException(f'exception while Producing messages to KafkaTopic , error: {e}')
        return status, error

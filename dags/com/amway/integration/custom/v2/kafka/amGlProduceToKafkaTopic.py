from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.variable import Variable
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
import com.amway.integration.custom.v2.kafka.ItemMaster.ABGCDM_ItemMaster_CDM_ItemMasterCDM_pb2 as ABGCDM_ItemMaster_CDM
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateRandom, handleFailures
from com.amway.integration.custom.v2.pvf.amGlTranLog import logToPVF
import os, json, csv, datetime
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

def get_var_config_value(d, key_to_find):
    if isinstance(d, dict):
        for key, value in d.items():
            if key == key_to_find:
                return value
            elif isinstance(value, dict):
                result = get_var_config_value(value, key_to_find)
                if result is not None:
                    return result
            elif isinstance(value, list):
                for item in value:
                    result = get_var_config_value(item, key_to_find)
                    if result is not None:
                        return result
    return None

def get_subject_name(subject: str, ctx: SerializationContext) -> str:
    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
    if not dag_id:
        raise ValueError("AIRFLOW_CTX_DAG_ID environment variable is missing.")

    try:
        data = Variable.get(f"{dag_id}_config", deserialize_json=True)
    except Exception as e:
        raise RuntimeError(f"Failed to load Airflow Variable {dag_id}_config: {e}")

    subject_name = get_var_config_value(data, "subject_name")
    if not subject_name:
        raise KeyError(f"'subject_name' key not found in {dag_id}_config Airflow Variable")
    return subject_name

def get_current_datetime(format_string):
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime(format_string)
    return formatted_datetime

def itemmastercdm_producer_function(local_path, operator_config):
    dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID')
    host = f'Airflow - Node Host - {os.uname()[1]}'
    
    try:
        data_keys = operator_config
        if data_keys is not None:
            itemmaster_topic= operator_config['topic']
            level = data_keys['Level']
            name = data_keys['Name']
            
            try:
                confluent_conf = Variable.get("confluent_conf", deserialize_json=True)
            except KeyError:
                raise RuntimeError("Airflow Variable 'confluent_conf' is not set or available. Please configure it.")

            schema_registry_url = confluent_conf['schema_registry_url']
            schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})

            # Create the serializer
            protobuf_serializer = ProtobufSerializer(ABGCDM_ItemMaster_CDM.ABGCDM_ItemMaster_CDM_ItemMasterCDM,schema_registry_client,{'auto.register.schemas': False, 'use.deprecated.format': False, 'subject.name.strategy': get_subject_name, 'use.latest.version': True})

            list_of_files=os.listdir(local_path)
            expected_field_count = len(data_keys['fields'])

            if list_of_files:
                for file_name in list_of_files:
                    file_path = os.path.join(local_path, file_name)
                    if os.path.isfile(file_path):
                        if Path(file_path).stat().st_size > 0:
                            with open(file_path, "r", encoding="utf-8") as file_obj:
                                for line_number, line in enumerate(file_obj, start=1): #line in lines
                                    # Remove newline and split by |
                                    values = line.strip().split('|')

                                    # Strip trailing empty values if extra
                                    if len(values) > expected_field_count:
                                        values = values[:expected_field_count]
                                    else:
                                        values += [''] * (expected_field_count - len(values)) #pad missing fields

                                    # Map keys to values
                                    row_dict = dict(zip(data_keys['fields'], values))

                                    CurrentTime = get_current_datetime('%H:%M:%S')
                                    CurrentDate = get_current_datetime('%m/%d/%Y')

                                    message_dict = {
                                        "Control": {
                                            "General":{
                                                "FromApplication": data_keys['SourceApplication'],
                                                "CurrentDate": CurrentDate,
                                                "CurrentTime": CurrentTime}
                                        },
                                        "Data": {
                                            "EntityCode": row_dict['EntityCode'],
                                            "ProductItem": row_dict['ProductItem'],
                                            "Description": row_dict['Description'],
                                            "ProductLabelDescription": row_dict['ProductLabelDescription'],
                                            "Description2": row_dict['Description2'],
                                            "Logistics":{
                                                "Codes":{
                                                    "HarmonizedShipCode_HSCD": row_dict['Logistics']
                                                }
                                            },
                                        },
                                        "EventInfo":{
                                            "EventID": row_dict['ProductItem'],
                                            "SourceApplication": data_keys['SourceApplication'],
                                            "EventCode": data_keys['EventCode'],
                                            "PublishTimestamp": f"{CurrentDate} {CurrentTime}",
                                            "AmwayCountryCode": row_dict['EntityCode'],
                                            "SourceIntegrationID": dag_id,
                                            "SourceUser": data_keys['SourceUser'],
                                            "SourceHost": data_keys['SourceHost'],
                                            "SourceIntegrationEnvironment":{
                                                "Name": name,
                                                "Level": level,
                                                "Host": host
                                            }
                                        }
                                    }
                                    
                                    # Construct the Protobuf message from the dict
                                    message_obj = ABGCDM_ItemMaster_CDM.ABGCDM_ItemMaster_CDM_ItemMasterCDM(**message_dict)

                                    # Serialize the message
                                    serialization_ctx = SerializationContext(itemmaster_topic, MessageField.VALUE)
                                    value = protobuf_serializer(message_obj, serialization_ctx)
                                    key = row_dict['ProductItem']
                                    yield (key,
                                        value,
                                        )
                        else:
                            logInfo(f'skipping {file_name} as file-size is 0kb')
                    else:
                        logInfo(f'Operator: {file_name} is a directory')
        else:
            logError(f"itemmastercdm_producer_function: Missing 'custom_args' or None in operator_config:{operator_config}")
            raise ValueError(f"itemmastercdm_producer_function: Missing 'custom_args' or None in operator_config:{operator_config}")
    except Exception as e:
        logError(f'itemmastercdm_producer_function: Failed to Publish to ItemMaster_CDM - reason is {e}')
        error = e
        raise AirflowException(f"itemmastercdm_producer_function: exception while publishing to {operator_config['topic']} , error: {error}")

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

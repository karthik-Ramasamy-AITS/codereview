from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils import timezone
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
import com.amway.integration.custom.v0.Source.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM_pb2 as TransactionSourceEventUDM_pb2
import com.amway.integration.custom.v0.Activity.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM_pb2 as TransactionActivityEventUDM_pb2
from com.amway.integration.custom.v1.AmGlCommonV1 import logInfo, generateRandom
import json, random

default_args = {
    "owner": "GICOE",
    "retries": 0,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=5),
}

def prod_function():
    """Produces `num_treats` messages containing the pet's name, a randomly picked
    pet mood post treat and whether or not it was the last treat in a series."""
    num_treats = 5 
    pet_name = 'YG'
    serializer = StringSerializer('utf_8')
    schema_registry_conf = {'url': 'https://api-dv.amwayglobal.com/integrationschemaregistry-dev'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    SourceEvents = [{
                                    "TransactionEventID":  "080813",
                                    "TransactionSourceObject": "YG",
                                    "TransactionSourceTimestamp": str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]),
                                    "AffilateCode": "000",
                                    "TransactionValues": None,
                                    "TransactionKeys": None
                                }]
    protobuf_serializer = ProtobufSerializer(TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM,
                                                schema_registry_client,
                                                {'use.deprecated.format': False})
    transactionSourceEventUDM = TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM(SourceEvents=SourceEvents,
                                                                    ProcessedService='YG',
                                                                    ProcessedTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessedServer='Composer',
                                                                    ProcessedContextID='1234',
                                                                    SourceApp='ABCD'
                                                                    )
    topic=f"pvf_dv_global_transactionsourceeventudm"
    sc = SerializationContext(topic, MessageField.VALUE)
    value = protobuf_serializer(transactionSourceEventUDM, sc)
    headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionSourceEventUDM"
                } 
    for i in range(num_treats):
        final_treat = False
        pet_mood_post_treat = random.choices(
            ["content", "happy", "zoomy", "bouncy"], weights=[2, 2, 1, 1], k=1
        )[0]
        if i + 1 == num_treats:
            final_treat = True
        yield (
            serializer(str(i)),
            value
            ,
        )

dag = DAG(
    dag_id='KafkaOperator_sample',
    description='This Dag demonstrates Kafka Operator',
    start_date=timezone.parse('2022-12-24 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

@dag.task(task_id="testKafkaOperator")
def testKafkaOperator(objects, **kwargs):
     produce_treats = ProduceToTopicOperator(
          task_id="produce_treats",
          kafka_config_id="confluentcloud_dev",
          topic="pvf_dv_global_transactionsourceeventudm",
          producer_function=prod_function,
          poll_timeout=10,
     ).execute(None)
     print(f'Dag : After Calling Kafka Operator {produce_treats}')
    

start = BashOperator(
    task_id="start",
    bash_command="echo started the dag",
    dag=dag
)

testKafkaOperator(start.output)
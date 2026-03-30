from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
import com.amway.integration.custom.v2.pvf.Source.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM_pb2 as TransactionSourceEventUDM_pb2
import com.amway.integration.custom.v2.pvf.Activity.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM_pb2 as TransactionActivityEventUDM_pb2
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom
from airflow.models.variable import Variable
from datetime import datetime
import os

def pvfYield(key, transactionEventID, transactionSourceObject, dag_id, dag_run_id, sourceApp, type, targetApplicationCode, ActivityMessage, confluent_conf):   
    try:
        schema_registry_conf = {'url': ''}
        schema_registry_conf['url'] = confluent_conf['schema_registry_url']
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        schema_registry_client2 = SchemaRegistryClient(schema_registry_conf)
        if type is 'SOURCE':
            logDebug(f'Generating Source Protobuf Message')
            SourceEvents = [{
                                    "TransactionEventID":  transactionEventID,
                                    "TransactionSourceObject": transactionSourceObject,
                                    "TransactionSourceTimestamp": str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]),
                                    "AffilateCode": "000",
                                    "TransactionValues": [
                                        {
                                            "key" : "IntegrationID",
                                            "value" :  transactionSourceObject
                                        }
                                    ],
                                    "TransactionKeys": [
                                        {
                                            "key" : "ActivityMessage",
                                            "value" :  ActivityMessage
                                        }
                                    ]
                                }]
            protobuf_serializer = ProtobufSerializer(TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM,
                                                schema_registry_client,
                                                {'auto.register.schemas': False, 'use.deprecated.format': False})
            transactionSourceEventUDM = TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM(SourceEvents=SourceEvents,
                                                                    ProcessedService=dag_id,
                                                                    ProcessedTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessedServer=f'Airflow - Node Host - {os.uname()[1]}',
                                                                    ProcessedContextID=dag_run_id,
                                                                    SourceApp=sourceApp
                                                                    )
            topic=confluent_conf['pvf_source_topic']
            sc = SerializationContext(topic, MessageField.VALUE)
            value = protobuf_serializer(transactionSourceEventUDM, sc)
            headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionSourceEventUDM"
            }
            yield (
                key,
                value
                ,
            )
        elif type is 'ERROR':
            logDebug(f'Generating Error Protobuf Message')
            ActivityEvents = [{
                                    "ActivityCode" : "Failure",
                                    "TransactionEventID":  transactionEventID,
                                    "TargetApplicationCode" : targetApplicationCode,
                                    "AffilateCode": "000",
                                    "ActivityMessage": ActivityMessage,
                                    "TransactionValues": [
                                        {
                                            "key" : "IntegrationID",
                                            "value" :  transactionSourceObject
                                        }
                                    ],
                                    "TransactionKeys": [
                                        {
                                            "key" : "ActivityMessage",
                                            "value" :  ActivityMessage
                                        }
                                    ]
                                }]
            protobuf_serializer_error = ProtobufSerializer(TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM,
                                                schema_registry_client2,
                                                {'auto.register.schemas': False, 'use.deprecated.format': False})
            transactionActivityEventUDM = TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM(ActivityEvents=ActivityEvents,
                                                                    ProcessService=dag_id,
                                                                    ProcessTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessServer=f'Airflow - Node Host - {os.uname()[1]}',
                                                                    ContextID=dag_run_id,
                                                                    TrackID=dag_run_id,
                                                                    SourceApp=''
                                                                    )
            topic_error=confluent_conf['pvf_activity_topic']
            sc_error = SerializationContext(topic_error, MessageField.VALUE)
            value = protobuf_serializer_error(transactionActivityEventUDM, sc_error)
            headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionActivityEventUDM"
            }
            yield (
                key,
                value
                ,
            )
        else:
            logDebug(f'Generating Activity Protobuf Message')
            ActivityEvents = [{
                                    "ActivityCode" : "Success",
                                    "TransactionEventID":  transactionEventID,
                                    "TargetApplicationCode" : targetApplicationCode,
                                    "AffilateCode": "000",
                                    "ActivityMessage": ActivityMessage,
                                    "TransactionValues": [
                                        {
                                            "key" : "IntegrationID",
                                            "value" :  transactionSourceObject
                                        }
                                    ],
                                    "TransactionKeys": [
                                        {
                                            "key" : "ActivityMessage",
                                            "value" :  ActivityMessage
                                        }
                                    ]
                                }]
            protobuf_serializer_activity = ProtobufSerializer(TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM,
                                                schema_registry_client2,
                                                {'auto.register.schemas': False, 'use.deprecated.format': False})
            transactionActivityEventUDM = TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM(ActivityEvents=ActivityEvents,
                                                                    ProcessService=dag_id,
                                                                    ProcessTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessServer=f'Airflow - Node Host - {os.uname()[1]}',
                                                                    ContextID=dag_run_id,
                                                                    TrackID=dag_run_id,
                                                                    SourceApp=''
                                                                    )
            topic_activity=confluent_conf['pvf_activity_topic']
            sc_activity = SerializationContext(topic_activity, MessageField.VALUE)
            value = protobuf_serializer_activity(transactionActivityEventUDM, sc_activity)
            headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionActivityEventUDM"
            }
            yield (
                key,
                value
                ,
            )
    except Exception as e:
        logError(f'Failed to Send to PVF - reason is {e}')


def logToPVF(key, transactionEventID, transactionSourceObject, dag_id, dag_run_id, sourceApp, type, targetApplicationCode, ActivityMessage):
    confluent_conf = Variable.get("confluent_conf", deserialize_json=True)
    pvf_source_topic = confluent_conf['pvf_source_topic']
    pvf_activity_topic = confluent_conf['pvf_activity_topic']
    if type is 'SOURCE':
        topic = pvf_source_topic
        logDebug(f'sending to {pvf_source_topic}')
    else:
        topic = pvf_activity_topic
        logDebug(f'sending to {pvf_activity_topic}')
    if len(ActivityMessage) > 200:
        ActivityMessage = ActivityMessage[:200]
    sendToPVF = ProduceToTopicOperator(
          task_id=generateRandom(),
          kafka_config_id=confluent_conf['pvf_connection'],
          topic=topic,
          producer_function=pvfYield,
          producer_function_args = [
              key, transactionEventID, transactionSourceObject, dag_id, dag_run_id, sourceApp, type, targetApplicationCode, ActivityMessage, confluent_conf
          ],
          poll_timeout=10,
     ).execute(None)
    logDebug(f'sent to PVF {sendToPVF}')
import os
from pathlib import Path
from typing import Any
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.variable import Variable
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from .schema_registry_client import SchemaRegistryClient
from .protobuf import ProtobufSerializer
from confluent_kafka import Producer
import com.amway.integration.custom.Source.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM_pb2 as TransactionSourceEventUDM_pb2
import com.amway.integration.custom.Activity.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM_pb2 as TransactionActivityEventUDM_pb2

connection_config = Variable.get("KAFKA_PRODUCER_CONFIG", deserialize_json=True)
common_config = Variable.get("KAFKA_COMMON_CONFIG", deserialize_json=True)
def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

class AmGlTrnLogOperator(BaseOperator):

    template_fields = ('key','TransactionEventID', 'TransactionSourceObject', 
                       'TransactionSourceTimestamp', 'AffilateCode', 
                       'TransactionValues', 'TransactionKeys',
                       'ProcessedService', 'ProcessedTimestamp',
                       'ProcessedServer', 'ProcessedContextID',
                       'SourceApp', 'type', 'TargetApplicationCode',
                       'ActivityMessage'
                       )

    def __init__(
        self,
        *,
        key=None,
        TransactionEventID=None,
        TransactionSourceObject=None,
        TransactionSourceTimestamp=None,
        AffilateCode=None,
        TransactionValues=None,
        TransactionKeys=None,
        ProcessedService=None,
        ProcessedTimestamp=None,
        ProcessedServer=None,
        ProcessedContextID=None,
        SourceApp=None,
        type=None,
        TargetApplicationCode=None,
        ActivityMessage=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.key = key
        self.TransactionEventID = TransactionEventID
        self.TransactionSourceObject = TransactionSourceObject
        self.TransactionSourceTimestamp = TransactionSourceTimestamp
        self.AffilateCode = AffilateCode
        self.TransactionValues = TransactionValues
        self.TransactionKeys = TransactionKeys
        self.ProcessedService=ProcessedService
        self.ProcessedTimestamp=ProcessedTimestamp
        self.ProcessedServer=ProcessedServer
        self.ProcessedContextID=ProcessedContextID
        self.SourceApp=SourceApp
        self.type=type
        self.TargetApplicationCode=TargetApplicationCode
        self.ActivityMessage=ActivityMessage
    
    def execute(self, context: Any) -> str:
        status = 'FAILED'
        try:
            env = common_config['env']
            status = 'SUCCESS'
            if self.TransactionSourceTimestamp is None:
                self.TransactionSourceTimestamp = str(datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
            if self.ProcessedTimestamp is None:
                self.ProcessedTimestamp = str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z'
            producer = Producer(connection_config)
            serializer = StringSerializer('utf_8')
            schema_registry_conf = {'url': f"{common_config['schema_registry_conf']}"}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)    
            if self.type is 'SOURCE':    
                SourceEvents = [{
                                    "TransactionEventID":  self.TransactionEventID,
                                    "TransactionSourceObject": self.TransactionSourceObject,
                                    "TransactionSourceTimestamp": self.TransactionSourceTimestamp,
                                    "AffilateCode": self.AffilateCode,
                                    "TransactionValues": self.TransactionValues,
                                    "TransactionKeys": self.TransactionKeys
                                }]
                protobuf_serializer = ProtobufSerializer(TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM,
                                                schema_registry_client,
                                                {'use.deprecated.format': False})
                transactionSourceEventUDM = TransactionSourceEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionSourceEventUDM(SourceEvents=SourceEvents,
                                                                    ProcessedService=self.ProcessedService,
                                                                    ProcessedTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessedServer=self.ProcessedServer,
                                                                    ProcessedContextID=self.ProcessedContextID,
                                                                    SourceApp=self.SourceApp
                                                                    )
                topic=f"pvf_{env}_global_transactionsourceeventudm"
                sc = SerializationContext(topic, MessageField.VALUE)
                value = protobuf_serializer(transactionSourceEventUDM, sc)
                headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionSourceEventUDM"
                }
                
            else:
                ActivityEvents = [{
                                    "ActivityCode" : "Success",
                                    "TransactionEventID":  self.TransactionEventID,
                                    "TargetApplicationCode": self.TargetApplicationCode,
                                    "AffilateCode": self.AffilateCode,
                                    "ActivityMessage": self.ActivityMessage,
                                    "TransactionValues": self.TransactionValues,
                                    "TransactionKeys": self.TransactionKeys
                                }]
                protobuf_serializer = ProtobufSerializer(TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM,
                                                schema_registry_client,
                                                {'use.deprecated.format': False})
                transactionActivityEventUDM = TransactionActivityEventUDM_pb2.AmGlTrnLog_DocTypes_Log_TransactionActivityEventUDM(ActivityEvents=ActivityEvents,
                                                                    ProcessService=self.ProcessedService,
                                                                    ProcessTimestamp=str(datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])+'Z',
                                                                    ProcessServer=self.ProcessedServer,
                                                                    ContextID=self.ProcessedContextID,
                                                                    TrackID=self.TransactionEventID,
                                                                    SourceApp=self.SourceApp
                                                                    )
                topic=f"pvf_{env}_global_transactionactivityeventudm"
                sc = SerializationContext(topic, MessageField.VALUE)
                value = protobuf_serializer(transactionActivityEventUDM, sc)
                headers = {
                    "documentType": "AmGlTrnLog.DocTypes.Log:TransactionActivityEventUDM"
                }
                
            producer.produce(topic=topic, 
                                key=serializer(self.key),
                                value=value,
                                headers=headers,
                                on_delivery=delivery_report)
            producer.flush()
        except Exception as e:
            print('Failed to publish to kafka')
            status = str(e)
            raise AirflowException(f"Error while publishing event to Kafka , error: {status}")

        return status

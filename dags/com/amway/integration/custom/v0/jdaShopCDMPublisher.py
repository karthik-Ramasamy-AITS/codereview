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
import com.amway.integration.custom.abgcdm.purchaseorderrequestcdm.ABGCDM_PurchaseOrderRequest_CDM_PurchaseOrderRequestCDM_pb2 as PurchaseOrderRequestCDM_pb2
import com.amway.integration.custom.abgcdm.recommendedorderrequestcdm.ABGCDM_RecommendedOrder_CDM_RecommendedOrderRequestCDM_pb2 as RecommendedOrderRequestCDM_pb2

jdashop_env = Variable.get("Jdashop_env", deserialize_json=False)
connection_config = Variable.get(f"Jdashop_producer_config_{jdashop_env}", deserialize_json=True)

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
    
def getSchemaRegistryURL(env):
    schema_registry_conf = f'https://api-env.amwayglobal.com/integrationschemaregistry/'
    if env == 'dv':
        schema_registry_conf = schema_registry_conf.replace("env", env)
        schema_registry_conf = schema_registry_conf+'dev'
    elif env == 'ts':
        schema_registry_conf = schema_registry_conf.replace("env", env)
        schema_registry_conf = schema_registry_conf+'dev'        
    elif env == 'qa':
        schema_registry_conf = schema_registry_conf.replace("env", env)
        schema_registry_conf = schema_registry_conf+env        
    elif env == 'perf':
        schema_registry_conf = schema_registry_conf.replace("env", env)
        schema_registry_conf = schema_registry_conf+env
    else:
        schema_registry_conf = 'https://api.amwayglobal.com/integrationschemaregistry'
    return schema_registry_conf

class JdaShopCDMPublisher(BaseOperator):

    template_fields = ('RICE_ID','BATCH_ID', 'LOAD_ID', 
                       'RECORD_ID', 'SOURCE_LOC', 
                       'DEST_LOC', 'ADA_ITEM_NUMBER',
                       'QUANTITY', 'NEED_BY_DATE',
                       'HEADER_ATTRIBUTE1', 'VENDOR_NAME',
                       'LINE_ATTRIBUTE3', 'CDM'
                       )

    def __init__(
        self,
        *,
        RICE_ID=None,
        BATCH_ID=None,
        LOAD_ID=None,
        RECORD_ID=None,
        SOURCE_LOC=None,
        DEST_LOC=None,
        ADA_ITEM_NUMBER=None,
        QUANTITY=None,
        NEED_BY_DATE=None,
        HEADER_ATTRIBUTE1=None,
        VENDOR_NAME=None,
        LINE_ATTRIBUTE3=None,
        CDM=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.RICE_ID = RICE_ID
        self.BATCH_ID = BATCH_ID
        self.LOAD_ID = LOAD_ID
        self.RECORD_ID = RECORD_ID
        self.SOURCE_LOC = SOURCE_LOC
        self.DEST_LOC = DEST_LOC
        self.ADA_ITEM_NUMBER = ADA_ITEM_NUMBER
        self.QUANTITY=QUANTITY
        self.NEED_BY_DATE=NEED_BY_DATE
        self.HEADER_ATTRIBUTE1=HEADER_ATTRIBUTE1
        self.VENDOR_NAME=VENDOR_NAME
        self.LINE_ATTRIBUTE3=LINE_ATTRIBUTE3
        self.CDM=CDM
    
    def execute(self, context: Any) -> str:
        status = 'FAILED'
        try:
            print(f'jdashop_env is {jdashop_env}')
            print(f'CDM == {self.CDM}')
            producer = Producer(connection_config)
            serializer = StringSerializer('utf_8')
            schema_registry_url =  getSchemaRegistryURL(jdashop_env)
            print(f'schema_registry_url is {schema_registry_url}')
            schema_registry_conf = {'url': f"{schema_registry_url}"}
            print(f'schema_registry_conf - {schema_registry_conf}')
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            if self.CDM == 'ABGCDM.PurchaseOrderRequest.CDM:PurchaseOrderRequestCDM':
                print(f'ABGCDM.PurchaseOrderRequest.CDM:PurchaseOrderRequestCDM')
                topicName = f'jdashop_{jdashop_env}_global_purchaseorderrequestcdm_pub_v1'
                protobuf_serializer = ProtobufSerializer(PurchaseOrderRequestCDM_pb2.ABGCDM_PurchaseOrderRequest_CDM_PurchaseOrderRequestCDM,
                                                schema_registry_client,
                                                {
                                                    'use.deprecated.format': False
                                                 })
                print(f'protobuf_serializer')
                purchaseOrderRequestCDM = PurchaseOrderRequestCDM_pb2.ABGCDM_PurchaseOrderRequest_CDM_PurchaseOrderRequestCDM(
                    Control={
                        "General" : {
                            "CanonicalVersion" : "POREQUISITION",
                            "FromApplication" : "JDAShop",
                            "ToApplication"   : "OEBS"
                        },
                        "InterEnterprise" : {
                            "SenderId" : "JDAShop",
                            "ReceiverId" : "I1850"
                        },
                        "ConversationProcessing" : {}
                    },
                    Data={
                        "Request" : {
                            "BatchID" : f"{self.BATCH_ID}",
                            "RequestID" : f"{self.LOAD_ID}"
                        },
                        "Detail" : [{
                            "RequestedDeliveryDate" : f"{self.NEED_BY_DATE}",
                            "Product" : {
                                "ProductItem" : f"{self.ADA_ITEM_NUMBER}"
                            },
                            "Quantity" : {
                                "TransactionQuantityOrdered" : f"{self.QUANTITY}"
                            },
                            "ShipFrom" : {
                                "ShipFrom" : {
                                    "key" : f"{self.SOURCE_LOC}"
                                }
                            },
                            "ShipTo" : {
                                "ShipToKey" : {
                                    "key" : f"{self.DEST_LOC}",
                                    "descriptive" : f"{self.VENDOR_NAME}"
                                }
                            }
                        }]
                    },
                    EventInfo={
                        "EventID" : f"{self.RECORD_ID}",
                        "EventCode" : f"{self.RICE_ID}",
                        "EventDescription" : "PORequest From JDAShop To OEBS",
                        "AmwayAffililateCode" : f"{self.HEADER_ATTRIBUTE1}",
                        "SourceApplication" : "JDAShop",
                        "SourceIntegrationID" : "JDAShop"
                    }
                )
                sc = SerializationContext(topicName, MessageField.VALUE, 'ABGCDM.PurchaseOrderRequest.CDM:PurchaseOrderRequestCDM')
                print(f'SerializationContext')
                value = protobuf_serializer(purchaseOrderRequestCDM, sc)
                print(f'Value Serialized')
                headers = {
                    "documentType": "ABGCDM.PurchaseOrderRequest.CDM:PurchaseOrderRequestCDM"
                }
                producer.produce(topic=topicName, 
                                        key=serializer(self.RECORD_ID),
                                        value=value,
                                        headers=headers,
                                        on_delivery=delivery_report)
                print(f'Message Produced')
                producer.flush()
                print(f'Message Flushed')
            elif self.CDM == 'ABGCDM.RecommendedOrder.CDM:RecommendedOrderRequestCDM':
                print(f'ABGCDM.RecommendedOrder.CDM:RecommendedOrderRequestCDM')
                topicName = f'jdashop_{jdashop_env}_global_recommendedorderrequestcdm_pub_v1'
                protobuf_serializer = ProtobufSerializer(RecommendedOrderRequestCDM_pb2.ABGCDM_RecommendedOrder_CDM_RecommendedOrderRequestCDM,
                                                schema_registry_client,
                                                {
                                                    'use.deprecated.format': False
                                                 })
                print(f'protobuf_serializer')
                recommendedOrderRequestCDM = RecommendedOrderRequestCDM_pb2.ABGCDM_RecommendedOrder_CDM_RecommendedOrderRequestCDM(
                    Control={
                        "General" : {
                            "CanonicalVersion" : "POREQUISITION",
                            "FromApplication" : "JDAShop",
                            "ToApplication"   : "OEBS"
                        },
                        "InterEnterprise" : {
                            "SenderId" : "JDAShop",
                            "ReceiverId" : "I1850"
                        },
                        "ConversationProcessing" : {}
                    },
                    Data={
                        "Request" : {
                            "BatchID" : f"{self.BATCH_ID}",
                            "Reference" : f"{self.LOAD_ID}"
                        },
                        "Detail" : [{
                            "RequestedDeliveryDate" : f"{self.NEED_BY_DATE}",
                            "RequestedShipDate" : f"{self.LINE_ATTRIBUTE3}",
                            "Product" : {
                                "ProductItem" : f"{self.ADA_ITEM_NUMBER}"
                            },
                            "Quantity" : {
                                "TransactionQuantityOrdered" : f"{self.QUANTITY}"
                            },
                            "ShipFrom" : {
                                "ShipFrom" : {
                                    "key" : f"{self.SOURCE_LOC}"
                                }
                            },
                            "ShipTo" : {
                                "ShipToKey" : {
                                    "key" : f"{self.DEST_LOC}",
                                    "descriptive" : f"{self.VENDOR_NAME}"
                                }
                            }
                        }]
                    },
                    EventInfo={
                        "EventID" : f"{self.RECORD_ID}",
                        "EventCode" : f"{self.RICE_ID}",
                        "EventDescription" : "PORequest From JDAShop To OEBS",
                        "AmwayAffililateCode" : f"{self.HEADER_ATTRIBUTE1}",
                        "SourceApplication" : "JDAShop",
                        "SourceIntegrationID" : "JDAShop"
                    }
                )
                sc = SerializationContext(topicName, MessageField.VALUE, 'ABGCDM.RecommendedOrder.CDM:RecommendedOrderRequestCDM')
                print(f'SerializationContext')
                value = protobuf_serializer(recommendedOrderRequestCDM, sc)
                print(f'Value Serialized')
                headers = {
                    "documentType": "ABGCDM.RecommendedOrder.CDM:RecommendedOrderRequestCDM"
                }
                producer.produce(topic=topicName, 
                                        key=serializer(self.RECORD_ID),
                                        value=value,
                                        headers=headers,
                                        on_delivery=delivery_report)
                print(f'Message Produced')
                producer.flush()
                print(f'Message Flushed')
            else:
                print(f'Not matching the CDM')                         
            status = 'SUCCESS'
        except Exception as e:
            print('Failed to publish to kafka')
            status = str(e)
            raise AirflowException(f"Error while publishing event to Kafka , error: {status}")

        return status
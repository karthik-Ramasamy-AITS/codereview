from airflow.models import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.hooks.http import HttpHook
from prometheus_client.parser import text_string_to_metric_families
from airflow.utils import timezone
from datetime import timedelta,datetime
from airflow.models.variable import Variable
from airflow.decorators import task, task_group
import json, pymsteams, time, requests
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, generateRandom, generateInstanceId

default_args = {
    "owner": "GICOE",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

#dag properties
dag = DAG(
    dag_id='confluent_cloud_alerts',
    description='Airflow Dag to send Alerts when Consumer lag is detected',
    start_date=timezone.parse('2023-06-27 00:00'),
    max_active_runs=1,
    default_args=default_args,
    schedule_interval='@hourly',
    tags=['confluent', 'monitoring', 'webhook'],
    catchup=False
)

def sendAlert(metric,lag_threshold,webhook):
    myTeamsMessage = pymsteams.connectorcard(webhook)
    myTeamsMessage.color("#d90404")
    # create the section
    myMessageSection = pymsteams.cardsection()
    myMessageSection.activityTitle("Kafka Consumer Alerts")

    # Add Facts
    myMessageSection.addFact("Topic", metric.labels['topic'])
    myMessageSection.addFact("Consumer Group Id", metric.labels['consumer_group_id'])
    myMessageSection.addFact("Consumer Lag", metric.value)
    myMessageSection.addFact("Timestamp", str(datetime.fromtimestamp(metric.timestamp)))
    # Section Text
    myMessageSection.text(f"Consumer lag has exceeded {lag_threshold}")

    myTeamsMessage.addSection(myMessageSection)
    myTeamsMessage.summary("This is a summary")
    # send the message
    myTeamsMessage.send()

def checkCondition(config,topic,lag):  
    ignore=False
    check=False
    lag_threshold = 500
    if topic in config:
        conditions = config[topic]
        if "ignore" in conditions:
            ignore = conditions['ignore']
        if "lag_threshold" in conditions:
            lag_threshold = conditions['lag_threshold']
            
    #only send if lag passed threshold and ignore is not turned on
    if lag > lag_threshold and ignore==False:
        check = True
        print(f"********{topic} condition has been met with lag_threshold: {lag_threshold}, ignore={ignore}, sending alert...***********")
    return {"check":check,"lag_threshold":lag_threshold}

 
@dag.task(task_id="init")
def init(objects, **context):
     iconfig = Variable.get("confluent_cloud_alerts_config", deserialize_json=True)
     logDebug(f'Dag : Upload status {iconfig} ')
     iconfig['instance_id'] = generateInstanceId()
     return iconfig

@dag.task(task_id="getMetrics")
def getMetrics(config):
    #get variables
    endpoint = config['url']
    kafka_id = config['kafka_id']
    connection = config['connection']
    #get connections
    httpRequest = HttpHook("GET",connection)
    connection = HttpHook.get_connection(connection)
    username = connection.login
    password = connection.get_password()
    httpRequest = httpRequest.get_conn()
    #request for metrics
    metrics = httpRequest.get(f"{endpoint}{kafka_id}", auth = (username, password)).text
    return metrics

@dag.task(task_id="processMetrics")
def processMetrics(metrics, config):
    snow_webhook = config['snow_webhook']
    snow_webhook_connection = config['snow_webhook_connection']
    #get connections
    connection = HttpHook.get_connection(snow_webhook_connection)
    username = connection.login
    password = connection.get_password()
    for family in text_string_to_metric_families(metrics):
        for metric in family.samples:
            if "consumer_lag_offsets" in metric.name:
                result = checkCondition(config["conditions"],metric.labels['topic'],metric.value)
                if result["check"]:
                    sendAlert(metric,result['lag_threshold'],config['teams_webhook'])
                    incident = {
                        'resource' : config['resource_name'],
                        'resource_name' : config['resource_name'],
                        'started_at' : int(time.time()),
                        'state': 'open',
                        'summary': f'Consumer Group Id: {metric.labels["consumer_group_id"]} has exceeded Consumer lag as {result["lag_threshold"]} for Topic {metric.labels['topic']}',
                        'resource_type_display_name': config['resource_name'],
                        'resource_display_name' : config['resource_name'],
                        'url': f'{config["resource_name"]} -> {metric.labels["topic"]} -> {metric.labels["consumer_group_id"]}',
                        'condition_name': 'Consumer lag Exceeded',
                        'threshold_value': '500',
                        'observed_value': f'{result["lag_threshold"]}',
                        'displayName': 'Consumer lag',
                        'metric' : {
                            'type': 'consumer lag', 'displayName': 'Consumer Lag'
                        },
                        'condition' : ''
                    }
                    payload = {}
                    payload['incident'] = incident
                    data = json.dumps(payload)
                    headers = {'Content-Type' : 'application/json', 'Accept' : 'application/json'}
                    auth = (username, password)
                    # response = httpRequest.post(url=snow_webhook, data=data, headers=headers, auth = (username, password)).text
                    response = requests.post(url=snow_webhook,data=data,auth=auth,headers=headers)
                    logInfo(response.text)

vars = init(None)
metrics = getMetrics(vars)
processMetrics(metrics, vars)
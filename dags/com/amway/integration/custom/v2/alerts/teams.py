from com.amway.integration.custom.v2.AmGlCommon import logError, generateInstanceId
import traceback, pymsteams
from airflow.models.variable import Variable
from airflow.operators.email import EmailOperator

alert_config = Variable.get("amgl_alert_config", deserialize_json=True)
teams_webhook_url = alert_config['teams_webhook_url']
teams_channel_email_address = alert_config['teams_channel_email_address']
delivery_type = alert_config['delivery_type']

def failure_callback(context):
    try:
        ti = context.get("task_instance")
        dag_id = ti.dag_id if ti else 'Unknown DAG'
        task_id = ti.task_id if ti else 'Unknown Task'
        log_url = getattr(ti, 'log_url', 'Unknown')
        exception = context.get('exception')
        formatted_exception = ''.join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        ).strip()
        if delivery_type != 'email':
            # Teams alert logic
            myTeamsMessage = pymsteams.connectorcard(teams_webhook_url)
            myTeamsMessage.color("#d90404")
            myMessageSection = pymsteams.cardsection()
            myMessageSection.activityTitle("Airflow/Composer DAG Alerts")
            myMessageSection.addFact("DAG", dag_id)
            myMessageSection.addFact("Task", task_id)
            myMessageSection.addFact("Execution Time", str(context.get('logical_date')))
            myMessageSection.addFact("Exception", formatted_exception)
            myMessageSection.text("DAG scheduled execution failed")
            myTeamsMessage.addLinkButton("Click Here for More Details", log_url)
            myTeamsMessage.addSection(myMessageSection)
            myTeamsMessage.summary("DAG scheduled execution failed")
            myTeamsMessage.send()
        else:
            # Email alert logic
            html_template = f"""<h3>Airflow/Composer DAG Alerts | {dag_id} | {task_id}</h3>
                 <h3>Execution Time : {str(context.get('logical_date'))} </h3>
                 <p>Exception: {formatted_exception}</p>
                 <p>Click Here for More Details: <a href="{log_url}">{log_url}</a></p>
                 <p>This is an automated message sent from Airflow with a file attachment.</p>
                 <p>Regards,<br>Airflow Bot</p>
            """
            status = EmailOperator(
                task_id=generateInstanceId(),
                to=teams_channel_email_address,
                subject=f"Airflow/Composer DAG Alerts | {dag_id} | {task_id}",
                html_content=html_template,
            ).execute(context)
            logError(f'Teams Alert Status {status}')
    except Exception as err:
        logError(f'Error in failure_callback: {err}')

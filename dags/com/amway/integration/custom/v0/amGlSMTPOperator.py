import smtplib
from smtplib import SMTPException
from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.variable import Variable

smtp_config = Variable.get("SMTP_CONFIG", deserialize_json=True)


class AmGlSMTPOperator(BaseOperator):

    template_fields = ('sender' , 'receivers', 'message')

    def __init__(
        self,
        *,
        sender=None,
        receivers=None,
        message=None,                
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sender = sender
        self.receivers = receivers
        self.message = message
    
    def execute(self, context: Any) -> str:
        status = 'FAILED'
        try:
            print('Initiating Email Confiurations')
            smtp_host = smtp_config["smtp_host"]
            smtp_port = smtp_config["smtp_port"]
            if self.sender is None:
                from_address = smtp_config["from_address"]
            else:
                from_address = self.sender
            smtpObj = smtplib.SMTP(smtp_host, int(smtp_port))
            smtpObj.sendmail(from_address, self.receivers, self.message)         
            print("Successfully sent email")
            status = 'SUCCESS'
        except Exception as e:
            print('Failed to publish to kafka')
            status = str(e)
            raise AirflowException(f"Error while publishing event to Kafka , error: {status}")

        return status

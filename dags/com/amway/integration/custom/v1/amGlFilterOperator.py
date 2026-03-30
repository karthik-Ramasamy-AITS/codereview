from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from com.amway.integration.custom.v1.AmGlCommonV1 import filter_files

class AmGlFilterOperator(BaseOperator):

    template_fields = ('list_of_files' , 'regex')

    def __init__(
        self,
        *,
        list_of_files=None,
        regex=None,            
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.list_of_files = list_of_files
        self.regex = regex
    
    def execute(self, context: Any) -> str:
        return filter_files(self.list_of_files, self.regex)

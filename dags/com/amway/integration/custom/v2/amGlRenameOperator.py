from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from com.amway.integration.custom.v2.AmGlCommon import rename_files

class AmGlRenameOperator(BaseOperator):

    template_fields = ('list_of_files' , 'regex', 'replacement', 'instance_id', 'rename_mask')

    def __init__(
        self,
        *,
        list_of_files=None,
        regex=None,
        replacement=None,
        instance_id=None,
        rename_mask=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.list_of_files = list_of_files
        self.regex = regex
        self.replacement = replacement
        self.instance_id = instance_id
        self.rename_mask = rename_mask

    def execute(self, context: Any) -> str:
        return rename_files(self.list_of_files, self.regex, self.replacement, self.instance_id, self.rename_mask)


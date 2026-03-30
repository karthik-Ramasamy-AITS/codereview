from typing import Any
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.samba.hooks.samba import SambaHook
from com.amway.integration.custom.v2.AmGlCommon import logInfo, logDebug, logError, filter_files, rename_files, generateRandom
from com.amway.integration.custom.v2.sharepoint.amGlSharepointCleanup import AmGlSharepointCleanup
from com.amway.integration.custom.v2.samba.amGlSambaCleanup import AmGlSambaCleanup
from com.amway.integration.custom.v2.sftp.amGlSFTPCleanup import AmGlSFTPCleanup
from com.amway.integration.custom.v2.ftp.amGlFTPCleanup import AmGlFTPCleanup
from com.amway.integration.custom.v2.gcs.amGlGCSCleanup import AmGlGCSCleanup
from com.amway.integration.custom.v2.s3.amGlS3Cleanup import AmGlS3Cleanup

class AmGlCleanup(BaseOperator):

    template_fields = ('conn_id', 'file_filter', 'remote_path', 'type', 'delete_sources', 'instance_id', 'bucket_name', 'local_path', 'continue_on_failure', 'remote_url', 'team_site_url')

    def __init__(
        self,
        *,
        conn_id,
        file_filter = '*',
        remote_path,
        type,
        delete_sources=False,
        instance_id,
        bucket_name = None,
        local_path,
        continue_on_failure = False,
        remote_url = None,
        team_site_url = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_filter = file_filter
        self.remote_path=remote_path
        self.type = type
        self.delete_sources = delete_sources
        self.instance_id = instance_id
        self.bucket_name = bucket_name
        self.local_path = local_path
        self.continue_on_failure = continue_on_failure
        self.remote_url = remote_url
        self.team_site_url = team_site_url

    def execute(self, context: Any) -> str:
        logDebug(f'Operator : called module with {self.conn_id}, {self.file_filter}, {self.type}, {self.delete_sources}, {self.remote_url}, {self.team_site_url} and {self.remote_path}')
        if self.delete_sources is True and self.type == 'samba':
            logDebug(f'Operator : Cleaning up Samba')
            return AmGlSambaCleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         remote_path=self.remote_path,
                         instance_id=self.instance_id,
                         local_path=self.local_path,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        elif self.delete_sources is True and self.type == 'sftp':
            logDebug(f'Operator : Cleaning up SFTP')
            return AmGlSFTPCleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         remote_path=self.remote_path,
                         instance_id=self.instance_id,
                         local_path=self.local_path,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        elif self.delete_sources is True and self.type == 'ftp':
            logDebug(f'Operator : Cleaning up FTP')
            return AmGlFTPCleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         remote_path=self.remote_path,
                         instance_id=self.instance_id,
                         local_path=self.local_path,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        elif self.delete_sources is True and self.type == 'gcs':
            logDebug(f'Operator : Cleaning up GCS')
            return AmGlGCSCleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         remote_path=self.remote_path,
                         instance_id=self.instance_id,
                         bucket_name = self.bucket_name,
                         local_path=self.local_path,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        elif self.delete_sources is True and self.type == 's3':
            logDebug(f'Operator : Cleaning up S3')
            return AmGlS3Cleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         remote_path=self.remote_path,
                         instance_id=self.instance_id,
                         bucket_name=self.bucket_name,
                         local_path=self.local_path,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        elif self.delete_sources is True and self.type == 'sharepoint':
            logDebug(f'Operator : Cleaning up sharepoint')
            return AmGlSharepointCleanup(
                         task_id = generateRandom(),
                         conn_id=self.conn_id,
                         file_filter=self.file_filter,
                         local_path=self.local_path,
                         instance_id=self.instance_id,
                         remote_url=self.remote_url,
                         team_site_url=self.team_site_url,
                         continue_on_failure=self.continue_on_failure
                    ).execute(None)
        else:
            return None, None

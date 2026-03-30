import os
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

from airflow.providers.ssh.hooks.ssh import SSHHook


class SFTPGetMultipleFilesOperator(BaseOperator):

    template_fields = ('local_directory', 'remote_filename_pattern', 'remote_host', 'remote_dir')

    def __init__(
        self,
        *,
        ssh_hook=None,
        ssh_conn_id=None,
        remote_host=None,
        local_directory=None,
        remote_filename_pattern=None,
        remote_dir=None,
        filetype=None,
        confirm=True,
        create_intermediate_dirs=False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_directory = local_directory
        self.filetype = filetype
        self.remote_filename_pattern = remote_filename_pattern
        self.remote_dir = remote_dir
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs

    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()
                self.log.info(f'remote dir {self.remote_dir}')
                #sftp_client.chdir(self.remote_dir)
                #sftp_client.listdir
                all_files = sftp_client.listdir(self.remote_dir)
                for i in all_files:
                    self.log.info(f'file or directory name {i}')
                self.log.info(f'Found {len(all_files)} files on server')
                matching_files = all_files
                filename_pattern = '*.*'
                if self.remote_filename_pattern is not None:
                    filename_pattern = self.remote_filename_pattern
                    matching_files = [f for f in all_files
                                  if f.find(filename_pattern) != -1]
                # if file type is specified filter matching files for the file type
                if self.filetype is not None:
                    matching_files = [filename for filename in matching_files
                                      if filename[-len(self.filetype):] == self.filetype]
                self.log.info(f'Found {len(matching_files)} files with name including {filename_pattern}')

                local_folder = os.path.dirname(self.local_directory)
                if self.create_intermediate_dirs:
                    Path(local_folder).mkdir(parents=True, exist_ok=True)

                for f in matching_files:
                    self.log.info(f"Starting to transfer from {self.remote_dir}/{f} to {self.local_directory}/{f}")
                    sftp_client.get(f'{self.remote_dir}/{f}', f'{self.local_directory}/{f}')

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return matching_files


def _make_intermediate_dirs(sftp_client, remote_directory) -> None:
    """
    Create all the intermediate directories in a remote host

    :param sftp_client: A Paramiko SFTP client.
    :param remote_directory: Absolute Path of the directory containing the file
    :return:
    """
    if remote_directory == '/':
        sftp_client.chdir('/')
        return
    if remote_directory == '':
        return
    try:
        sftp_client.chdir(remote_directory)
    except OSError:
        dirname, basename = os.path.split(remote_directory.rstrip('/'))
        _make_intermediate_dirs(sftp_client, dirname)
        sftp_client.mkdir(basename)
        sftp_client.chdir(basename)
        return
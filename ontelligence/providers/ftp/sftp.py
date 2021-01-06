import os
from typing import Optional, List

from ontelligence.providers.ftp.base import BaseSftpProvider


class SFTP(BaseSftpProvider):

    path = None
    packet_size = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
        if 'packet_size' in kwargs:
            self.packet_size = kwargs['packet_size']

    def make_directory(self, path: str) -> None:
        return self.get_conn().mkdir(remotepath=path)

    def list_directory(self, path: Optional[str] = None):
        if path:
            self.get_conn().cwd(path)
        return self.get_conn().listdir()

    def get_latest_file(self, files: List[str]):
        latest_file = None
        latest_time = None
        for file in files:
            time = self.get_conn().sendcmd('MDTM ' + file)
            if (latest_time is None) or (time > latest_time):
                latest_file = file
                latest_time = time
        return latest_file

    def download_file(self, file: str, output_folder: Optional[str] = ''):
        local_path = os.path.join(output_folder, file)
        self.get_conn().get(remotepath=file, localpath=local_path)
        return local_path

    def upload_file(self, file_path: str):
        file_name = os.path.split(file_path)[1]
        self.get_conn().put(localpath=file_path, remotepath=file_name)

    def delete_file(self, file: str):
        raise NotImplementedError

    def move_file(self, file: str, new_file_path: str):
        raise NotImplementedError

    def get_size(self, file: str, is_binary=False, is_ascii=False):
        raise NotImplementedError

########################################################################################################################
# Metadata.
########################################################################################################################

    def get_file_modification_date(self, file):
        raise NotImplementedError

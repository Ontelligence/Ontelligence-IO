import os
from typing import Optional, List

from ontelligence.providers.ftp.base import BaseFtpProvider


class FTP(BaseFtpProvider):

    path = None
    packet_size = None

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(conn_id=conn_id, **kwargs)
        if 'packet_size' in kwargs:
            self.packet_size = kwargs['packet_size']

    def make_directory(self, path: str) -> None:
        return self.get_conn().mkd(dirname=path)

    def list_directory(self, path: Optional[str] = None):
        if path:
            self.get_conn().cwd(path)
        return self.get_conn().nlst()

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
        with open(local_path, 'wb') as f:
            self.get_conn().retrbinary('RETR ' + file, f.write)
        self.log.info('Downloaded file from FTP: {}\n'.format(local_path))
        return local_path

    def upload_file(self, file_path: str):
        file_name = os.path.split(file_path)[1]
        self.get_conn().put(localpath=file_path, remotepath=file_name)
        with open(file_path, 'rb') as f:
            self.get_conn().storbinary('STOR ' + file_name, f)
        self.log.info('Uploaded {} to {}'.format(file_name, self.path))

    def delete_file(self, file: str):
        self.get_conn().delete(filename=file)

    def move_file(self, file: str, new_file_path: str):
        self.get_conn().rename(fromname=file, toname=new_file_path)

    def get_size(self, file: str, is_binary=False, is_ascii=False):
        if is_binary:
            self.get_conn().sendcmd('TYPE I')
        if is_ascii:
            self.get_conn().sendcmd('TYPE A')
        return self.get_conn().size(filename=file)

########################################################################################################################
# Metadata.
########################################################################################################################

    def get_file_modification_date(self, file):
        file_details = [x for x in self.get_conn().mlsd() if file in x[0]][0]
        return file_details[1]['modify']

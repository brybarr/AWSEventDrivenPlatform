import paramiko
from datetime import datetime
from functools import partial


def progress_callback(transferred, total, file):
    progress_percent = transferred / total * 100
    if progress_percent == 100:
        print(f"Downloaded successfully [{file}]: {progress_percent:.2f}%")


class SFTPClient:
    def __init__(self, HOSTNAME: str, USERNAME: str, PASSWORD: str):
        self.hostname = HOSTNAME
        self.username = USERNAME
        self.password = PASSWORD
        self.SSHClient = paramiko.SSHClient()

    def getListOfFilesFromPath(self, path: str):
        with self.SSHClient as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname,
                               username=self.username, password=self.password)
            with ssh_client.open_sftp() as sftp_client:
                sftp_client.chdir(path)
                files = sftp_client.listdir()
        return files

    def transferFilesToLocal(self, sftp_file_path, localFilePath, is_debug=False):
        file_to_process = f"{sftp_file_path} to {localFilePath}"
        with self.SSHClient as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname,
                               username=self.username, password=self.password)
            with ssh_client.open_sftp() as sftp_client:
                if is_debug:
                    sftp_client.get(
                        sftp_file_path,
                        localFilePath,
                        callback=partial(progress_callback,
                                         file=file_to_process)
                    )
                else:
                    sftp_client.get(
                        sftp_file_path,
                        localFilePath
                    )

    def getFileTimestampInSFTP(self, sftp_file_path):
        with self.SSHClient as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname,
                               username=self.username, password=self.password)
            with ssh_client.open_sftp() as sftp_client:
                file_ts = str(datetime.fromtimestamp(
                    sftp_client.stat(sftp_file_path).st_mtime))
        return file_ts

    def sortFilesBasedOnLastModifiedDate(self, files, desc=True):
        with self.SSHClient as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname,
                               username=self.username, password=self.password)
            with ssh_client.open_sftp() as sftp_client:
                if desc:
                    sorted_files = sorted(
                        files, key=lambda x: sftp_client.stat(x).st_mtime, reverse=True)
                else:
                    sorted_files = sorted(
                        files, key=lambda x: sftp_client.stat(x).st_mtime)
        return sorted_files

    def removeOldFilesFromListByCutoffDt(self, files, cutoff_dt):
        cutoff_dt = datetime.strptime(cutoff_dt, '%Y-%m-%d %H:%M:%S')
        with self.SSHClient as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname,
                               username=self.username, password=self.password)
            with ssh_client.open_sftp() as sftp_client:
                new_files = [
                    new_file for new_file in files if datetime.strptime(str(datetime.fromtimestamp(
                        sftp_client.stat(new_file).st_mtime)), '%Y-%m-%d %H:%M:%S') > cutoff_dt
                ]
        return new_files

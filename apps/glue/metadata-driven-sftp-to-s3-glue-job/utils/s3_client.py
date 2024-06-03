import boto3
import os
from functools import partial


def progress_callback(bytes_transferred, localFilePath, bucket):
    progress_percent = int(100 * bytes_transferred /
                           os.path.getsize(localFilePath))
    if progress_percent == 100:
        print(
            f"Uploaded successfully [{localFilePath} to {bucket}]: {progress_percent}%")


class S3Client:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_transfer = boto3.s3.transfer.S3Transfer(self.s3_client)

    def uploadLocalFileToS3(self, _localFilePath: str, _bucket: str, _objectKey: str, is_debug=False):
        destination_uri = f'{_bucket}/{_objectKey}'
        destination_bucket_name, destination_object_key = destination_uri.split(
            '/', 1)
        if is_debug:
            self.s3_transfer.upload_file(
                _localFilePath,
                destination_bucket_name,
                destination_object_key,
                callback=partial(progress_callback,
                                 localFilePath=_localFilePath, bucket=_bucket)
            )
        else:
            self.s3_transfer.upload_file(
                _localFilePath,
                destination_bucket_name,
                destination_object_key
            )

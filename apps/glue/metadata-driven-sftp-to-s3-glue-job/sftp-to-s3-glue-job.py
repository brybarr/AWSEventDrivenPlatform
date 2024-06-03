
from utils.secrets_manager import SecretManager
from utils.s3_client import S3Client
from utils.snowflake_client import SnowflakeClient
from utils.sftp_client import SFTPClient
from datetime import datetime
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys
import re
import tempfile
import os


class SftpToS3():
    def __init__(self) -> None:
        self.args = getResolvedOptions(
            sys.argv, ['snowflake_db', 'snowflake_schema', 'snowflake_ingestion_metadata_table'])
        self.db = self.args['snowflake_db']
        self.schema = self.args['snowflake_schema']
        self.ingestion_metadata_table = self.args['snowflake_ingestion_metadata_table']
        self.snowflake_client = SnowflakeClient(db=self.db, schema=self.schema)
        self.s3_client = S3Client()
        self.secrets_manager = SecretManager()

    def runSftpToS3(self):
        metadata = self.snowflake_client.getDataFromTable(
            query=f"""
                SELECT *,
                    REGEXP_SUBSTR(ON_LANDING[0], '"file_name_pattern": "([^"]+)"', 1, 1, 'e') AS FILE_PATTERN
                FROM {self.ingestion_metadata_table}
                WHERE SFTP_FOLDER IS NOT NULL
            """
        )

        num_of_config_processed = 0
        for config in metadata:
            num_of_config_processed += 1

            self.snowflake_client.updateJsonEvent(
                SOURCE_NAME=config.get('SOURCE_NAME'),
                GENERIC_FILE_NAME=config.get('GENERIC_FILE_NAME'),
                BUCKET_NAME=config.get('LANDING_BUCKET_PATH'),
                FILE_PATH=config.get('SFTP_FOLDER'),
            )
            # Get SFTP Credentials From Secret Manager
            component_name = 'Getting SFTP Credentials'
            USERNAME, PASSWORD, HOSTNAME = (
                self.secrets_manager.getSftpCredsFromSecrets(
                    config.get('SECRET_ID'))
            )
            sftp_client = SFTPClient(HOSTNAME, USERNAME, PASSWORD)

            try:
                # Listing of files from SFTP Path
                component_name = 'Listing of Files'
                remote_files = sftp_client.getListOfFilesFromPath(
                    config.get('SFTP_FOLDER'))

                # Checks if there is file pattern in config
                component_name = 'Checking if there is File Pattern'
                file_pattern = config.get('FILE_PATTERN')
                if file_pattern:
                    print(f'File Pattern Found {file_pattern}')
                    matched_files = [file for file in remote_files if re.compile(
                        file_pattern).search(file)]
                else:
                    print('No File Pattern')
                    continue

                # Get latest file timestamp for specific source_name in events table
                component_name = 'Getting Latest File Timestamp'
                event_latest_ts = self.snowflake_client.getLatestTimestampBySourceNameAndGenericFilename(
                    config.get('SOURCE_NAME'), config.get('GENERIC_FILE_NAME'))
                print(f'Latest File Timestamp {event_latest_ts}')

                # Check if latest file timestamp from events table is None
                component_name = 'Matching and Filtering Remote Files'
                if event_latest_ts == 'None':
                    new_remote_files = matched_files
                else:
                    print(event_latest_ts)
                    # add sftp path for all files in the list
                    matched_files = [
                        f"{config.get('SFTP_FOLDER')}/{file}" for file in matched_files]
                    # sort the list by last modified date DESC
                    sorted_files = sftp_client.sortFilesBasedOnLastModifiedDate(
                        matched_files)
                    # get only latest files starting from cutoff dt
                    new_remote_files = sftp_client.removeOldFilesFromListByCutoffDt(
                        sorted_files, event_latest_ts)
                    # remove sftp path from files in the list
                    new_remote_files = [file.replace(
                        f"{config.get('SFTP_FOLDER')}/", "")for file in new_remote_files]

                print(f"ALL FILES COUNT: {len(remote_files)}")
                print(f"MATCHED FILES COUNT: {len(matched_files)}")
                print(f"NEW FILES COUNT: {len(new_remote_files)}")

                # Copy files to s3 if there are matched files
                if len(new_remote_files) > 0:
                    component_name = 'Copying File From SFTP to S3'
                    # print(f"STAGE: {component_name}")

                    for _file in new_remote_files:
                        sftp_file_path = f"{config.get('SFTP_FOLDER')}/{_file}"
                        localTmpDir = tempfile.gettempdir()
                        localFilePath = os.path.normpath(
                            os.path.join(localTmpDir, _file))
                        file_timestamp = (
                            sftp_client.getFileTimestampInSFTP(sftp_file_path)
                        )

                        sftp_client.transferFilesToLocal(
                            sftp_file_path, localFilePath)
                        self.s3_client.uploadLocalFileToS3(
                            localFilePath, config.get('LANDING_BUCKET_PATH'), _file)

                        self.snowflake_client.updateJsonEvent(
                            ACTION_STATUS="SUCCESS",
                            COMPONENT_NAME=component_name,
                            ACTION=f"Successfully Copied File {sftp_file_path} to S3 {config.get('LANDING_BUCKET_PATH')}",
                            ACTION_TIMESTAMP=str(datetime.now()),
                            FILE_NAME=_file,
                            FILE_TIMESTAMP=file_timestamp,
                        )
                        self.snowflake_client.logFileAuditEvent()

            except Exception as err:
                # print(f"COPYING FAILED: {component_name} -> Error: {errMsg}")
                self.snowflake_client.updateJsonEvent(
                    COMPONENT_NAME=component_name,
                    ACTION=f"Failed to copy file: {err}",
                    ACTION_STATUS="FAILED",
                    ACTION_TIMESTAMP=str(datetime.now()),
                )
                self.snowflake_client.logFileAuditEvent()

            print(
                f"Processed Config in Metadata: {num_of_config_processed}/{len(metadata)}")


if __name__ == "__main__":
    SftpToS3().runSftpToS3()

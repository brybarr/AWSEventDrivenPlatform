import snowflake.connector as snowflake_conn
import json
from snowflake.connector import ProgrammingError, DictCursor
from utils.secrets_manager import SecretManager
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys


class SnowflakeClient:
    def __init__(self, db: str, schema: str):
        self.args = getResolvedOptions(
            sys.argv, ['environment', 'snowflake_event_logs_table', 'snowflake_account'])
        self.environment = self.args['environment']
        self.events_table = self.args['snowflake_event_logs_table']
        self.account = self.args['snowflake_account']
        self.warehouse = {
            'POC': 'DP_DEV_WH_ADMIN',
            'DEV': 'DP_DEV_WH_ADMIN',
            'PROD': 'DP_SRC_SRV_WH_PROD'
        }.get(self.environment.upper(), None)
        self.db = db
        self.schema = schema
        secrets_manager = SecretManager()
        self.user, self.password = (
            secrets_manager.getSnowflakeCredsFromSecrets(
                f"tmr-dp-{self.environment}/snowflake-secrets")
        )
        self.event_dict = self.createInitialJsonEvent()

    def getDataFromTable(self, query: str):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            return sf_conn.cursor(DictCursor).execute(query).fetchall()

    def getLatestExecutionId(self):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            query = f"""
                    SELECT CAST(ZEROIFNULL(MAX(execution_id)) AS INT) + 1 AS MAX_EXEC_ID
                    FROM {self.events_table}
                """
            return sf_conn.cursor(DictCursor).execute(query).fetchone().get('MAX_EXEC_ID')

    def getFileTsInLogs(self, file_name):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            query = f"""
                    SELECT MAX(FILE_TIMESTAMP) AS FILE_TIMESTAMP
                    FROM {self.events_table}
                    WHERE FILE_NAME = '{file_name}' AND ACTION_STATUS LIKE '%SUCCESS%'
                """
            file_ts = (
                sf_conn.cursor(DictCursor).execute(
                    query).fetchone().get('FILE_TIMESTAMP')
            )
            return str(file_ts)

    def getLatestTimestampBySourceNameAndGenericFilename(self, source_name, generic_file_name):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            query = f"""
                    SELECT MAX(file_timestamp) as LAST_PROCESSED_TS
                    FROM  {self.events_table}
                    WHERE LOWER(source_name) = LOWER('{source_name}') AND LOWER(generic_file_name) = LOWER('{generic_file_name}')
            """
            latest_ts = (
                sf_conn.cursor(DictCursor).execute(
                    query).fetchone().get('LAST_PROCESSED_TS')
            )
            return str(latest_ts)

    def isFileExists(self, file_path):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            query = f"""
                    SELECT CONCAT(file_path, '/',file_name) AS FILE_FULL_PATH
                    FROM {self.events_table}
                    WHERE CONCAT(file_path, '/',file_name) = '{file_path}'
                """
            result = sf_conn.cursor(DictCursor).execute(query).fetchone()
            if result is None:
                return False
            else:
                return True

    def createInitialJsonEvent(self):
        event_dict = {}
        event_dict['SOURCE_NAME'] = None
        event_dict['GENERIC_FILE_NAME'] = None
        event_dict['BUCKET_NAME'] = None
        event_dict['FILE_PATH'] = None
        event_dict['COMPONENT_NAME'] = None
        event_dict['ACTION'] = None
        event_dict['ACTION_TIMESTAMP'] = str(datetime.now())
        event_dict['ACTION_STATUS'] = None
        event_dict['FILE_NAME'] = None
        event_dict['FILE_TIMESTAMP'] = None
        return event_dict

    def updateJsonEvent(self, **kwargs):
        self.event_dict['SOURCE_NAME'] = kwargs['SOURCE_NAME'] if 'SOURCE_NAME' in kwargs.keys(
        ) else self.event_dict['SOURCE_NAME']
        self.event_dict['GENERIC_FILE_NAME'] = kwargs['GENERIC_FILE_NAME'] if 'GENERIC_FILE_NAME' in kwargs.keys(
        ) else self.event_dict['GENERIC_FILE_NAME']
        self.event_dict['BUCKET_NAME'] = kwargs['BUCKET_NAME'] if 'BUCKET_NAME' in kwargs.keys(
        ) else self.event_dict['BUCKET_NAME']
        self.event_dict['FILE_PATH'] = kwargs['FILE_PATH'] if 'FILE_PATH' in kwargs.keys(
        ) else self.event_dict['FILE_PATH']
        self.event_dict['COMPONENT_NAME'] = kwargs['COMPONENT_NAME'] if 'COMPONENT_NAME' in kwargs.keys(
        ) else self.event_dict['COMPONENT_NAME']
        self.event_dict['ACTION'] = kwargs['ACTION'] if 'ACTION' in kwargs.keys(
        ) else self.event_dict['ACTION']
        self.event_dict['ACTION_TIMESTAMP'] = kwargs['ACTION_TIMESTAMP'] if 'ACTION_TIMESTAMP' in kwargs.keys(
        ) else self.event_dict['ACTION_TIMESTAMP']
        self.event_dict['ACTION_STATUS'] = kwargs['ACTION_STATUS'] if 'ACTION_STATUS' in kwargs.keys(
        ) else self.event_dict['ACTION_STATUS']
        self.event_dict['FILE_NAME'] = kwargs['FILE_NAME'] if 'FILE_NAME' in kwargs.keys(
        ) else self.event_dict['FILE_NAME']
        self.event_dict['FILE_TIMESTAMP'] = kwargs['FILE_TIMESTAMP'] if 'FILE_TIMESTAMP' in kwargs.keys(
        ) else self.event_dict['FILE_TIMESTAMP']

    def logFileAuditEvent(self):
        with snowflake_conn.connect(
            account=self.account,
            warehouse=self.warehouse,
            database=self.db,
            schema=self.schema,
            user=self.user,
            password=self.password
        ) as sf_conn:
            json_event = (
                json.loads(json.dumps(self.event_dict))
            )
            SOURCE_NAME = json_event.get(
                'SOURCE_NAME') if json_event.get('SOURCE_NAME') else ''
            GENERIC_FILE_NAME = json_event.get(
                'GENERIC_FILE_NAME') if json_event.get('GENERIC_FILE_NAME') else ''
            BUCKET_NAME = json_event.get(
                'BUCKET_NAME') if json_event.get('BUCKET_NAME') else ''
            FILE_PATH = json_event.get(
                'FILE_PATH') if json_event.get('FILE_PATH') else ''
            FILE_NAME = json_event.get(
                'FILE_NAME') if json_event.get('FILE_NAME') else ''
            FILE_TIMESTAMP = json_event.get(
                'FILE_TIMESTAMP') if json_event.get('FILE_TIMESTAMP') else ''
            COMPONENT_NAME = json_event.get(
                'COMPONENT_NAME') if json_event.get('COMPONENT_NAME') else ''
            ACTION = json_event.get(
                'ACTION') if json_event.get('ACTION') else ''
            ACTION_STATUS = json_event.get(
                'ACTION_STATUS') if json_event.get('ACTION_STATUS') else ''
            ACTION_TIMESTAMP = json_event.get(
                'ACTION_TIMESTAMP') if json_event.get('ACTION_TIMESTAMP') else ''

            query = f"""
                INSERT INTO {self.events_table}
                (
                    SOURCE_NAME,
                    GENERIC_FILE_NAME,
                    BUCKET_NAME,
                    FILE_PATH,
                    FILE_NAME,
                    FILE_TIMESTAMP,
                    COMPONENT_NAME,
                    ACTION,
                    ACTION_STATUS,
                    ACTION_TIMESTAMP
                )
                VALUES
                (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
            """

            sf_conn.cursor().execute(
                query,
                (
                    SOURCE_NAME,
                    GENERIC_FILE_NAME,
                    BUCKET_NAME,
                    FILE_PATH,
                    FILE_NAME,
                    FILE_TIMESTAMP,
                    COMPONENT_NAME,
                    ACTION,
                    ACTION_STATUS,
                    ACTION_TIMESTAMP
                )
            )

import json
import sys
from abc import abstractmethod
if sys.version_info >= (3,8):
    from importlib import metadata
else:
    import importlib_metadata as metadata
import logging
import boto3
from datetime import datetime
from dateutil import tz
import snowflake.connector as snowflake_connector
import rules


logger = logging.getLogger()
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

class StaticEntryPoint:
    def __init__(self, name, cls_target):
        self._cls_target = cls_target
        self.name = name

    def load(self):
        return self._cls_target


processing_rules = {entry.name: entry for entry in
                        set(metadata.entry_points().get("dp.rules",
                                                        (StaticEntryPoint("s3.file_name_regex_check",
                                                                          rules.FilenamePatternRegexRule),
                                                         StaticEntryPoint("s3.file_size_check",
                                                                          rules.FilenameSizeRule),
                                                         StaticEntryPoint("s3.duplication_check",
                                                                          rules.FileDuplicationCheckRule),
                                                         StaticEntryPoint("s3.decompress_archive",
                                                                          rules.DecompressArchive),
                                                         StaticEntryPoint("glue.call_job",
                                                                          rules.CallGlueJob))))}


class BaseContext:

    def __init__(self, context, variables, event_source, event_key, event_type):
        self._process_context = context
        self._variables = variables
        self._event_source = event_source
        self._event_key = event_key
        self._event_type = event_type
        self._config = {}
        self._start_time = datetime.utcnow()

    @property
    def config(self):
        return self._config

    @abstractmethod
    def init(self, event_data):
        pass

    @abstractmethod
    def get_rules_config(self):
        pass

    @abstractmethod
    def log_audit_event(self, bucket_name, file_path, file_name, component_name, action, action_status, file_timestamp=''):
        pass

    @abstractmethod
    def get_audit_events(self, bucket_name, file_path, file_name, component_name, action_status, timestamp=None):
        pass

    @abstractmethod
    def mark_failed(self, file_key):
        pass

    @abstractmethod
    def mark_success(self, file_key):
        pass


class SnowflakeContext(BaseContext):

    def __init__(self, user, password, **kwargs):
        super(SnowflakeContext, self).__init__(**kwargs)
        self._account = self._variables.get("SNOWFLAKE_ACCOUNT")
        self._warehouse = self._variables.get("SNOWFLAKE_WAREHOUSE")
        self._user = user
        self._password = password
        self._metadata_database = self._variables.get("SNOWFLAKE_METADATA_DATABASE")
        self._metadata_schema = self._variables.get("SNOWFLAKE_METADATA_SCHEMA")
        self._metadata_rules_table = self._variables.get("SNOWFLAKE_METADATA_TABLE")
        self._event_log_table = self._variables.get("SNOWFLAKE_EVENT_LOG_TABLE")
        self._conn = False
        try:
            self._conn = snowflake_connector.connect(account=self._account,
                                               warehouse=self._warehouse,
                                               database=self._metadata_database,
                                               schema=self._metadata_schema,
                                               user=self._user,
                                               password=self._password)
            query = (f"SELECT * FROM "
                     f"{self._metadata_database}.{self._metadata_schema}.{self._metadata_rules_table}"
                     f" WHERE {self._event_type}_bucket_path = '{self._event_source}/{self._event_key}'")
            logger.info(f"Executing {query}")
            results = self._conn.cursor(snowflake_connector.DictCursor).execute(query).fetchall()
            logger.info(f"Results of the query: {results}")
            if not results or len(results) != 1:
                raise Exception(f"Expecting exactly one rule with references "
                                f"{self._event_type}_bucket_path = '{self._event_source}/{self._event_key}', but found "
                                f"{len(results) if results else 'none'}.")
            logger.info("Converting results of the query into dict to be used later")
            self._config = {key.lower():value for key, value in results[0].items()}
        except Exception as ex:
            if self._conn:
                self._conn.close()
            raise ex

    def init(self, file_key):
        file_keys = file_key.split("/")
        bucket = file_keys[0]
        file_path = "/".join(file_keys[1:-1])
        file_name = file_keys[-1]
        logger.info(f"Starting context for processing {file_key}")
        self.log_audit_event(bucket_name=bucket,
                             file_path=file_path,
                             file_name=file_name,
                             component_name="Context",
                             action='Processing Context', action_status='INIT')

    def get_rules_config(self):
            logger.info("Parsing rules config from the context init information")
            rule_configs = {rule_def if rule_def.find("{") < 0 else rule_def[:rule_def.index("{")]:
                                {} if rule_def.find("{") < 0 else json.loads(rule_def[rule_def.index("{"):])
                            for rule_def in json.loads(self._config.get(f"on_{self._event_type}".upper(),
                                                                        self._config.get(f"on_{self._event_type}".lower(),
                                                                                         '[]')))}
            logger.info(f"Found following rule configs: {rule_configs}")
            return rule_configs

    def log_audit_event(self, bucket_name, file_path, file_name, component_name, action, action_status, file_timestamp=None):
        local_timezone = tz.gettz("Australia/Queensland") # get local time zone
        local_current_time = datetime.utcnow().astimezone(local_timezone)
        logger.info(f"Logging audit event to event table with details {bucket_name}, {file_path}, {file_name}, "
                    f"{file_timestamp}, {component_name}, {action}, {action_status}",)
        file_ts = f"'{file_timestamp}'" if file_timestamp else "NULL"
        query = (f"INSERT INTO {self._metadata_database}.{self._metadata_schema}.{self._event_log_table} "
                 f"VALUES ('{self._config.get('source_name')}', '{self._config.get('generic_file_name')}', "
                 f"'{bucket_name}', '{file_path}', '{file_name}', {file_ts}, '{self._event_type}/{component_name}', "
                 f"'{action}', '{action_status}', '{local_current_time.strftime('%Y-%m-%d %H:%M:%S')}')")
        logger.info(f"Executing {query}")
        self._conn.cursor().execute(query)

    def get_audit_events(self, bucket_name, file_path, file_name, component_name, action_status, timestamp=None):
        query = (f"SELECT * FROM {self._metadata_database}.{self._metadata_schema}.{self._event_log_table}"
                 f" WHERE SOURCE_NAME = '{self.config.get('source_name')}' AND"
                 f" GENERIC_FILE_NAME = '{self.config.get('generic_file_name')}'"
                 f" AND BUCKET_NAME = '{bucket_name}'"
                 f" AND FILE_PATH = '{file_path}'"
                 f" AND FILE_NAME = '{file_name}'"
                 f" AND COMPONENT_NAME = '{self._event_type}/{component_name}'"
                 f" AND ACTION_STATUS = '{action_status}'"
                 f" AND ACTION_TIMESTAMP > '{timestamp}'") if timestamp else\
            (f"SELECT * FROM {self._metadata_database}.{self._metadata_schema}.{self._event_log_table}"
                 f" WHERE SOURCE_NAME = '{self.config.get('source_name')}' AND"
                 f" GENERIC_FILE_NAME = '{self.config.get('generic_file_name')}'"
                 f" AND BUCKET_NAME = '{bucket_name}'"
                 f" AND FILE_PATH = '{file_path}'"
                 f" AND FILE_NAME = '{file_name}'"
                 f" AND COMPONENT_NAME = '{self._event_type}/{component_name}'"
                 f" AND ACTION_STATUS = '{action_status}'")

        logger.info(f"Executing query: {query}")
        results = self._conn.cursor().execute(query).fetchall()
        # results = []
        return results

    def mark_failed(self, file_key):
        failure_call_ref = {"landing": self._landing_failure, "ingestion": self._ingestion_failure}.get(self._event_type)
        try:
            file_keys = file_key.split("/")
            bucket = file_keys[0]
            file_path = "/".join(file_keys[1:-1])
            file_name = file_keys[-1]
            logger.info(f"Marking processing {file_key} as failed")
            self.log_audit_event(bucket_name=bucket,
                                 file_path=file_path,
                                 file_name=file_name,
                                 component_name="Context",
                                 action='Processing Context', action_status='FAILED')
            failure_call_ref(file_key)
        finally:
            if self._conn:
                self._conn.close()

    def mark_success(self, file_key):
        success_call_ref = {"landing": self._landing_success, "ingestion": self._ingestion_success}.get(self._event_type)
        try:
            success_call_ref(file_key)
            file_keys = file_key.split("/")
            bucket = file_keys[0]
            file_path = "/".join(file_keys[1:-1])
            file_name = file_keys[-1]
            logger.info(f"Marking processing {file_key} as success")
            self.log_audit_event(bucket_name=bucket,
                                 file_path=file_path,
                                 file_name=file_name,
                                 component_name="Context",
                                 action='Processing Context', action_status='SUCCESS')
        finally:
            if self._conn:
                self._conn.close()

    def _landing_success(self, file_key):
        file_keys = file_key.split("/")
        copy_file = {"Bucket": file_keys[0],"Key": '/'.join(file_keys[1:])} 
        s3_resource.meta.client.copy(copy_file, f"{file_keys[0]}", f"archives/{'/'.join(file_keys[1:-1])}{self._start_time.strftime('/%Y/%m/%d/%H%M%S_')}{file_keys[-1]}")
        target_bucket = self._config.get("ingestion_bucket_path").split('/')[0] \
            if self._config.get("ingestion_bucket_path") else None
        if target_bucket:
            target_bucket_key = "/".join(self._config.get("ingestion_bucket_path").split('/')[1:])
            logger.info(f"Copying file {file_key} to {target_bucket}/{target_bucket_key}/{file_keys[-1]}")
            s3_resource.meta.client.copy(copy_file, f"{target_bucket}", f"{target_bucket_key}/{file_keys[-1]}")
        s3_resource.Object(f"{file_key.split('/')[0]}", f"{'/'.join(file_key.split('/')[1:])}").delete()
            

    def _landing_failure(self, file_key):
        target_bucket = self._config.get("rejection_bucket_path").split('/')[0]
        target_bucket_key = "/".join(self._config.get("rejection_bucket_path").split('/')[1:])
        logger.info(f"Copying file {file_key} to {target_bucket}/{target_bucket_key}/{file_key.split('/')[-1]}")
        file_keys = file_key.split("/")
        copy_file = {"Bucket": file_keys[0],"Key": '/'.join(file_keys[1:])}         
        s3_resource.meta.client.copy(copy_file, f"{file_keys[0]}", f"archives/{'/'.join(file_keys[1:-1])}{self._start_time.strftime('/%Y/%m/%d/%H%M%S_')}{file_keys[-1]}")
        s3_resource.meta.client.copy(copy_file, f"{target_bucket}", f"{target_bucket_key}/{file_keys[-1]}")
        s3_resource.Object(f"{file_key.split('/')[0]}", f"{'/'.join(file_keys[1:])}").delete()


    def _ingestion_success(self, file_name):
        pass

    def _ingestion_failure(self, file_name):
        pass


class RulesFactory:

    # EVENT_DICTIONARY = {"ObjectCreated:Put": "put"}
    # EVENT_SOURCE_IDS = {"landing": "LANDING_BUCKET"}

    @classmethod
    def build_rules(cls, context):
        rules_config = context.get_rules_config()
        factory = RulesFactory()
        rules = []
        for rule_key, rule_config in rules_config.items():
            rule = factory.build_rule(rule_key, rule_config)
            rules.append(rule)
        return rules

    def build_rule(self, rule_key, rule_config):
        logger.info(f"Loading rule for {rule_key} with config {rule_config}")
        rule_cls_def = processing_rules.get(rule_key)
        if rule_cls_def:
            return rule_cls_def.load()(**rule_config)
        else:
            raise Exception("No rules found for")
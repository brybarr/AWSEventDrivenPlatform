import sys
import os
import json
import logging
import traceback

from core import RulesFactory
from core import SnowflakeContext
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(logging.INFO)
console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
formater = logging.Formatter('%(levelname)-8s %(message)s')
console.setFormatter(formater)
logger.addHandler(console)


def get_secrets(secret_arn):
    logger.info(f"Retriving secrets from secrets manager {secret_arn}")
    if secret_arn:
        client = boto3.client(service_name='secretsmanager', region_name='ap-southeast-2')
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_arn
        )
        secret = get_secret_value_response['SecretString']
        return json.loads(secret, strict=False)
    else:
        raise Exception("Snowflake secret is missing, requires for connection to metadata store")


def run(event, context):
    logger.info(f"Received event {json.dumps(event)} for processing")
    events_mappings = json.loads(os.environ.get("events_mappings", "{}"))
    context_variables = json.loads(os.environ.get("context_variables", "{}"))
    secrets = get_secrets(context_variables.get("SNOWFLAKE_SECRET_ARN"))
    for record in event.get("Records", []):
        snowflake_context = False
        if record.get("eventSource", "") == "aws:s3" and record.get("eventName", "").split(":")[0] == "ObjectCreated" \
                and not(record.get("s3", {}).get("object", {}).get("key", "/").endswith("/"))\
                and not(record.get("s3", {}).get("object", {}).get("key", "/").startswith("archives/")):
            logger.info(f"Processing {record.get('eventName')} event for {record.get('s3').get('bucket').get('name')}/"
                        f"{record.get('s3').get('object').get('key')}")
            try:
                event_type = events_mappings.get(record.get("s3").get("configurationId"), False)
                if not event_type:
                    raise Exception(f"Could not find event type mapped to the configuration id "
                                    f"{record.get('s3').get('configurationId')}")
                event_details = {"event_source": record.get("s3").get("bucket").get("name"),
                                 "event_key": f"{'/'.join(record.get('s3').get('object').get('key').split('/')[:-1])}",
                                 "event_type": event_type}
                logger.info(f"Creating snowflake context for the file processing with {event_details}")
                snowflake_context = SnowflakeContext(context=context, variables=context_variables, **secrets,
                                                     **event_details)
                snowflake_context.init(f"{record.get('s3').get('bucket').get('name')}/"
                                       f"{record.get('s3').get('object').get('key')}")
                rules = RulesFactory.build_rules(snowflake_context)
                logger.info(f"Found {len(rules)} rules configured for {event_type} "
                            f"of {record.get('s3').get('object').get('key')}")
                for rule in rules:
                    rule.apply(snowflake_context, record.get("s3"))
            except Exception as ex:
                logger.error(f"While processing {record.get('s3').get('bucket').get('name')}/"
                             f"{record.get('s3').get('object').get('key')} exception is raised as {ex}")
                traceback.print_exc()
                if snowflake_context:
                    snowflake_context.mark_failed(f"{record.get('s3').get('bucket').get('name')}/"
                                                  f"{record.get('s3').get('object').get('key')}")
                continue
            snowflake_context.mark_success(f"{record.get('s3').get('bucket').get('name')}/"
                                           f"{record.get('s3').get('object').get('key')}")

        else:
            logger.warning(f"No Even handing is defined for {record.get('eventName')} events or "
                           f"folders are recognize as objects ,Ignoring "
                           f"{record.get('s3').get('bucket').get('name')}/{record.get('s3').get('object').get('key')}")


if __name__ == "__main__":
    import os
    os.environ["SNOWFLAKE_ACCOUNT"] = "lj20743.ap-southeast-2"
    os.environ["SNOWFLAKE_WAREHOUSE"] = "DP_SRC_SRV_WH_PROD"
    os.environ["SNOWFLAKE_METADATA_DATABASE"] = "DP_DEV_MTD"
    os.environ["SNOWFLAKE_METADATA_SCHEMA"] = "MTD"
    os.environ["SNOWFLAKE_METADATA_TABLE"] = "INGESTION_METADATA"
    os.environ["SNOWFLAKE_EVENT_LOG_TABLE"] = "EVENT_LOGS"
    os.environ["SNOWFLAKE_SECRET_ARN"] = "SNOWFLAKE_SECRET_ARN"

    run({...})

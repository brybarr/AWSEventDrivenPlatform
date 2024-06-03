# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import json
import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
import sys


class SecretManager:
    def __init__(self):
        self.args = getResolvedOptions(
            sys.argv, ['snowflake_region'])
        self.region = self.args['snowflake_region']

    def getSftpCredsFromSecrets(self, _secret_name: str):

        secret_name = _secret_name
        region_name = self.region

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        # Decrypts secret using the associated KMS key.
        _secrets = json.loads(get_secret_value_response['SecretString'])
        _username = _secrets['username']
        _password = _secrets['password']
        _host = _secrets['hostname']

        return _username, _password, _host

    def getSnowflakeCredsFromSecrets(self, _secret_name: str):

        secret_name = _secret_name
        region_name = self.region

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            raise e

        # Decrypts secret using the associated KMS key.
        _secrets = json.loads(get_secret_value_response['SecretString'])
        _username = _secrets['user']
        _password = _secrets['password']

        return _username, _password

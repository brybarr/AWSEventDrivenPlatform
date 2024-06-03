import json
import requests
import boto3
import jmespath
from urllib.parse import urlparse, parse_qs
import pandas
import smart_open
import time
import csv
import io
from zipfile import ZipFile

from datetime import datetime
from dateutil import tz
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)




# Retrieve API Credentials
def retrieve_api_credentials(event):
    ''' Extracts securely stored secrets from Secret Manager
    
    Args:
        event (dict): payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
            
    Returns:
        dict: Secret values stored in a dictionary
    '''
    # Secrets manager client
    secretsmanager = boto3.client(service_name='secretsmanager', region_name='ap-southeast-2')
    
    #Extract secret
    sm_response = secretsmanager.get_secret_value(SecretId=event['secret_id'],)
    secretDict = json.loads(sm_response['SecretString'])
    
    return secretDict

# Define a session to make HTTP requests
def define_session(event, secretDict):
    ''' Defines a HTTP session object to persist parameters across requests
    
    Args:
        event (dict): payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
        secretDict (dict): Secret values stored in a dictionary
            
    Returns:
        Session Object
    '''
    
    # Create session and update headers and authentication as defined in config
    session = requests.Session()
    session.headers.update(event['headers'])
    if event['auth_method'] == 'BasicAuth':
        logger.info('Performing BasicAuth')
        session.auth = (secretDict['username'],secretDict['password'])
    
    if event['auth_method'] == 'APIKey':
        logger.info('Performing APIKey')
        session.params =(secretDict)
        
    if event['auth_method'] == 'OAuth2_client_credentials':
        logger.info(f'''[LAMBDA LOG] - Attempting to perfom OAuth2_client_credentials authenticatation with the following details:
            - client_id: {secretDict['client_id']}
            - client_secret: ######################
            - scope: {secretDict['scope']}
            ''')
        
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': secretDict['client_id'],
            'client_secret': secretDict['client_secret'],
            'scope': secretDict['scope']
            }

        token_r = requests.post(secretDict['access_token_url'], data=token_data, verify=False)
        token_r_json = token_r.json()
        token = token_r_json['access_token']
        api_call_headers = {'Authorization': 'Bearer ' + token}
        session.headers.update(api_call_headers)
    
    if event['auth_method'] == 'OAuth2_SAML_assertion':
        logger.info(f'''[LAMBDA LOG] - Attempting to authenticate with the following details:
            - client_id: {secretDict['client_id']}
            - token_url: {secretDict['token_url']}
            - token_url_body: {secretDict['token_url_body']}
            - idp_url: {secretDict['idp_url']}
            - private_key: ######################
            - grant_type: {secretDict['grant_type']}
            - company_id: {secretDict['company_id']}
            ''')
        
        saml_assertion_data = {
            'client_id': secretDict['client_id'],
            'token_url': secretDict['token_url_body'],
            'private_key': secretDict['private_key'],
            'user_id': secretDict['user_id']
            }
        
        headers_auth = {'Content-Type': 'application/x-www-form-urlencoded'}
        saml_assertion_payload = requests.post(secretDict['idp_url'], data=saml_assertion_data, headers=headers_auth, verify=False)
        saml_assertion = saml_assertion_payload.text

        token_data = {
            'client_id': secretDict['client_id'],
            'grant_type': secretDict['grant_type'],
            'company_id': secretDict['company_id'],
            'assertion': saml_assertion
            }

        access_token_payload = requests.post(secretDict['token_url'], data=token_data, headers=headers_auth, verify=False)
        access_token = access_token_payload.json()
        token = access_token['access_token']
        api_call_headers = {'Authorization': 'Bearer ' + token}
        session.headers.update(api_call_headers)
    
    return session

# Function to make API call
def send_request(session, url, event):
    ''' Defines a requests to the specified endpoint and returns the payload in
            JSON format
    
    Args:
        event (dict): Payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
        session (object): Containing persistent request parameters
        url (str): Endpoint to extract data from
            
    Returns:
        JSON Payload (list): JSON object returned as a list of dicts
    '''
    
    logger.info("-------")

    # Send HTTP request updating parameters as defined in config
    response = session.get(url, verify=False, params = event['query_params'])
    logger.info(f"[LAMBDA LOG] - Attempting to query the following URL {url} with query parameters {event['query_params']}")
    if response.status_code != 200:
        logger.info('[LAMBDA LOG] - ERROR RECEIVING PAYLOAD')
        logger.info(f'[LAMBDA LOG] - Response body {response.text}')
    
    
    if event['pagination'] == 'Daisy':
        logger.info(f'[LAMBDA LOG] - Request status code for batch {event["lambda_run_id"]}_{event["batch_id"]}: ' + str(response.status_code))
        
    # Again, should be configured based on source, perhaps just return response, and manage logic downstream
    if event['file_type'] == 'xml':
        logger.info(f'[LAMBDA LOG] - Returned {event["file_type"]} Response')
        return response.text
        
    elif event['file_type'] == 'CSV_ZIP':
        logger.info(f'[LAMBDA LOG] - Returned zipped CSV response, type: {type(response.content)}')
        # Open zip file
        with ZipFile(io.BytesIO(response.content)) as myzip:
            logger.info(f'[LAMBDA LOG] - List of files in zip file: {myzip.namelist()} (should only be one file)')
            # Initialise in-memory file-like object 
            csv_buffer = io.StringIO()
            # Convert to dataframe and write to csv_buffer
            df = pandas.read_csv(myzip.open(myzip.namelist()[0]))
            df.to_csv(csv_buffer, sep=",", index=False)
        return csv_buffer.getvalue()
        
    # Assume JSON data
    else:
        logger.info(f'[LAMBDA LOG] - Returned json Response')
    return response.text

# Write data to S3 using put_object() method
def write_to_s3(payload, file_name, s3_client, event):
    ''' Writes data to S3
    
    To do:
        - Return a value which can be used to infer success or failure
        - Pass in kms keys via parameters / boto call
    
    Args:
        event (dict): Payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
        payload (object): Contains data to be written to S3
        file_name (str): Object name to be used when storing data in S3
        s3_client (client): A low-level client representing Amazon Simple Storage Service (S3)
            
    Returns:
        None
    '''
    
    # Put file into S3
    s3_client.put_object(
        Body=json.dumps(payload),
        Bucket = event['landing_bucket'],
        Key=file_name,
        ServerSideEncryption ='aws:kms',
        SSEKMSKeyId=event[f"{event['secret_id_athena']}_kms_key"]
        )

    # Count the number of recorsd
    records_count = len(payload)
    
    logger.info(f'[LAMBDA LOG] - Successfully pushed {str(records_count)} records to S3')
    return None

# Write data to S3 using smart_open
def write_to_s3_smart_v2(event, payload, file_name, s3_client, keys=[], update_dict = {}):
    ''' Writes data to S3 via smart_open 
    
    To do:
        - Return a value which can be used to infer success or failure
        - Pass in kms keys via parameters / boto call
    
    Args:
        event (dict): Payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
        payload (object): Contains data to be written to S3
        file_name (str): Object name to be used when storing data in S3
        s3_client (client): A low-level client representing Amazon Simple Storage Service (S3)
        keys (dict): Returns the keys of the JSON payload to write the schema
            
    Returns:
        None
    '''
    
    # client_kwargs = {'S3.Client.create_multipart_upload': {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': event[f"{event['secret_id_landing']}_kms_key"]}, 
    #     'S3.Client.put_object': {'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': event[f"{event['secret_id_landing']}_kms_key"]}}

    url=f's3://{event["landing_bucket_path"]}/{file_name}'

    with smart_open.open(url, 'w', transport_params={'client': s3_client}) as csvfile:
        if event['file_type'] == 'XML' or event['file_type'] == 'CSV_ZIP' or event['file_type'] == 'json':
            logger.info(f'[LAMBDA LOG] - Writing file to {event["landing_bucket_path"]}/{file_name} in S3 via smart_open')
            csvfile.write(payload)
        else:
            writer = csv.DictWriter(csvfile, fieldnames=keys, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            count_row = 0
            for data in payload:
                writer.writerow({**data, **update_dict})
                count_row += 1
            logger.info(f'[LAMBDA LOG] - Finished writing {len(payload)} / {str(count_row)} records to {event["landing_bucket_path"]}/{file_name} in S3 via smart_open')

    return None

# Function to manage paginiation (next_url_method)
def paginiation_url_daisy(event, session, url, s3_client, key_prefix):
    ''' Iterates over all next url tokens until all data is ingested or the 
        max count is reached
    
    To do:
        - 
    
    Args:
        event (dict): Payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
        session (object): Containing persistent request parameters
        url (str): Endpoint to extract data from
        s3_client (client): A low-level client representing Amazon Simple Storage Service (S3)
        key_prefix (str): Prefix of all batches written to S3
            
    Returns:
        updated_params (dict): The updated parameters for the next lambda invocation
    '''
    
    # Initial parameters
    count = 0
    end_loop = False
    next_url_list = []
    
    # Loop over all endpoints while there is a next token
    while end_loop == False and count < int(event['batch_upper_limit']):
        
        # Call send_request method to extract payload
        event["batch_id"] = count
        payload = send_request(session, url, event)
        
        # Create file name based on DAG_ID, Module etc
        file_name = f'{key_prefix}/batch_{str(event["lambda_run_id"])}_{str(event["batch_id"])}'
        logger.info(f'[LAMBDA LOG] - Recieved payload for batch and uploaded to: {event["landing_bucket"]}/{file_name}')
        
        # Filter payload and define next_url
        filtered_data = payload['filtered_data']
        if len(filtered_data) == 0:
            
            logger.info(f'[LAMBDA LOG] - No data to ingest')
            return 'No data'
            
        next_url = payload['next_url']
        next_url_list.append(next_url)

        if event["s3_write_type"] == 'batch':
            file_name = file_name + '.json'
            write_to_s3(payload=filtered_data, file_name=file_name, s3_client=s3_client, event=event)

        elif event["s3_write_type"] == 'smart':
            if int(event["batch_id"]) == 0:
                keys=filtered_data[0].keys()
            file_name = file_name + '.csv'
            write_to_s3_smart_v2(event=event, payload=filtered_data, file_name=file_name, s3_client=s3_client, keys=keys)
        
        # Update parameters to use the new token
        updated_params = parse_qs(urlparse(next_url).query)
        
        # update event to use updated params
        event["query_params"] = updated_params

        # Break loop if the URL is None, indicates all data is extracted
        if next_url == None:
            end_loop = True
            logger.info('[LAMBDA LOG] - Finished extracting all data')
            break
        
        count += 1
        
    logger.info(f'[LAMBDA LOG] - Next URLs used: {str(next_url_list)}'.replace(", ","\n - ").replace("[","\n - ").replace("]",""))
    
    # Determine what to return back to airflow
    if next_url == None:
        return None
    else:
        return updated_params

def run(event, context):
    ''' Cooridinates tasks to manage the ingestion of data using various ingestion methods
    
    To do:
        - Handle other forms of pagination and auth
        - Currently only supports a list of dicts - how keys are generated depends on a list of dicts.
            - This is a common response from json - list of records in json format
        - Does not handle recursive flatten
        - Use event more cleverly - generalise! (should not need if statements for logging hopefully)

    Args:
        event (dict): Payload from airflow which contains all information / 
            configuration required to ingest data from the specified endpoint
            
    Returns:
        data (dict): The updated parameters for the next lambda invocation
    '''
    start_time = time.time()
    s3 = boto3.client('s3')
    
    # Perform an incremental load if configured
    if event['merge_pattern'] == 'incremental' and event['lambda_run_id'] == 0:
        logger.info(f'[LAMBDA LOG] - Performing incremental load')
        
        # Define query to determine update
        event["query"] = f'SELECT MAX({event["watermark_col"]}) AS LAST_MODIFIED_DATE FROM "{event["athena_database"]}"."{event["athena_table"]}"'
        
  
    # Implement manual watermark appraoch:
    elif event['merge_pattern'] == 'APPEND_ONLY' and event['lambda_run_id'] == 0:
        logger.info(f'[LAMBDA LOG] - Performing full_baseline load')
        
        # # Parse result to datetime format and apply buffer
        # datetime_object = datetime.strptime(event['ingest_after'], '%d/%m/%Y')
        
        # #Conver to required format for the specific API
        # last_modified_date = datetime_object.strftime(event["required_format"])
        
        # # Pass watermark into format required for specific API
        # event["query_params"][event["api_filter"]] = event["api_filter_syntax"].replace("<athena_query_result>",last_modified_date)
        logger.info(f'[LAMBDA LOG] - {event["query_params"]}')
    
    # Extract creds
    secretDict = retrieve_api_credentials(event)
    
    logger.info('[LAMBDA LOG] - Successfully retrieved API Credentials')
    
    # Create HTTP session
    session = define_session(event=event,secretDict=secretDict)
    logger.info('[LAMBDA LOG] - Successfully created HTTP session')
    
    url = event['base_url'] + '/' + event['endpoint']

    local_timezone = tz.gettz("Australia/Queensland") # get local time zone
  
    local_time = datetime.now().astimezone(local_timezone)

    local_timestamp_str = local_time.strftime("%Y%m%d%H%M%S")
    
    generic_file_name = event['generic_file_name'] + '_' + local_timestamp_str

    if event['pagination'] == '_NA':
        payload = send_request(event=event, session=session, url=url)
        logger.info('[LAMBDA LOG] - Successfully made HTTP request for full load')
        if event['file_type'] == 'xml':
            logger.info('[LAMBDA LOG] - Processing an XML endpoint')
            file_name = generic_file_name + '.' + event['file_type']
            data = write_to_s3_smart_v2(payload=payload, s3_client=s3, event=event, file_name=file_name)
            
        elif event['file_type'] == 'csv_zip':
            logger.info('[LAMBDA LOG] - Processing a zipped CSV endpoint')
            file_name = generic_file_name + '.' + event['file_type']
            data = write_to_s3_smart_v2(payload=payload, s3_client=s3, event=event, file_name=file_name)

        else:
            logger.info('[LAMBDA LOG] - Processing a JSON endpoint')
            file_name = generic_file_name + '.' + event['file_type']
            # filtered_data = payload['filtered_data']
            # update_dict = payload['update_dict']
            # keys = [*list(filtered_data[0]), *list(update_dict)]
            data = write_to_s3_smart_v2(payload=payload, s3_client=s3, event=event, file_name=file_name)
            
        
    elif event['pagination'] == 'Daisy':
        logger.info('[LAMBDA LOG] - Performing pagination approach: \'Daisy\'')
        data = paginiation_url_daisy(event=event, session=session, url=url, s3_client=s3, key_prefix=key_prefix)
        
    else: 
        logger.info('[LAMBDA LOG] - Unknown pagination type')
        
    logger.info("-------")
    logger.info("[LAMBDA LOG] - Lambda invocation complete, %s second duration" % round((time.time() - start_time),4))
    return data

if __name__ == "__main__":

    run({},{})
#Imports
import sys
import numpy as np
import pandas as pd
import re
from io import StringIO
import boto3
import os
from datetime import datetime, timedelta
import time
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.sql import SparkSession

# Setting sessions and boto3 resources
glueContext = GlueContext(SparkSession.builder.getOrCreate().sparkContext)
csv_buffer = StringIO()
s3_resource = boto3.resource('s3')
s3 = boto3.client('s3')

# Retreiving args and assigning the required values to variables
args = getResolvedOptions(sys.argv,
                          ['source_name',
                          'generic_file_name',
                          'file_path',
                          'curated_bucket_path',
                          'rejection_bucket_path',
                          'ingestion_bucket_path',
                          'processed_path',
                          'file_delimiter',
                          'file_type',
                          'header',
                          'skipfooter',
                          'environment',
                          'dq_job_name',
                          'file_preprocessing',
                          'no_of_files_for_preprocessing'])

source_name = args['source_name']
generic_file_name = args['generic_file_name']
file_path = args['file_path']
dq_job_name = args['dq_job_name']
file_delimiter = args['file_delimiter']
file_type = args['file_type']
file_preprocessing = args['file_preprocessing']
no_of_files_for_preprocessing = args['no_of_files_for_preprocessing']


# Setting the header nd footer info which will be used in parse_csv
if args['header'] == "-1":
  header = None
else:
  header = int(args['header'])

skipfooter = int(args['skipfooter'])

# Setting the locations
# The preprocessed file will be in /preprocessed subdirectory for a given file
# The original files (ingestion bucket/key) will move from /unprocessed to /processed folder after preprocessing is complete
ingestion_bucket, ingestion_key = re.match(r"s3:\/\/(.+?)\/(.+)", file_path).groups()
preprocessed_key = ingestion_key.replace("unprocessed","preprocessed").replace(file_type,"csv")
processed_key  = ingestion_key.replace("unprocessed","processed")

# Prints for debugging
# print('ingestion_key  ' + ingestion_key)
# print('ingestion_bucket  ' + ingestion_bucket)
# print('preprocessed_key :  ' + preprocessed_key)
# print('processed_key :  ' + processed_key)

############  Start of Functions ###############

# This functions accepts a file path and create a csv without header and footer and writes into /preprocessed subdirectory
def parse_csv(file_path,file_delimiter,header,skipfooter):
    df = pd.read_csv(file_path, sep= file_delimiter, engine='python', header=header, skipfooter=skipfooter, index_col=0, encoding='cp1252')
    
    if file_preprocessing == 'append_files':
        # output csv without header
        df.to_csv(csv_buffer,header=False)
    else:
        df.to_csv(csv_buffer)  

    s3_resource.Object(ingestion_bucket, preprocessed_key).put(Body=csv_buffer.getvalue())

# This function is created for files which have preprocessing step as 'append_files'. This is to cater for cases where updates occuring through the week are sent everyday alongwith daily files. This function combines all the updates plus the daily files for day-1 in a single file for ingestion process to pick
def combine_files(subdir, match_string, bucket_name, output_bucket,output_path,filename):
    # Connect to S3 bucket
    s3 = boto3.resource('s3')
    bucket_name = bucket_name
    bucket = s3.Bucket(bucket_name)

    # Get list of matching files in subdirectory
    files = [obj.key for obj in bucket.objects.filter(Prefix=subdir) if match_string in obj.key]
    # Combine contents of matching files
    combined_content = b''
    try:
        for file in files:
            obj = s3.Object(bucket_name, file)
            file_content = obj.get()['Body'].read()
            combined_content += file_content
    except Exception as e:
        print("Error with combining files")
        print(e)

    # Save combined content to new file in S3
    try:
        new_key = f"{subdir}/{filename}"
        bucket.put_object(Key=new_key, Body=combined_content)
        # print('files combined')
    except Exception as e:
        print("Error with moving file into preprocessed subdirectory")
        print(e)
        

# This function is created to move the files from source to target location. IN this job it will be moving files from /unprocessed & /preprocessed folder to /processed folder.For netbi files, there are 2 preprocessing steps. The parsed csv files will be moved from /preprocessed to /processed location after the combined file is created. 
def move_files(source_bucket, source_prefix, dest_bucket, dest_prefix, match_string, file_state):
    s3 = boto3.resource('s3')

    # Get list of matching files in source location
    source_bucket = s3.Bucket(source_bucket)
    source_files = [obj.key for obj in source_bucket.objects.filter(Prefix=source_prefix) if match_string in obj.key]

    # Move matching files to destination location
    dest_bucket_loc = s3.Bucket(dest_bucket)
    for file in source_files:
        try:
            source_path = {'Bucket': source_bucket.name, 'Key': file}
            # print(source_path)
            dest_path = dest_prefix + '/' + file_state + '/' + os.path.basename(file)
            # print(dest_path)
            s3.meta.client.copy(source_path, dest_bucket, dest_path)
        except Exception as e:
            print ("Error with moving files to processed folder")
            print(e)
        try:
            source_key = source_prefix +'/' + os.path.basename(file) 
            # print(source_key)
            dest_bucket_loc.Object(source_key).delete()
            # print("file deleted")
        except Exception as e:
            print ("Error with deleting file: " + source_key)
            print(e)

############  End of Functions ###############

# Generate csv file without header and footer and move to /preprocessed location    
parse_csv (file_path,file_delimiter,header,skipfooter)

preprocessed_file_path = f"s3://{ingestion_bucket}/{preprocessed_key}"
# setting the subdir locations for /unprocessed, /preprocessed and /processed folders
preprocessed_subdir= os.path.dirname(preprocessed_key)
processed_subdir = os.path.dirname(processed_key)
ingest_subdir = os.path.dirname(ingestion_key)

# Prints for debugging
# print('preprocessed_file_path :  ' + preprocessed_file_path)
# print( 'preprocessed_subdir: ', preprocessed_subdir)
# print('processed_subdir: ', processed_subdir)

# Run the below block only for NetBI files which requires multiple daily files for same file type to combine into single file.
if file_preprocessing == 'append_files' and source_name == 'netbi':
    # retreiving the mod date abnd file date from the filename
    try:
        file_name = preprocessed_key.split('/')[-1]
        filedatestr = file_name.split('_')[-2]
        filedate = datetime.strptime(filedatestr,'%Y%m%d').date()
        moddatestr = file_name.split('_')[-1].split('.')[0][3:11]
        moddate = datetime.strptime(moddatestr,'%Y%m%d').date()
        print ('moddate:  ')
        print(moddate)
        print('filedate:  ')
        print(filedate)
    except Exception as e:
        print ("Error with extracting Filedate and modedate from Filename: " + preprocessed_key)
        print(e)
    # The below logic will only processed with combining the files if all files are received for the day and will only kick off when this glue job gets triggered when the latest daily file is received. This is to prevent the file merge operation to occur with every "update file" that is sent with daily file. This ensures that the files are only combined once when all files are received. For some netbi files the latest filedate and moddate difference is 2 days
    if int(no_of_files_for_preprocessing) == 7:
        date_diff =2
    else:
        date_diff = 1
        

    if moddate == filedate + timedelta(days = date_diff):
            # Wait till all files are received before combining the files
            while True:
                print("Continue processing when all files received")
                try:
                    response = s3.list_objects_v2(Bucket=ingestion_bucket, Prefix = preprocessed_subdir, MaxKeys=1000)
                    search_str = 'mod'+ moddatestr
                    files = response.get('Contents', [])
                    matching_files = []
                    for obj in files:
                        if search_str in obj['Key']:
                            matching_files.append(obj['Key'])
                    no_of_files_matched = len(matching_files)      
                    print('no of files matched')
                    print(len(matching_files))
                except Exception as e:
                    print(e)
                # when all expected files are received, combine the contents in one file into preprocessed folder and move the original files into processed folder.   
                if no_of_files_matched == int(no_of_files_for_preprocessing):
                    print("Expected total file count matched")
                    merged_file_name = generic_file_name +'_'+ moddatestr + '.csv'
                    preprocessed_file_path = f"s3://{ingestion_bucket}/{preprocessed_subdir}/{merged_file_name}"
                    combine_files(preprocessed_subdir, search_str, ingestion_bucket, ingestion_bucket,preprocessed_subdir,merged_file_name)
                    
                    # once combined file is generated, move original files from preprocessed and unprocessed folder to processed folder
                    move_files(ingestion_bucket, preprocessed_subdir, ingestion_bucket, processed_subdir, search_str, 'parsed')
                    print("Original files moved from preprocessed folder")
                    move_files(ingestion_bucket, ingest_subdir, ingestion_bucket, processed_subdir, search_str, 'original')
                    print("Original files moved from unprocessed folder")
                    
                    # Trigger the glue job for DQ checks. Note that the header is set to -1 as it is stripped off in the combined file
                    try:
                        client = boto3.client('glue')
                        response = client.start_job_run(
                                      JobName = dq_job_name,
                                      Arguments = {
                                         '--source_name': source_name,
                                         '--generic_file_name': generic_file_name,
                                         '--file_path': preprocessed_file_path,
                                         '--curated_bucket_path': args['curated_bucket_path'],
                                         '--rejection_bucket_path': args['rejection_bucket_path'],
                                         '--ingestion_bucket_path': args['ingestion_bucket_path'],
                                         '--processed_path':args['processed_path'],
                                         '--header': "-1"})
                    except Exception as e:
                        print("Error kicking off DQ Glue job")
                        print(e)
                    break
                else:
                    print('Total NetBI files received for preprocessing')
                    print(no_of_files_matched)
                    # If all files are not received, wait for some time (20 sec ) and try again
                    time.sleep(20)
    else:
        # This is the "updates files", leave the files in preprocessed folder after parse_csv
        print("Not the latest file, do nothing")

# For all other files which are not NetBI and doesn't have a preprocessing step of append_files
else:
    client = boto3.client('glue')
    response = client.start_job_run(
                  JobName = dq_job_name,
                  Arguments = {
                     '--source_name': source_name,
                     '--generic_file_name': generic_file_name,
                     '--file_path': preprocessed_file_path,
                     '--curated_bucket_path': args['curated_bucket_path'],
                     '--rejection_bucket_path': args['rejection_bucket_path'],
                     '--ingestion_bucket_path': args['ingestion_bucket_path'],
                     '--processed_path':args['processed_path'],
                     '--header':args['header']})
    
    copy_file = {"Bucket": ingestion_bucket,"Key": ingestion_key} 
    s3_resource.meta.client.copy(copy_file, ingestion_bucket,processed_key)
    s3_resource.Object(ingestion_bucket, ingestion_key).delete()


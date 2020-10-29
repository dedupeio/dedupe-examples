from __future__ import print_function
import re
import boto3
import botocore
import sys
import datetime
import os
import time
import pandas as pd
from six import string_types
import sys
pyver = sys.version_info[0]

if pyver<3:
    from StringIO import StringIO as SomethingIO
    from urlparse import urlparse
else:
    from io import BytesIO as SomethingIO
    from urllib.parse import urlparse
    
sys.path.insert(0, '../athena_example/')
import config

s3 = boto3.client('s3', region_name=config.REGION, 
                      aws_access_key_id=config.ACCESS_KEY_ID, aws_secret_access_key=config.SECRET_ACCESS_KEY)
  
athena = boto3.client('athena', region_name=config.REGION, 
                      aws_access_key_id=config.ACCESS_KEY_ID, aws_secret_access_key=config.SECRET_ACCESS_KEY)

def athena_to_panda(query, database=config.DATABASE, output_location=config.ATHENA_GARBAGE_PATH, region=config.REGION, workgroup=config.WORKGROUP, **kwargs):
    query_execution_id = athena_start_query(query, database, output_location, region, workgroup, wait_until_finished=True)
    df = pandas_read_csv(os.path.join(output_location, query_execution_id+'.csv'), **kwargs)
    return df


def athena_start_query(query, database=config.DATABASE, output_location=config.ATHENA_GARBAGE_PATH, region=config.REGION, workgroup=config.WORKGROUP, wait_until_finished=True):
    query_execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },    
        WorkGroup=workgroup,
        ResultConfiguration={
            "OutputLocation": output_location
        }
    )['QueryExecutionId']

    seconds_to_wait = 1

    if wait_until_finished:
        while True:
            time.sleep(seconds_to_wait)
            seconds_to_wait += 1
#             seconds_to_wait *= 2

            execution = athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            if execution['QueryExecution']['Status']['State'] not in ['QUEUED', 'RUNNING']:
                break

        if execution['QueryExecution']['Status']['State'] != 'SUCCEEDED':
            raise Exception("Athena query failed: %s" % ( execution['QueryExecution']['Status']['StateChangeReason'],), query_execution_id)

    return query_execution_id

# Copied from https://github.com/pandas-dev/pandas/blob/master/pandas/io/common.py
# Import it instead, when it's updated.
def is_s3_url(url):
    """Check for an s3, s3n, or s3a url"""
    try:
        return urlparse(url).scheme in ["s3", "s3n", "s3a"]
    except Exception:
        return False
    
def seperate_bucket_key(url):
    m = re.match('s3://([^/]+)/(.*)', url)
    return m.group(1), m.group(2)

def list_all(path):
    if is_s3_url(path):
        bucket, key = seperate_bucket_key(path)
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        return [key['Key'] for key in objects['Contents']]
    from os import listdir
    from os.path import isfile, join
    return listdir(path)
    

def pandas_read_csv(filepath_or_buffer, verbose=True, **kwargs):
    bucket, key = seperate_bucket_key(filepath_or_buffer)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(SomethingIO(obj['Body'].read()),  **kwargs)

def read(filename, verbose=True):
    log ("Reading {}".format(filename), verbose=verbose)
    if is_s3_url(filename):
        bucket, key = seperate_bucket_key(filename)
        obj=s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read()
    with open (filename) as f:
        return f.read()

def write(body, filename):
    bucket, key = seperate_bucket_key(filename)
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return
        
    
def file_exists(filename):
    bucket, key = seperate_bucket_key(filename)
    try:
        s3.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code']=='NoSuchKey':
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        return True
    
    
def log(outstr, logfile_name=config.LOG_FILE, timestamped=True, verbose=True, quiet=False):
    if verbose == False:
        return
    if timestamped:
        outstr = "[%s]\t%s\n" % (str(datetime.datetime.now()) , outstr)
    else:
        outstr = "%s\n" % (outstr,)

    with open(logfile_name, "a") as logfile:
        logfile.write(outstr)

    if not quiet:
        sys.stdout.write(outstr);
        sys.stdout.flush()
# Print iterations progress

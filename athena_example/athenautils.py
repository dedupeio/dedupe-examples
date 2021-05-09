from __future__ import print_function
import config
import re
import boto3
import botocore
import sys
import os
import time
from os import listdir
import shutil
import pandas as pd


pyver = sys.version_info[0]

if pyver < 3:
    from StringIO import StringIO as SomethingIO
    from urlparse import urlparse
else:
    from io import BytesIO as SomethingIO
    from urllib.parse import urlparse

sys.path.insert(0, "../athena_example/")

s3 = boto3.client(
    "s3",
    region_name=config.REGION,
    aws_access_key_id=config.ACCESS_KEY_ID,
    aws_secret_access_key=config.SECRET_ACCESS_KEY,
)

athena = boto3.client(
    "athena",
    region_name=config.REGION,
    aws_access_key_id=config.ACCESS_KEY_ID,
    aws_secret_access_key=config.SECRET_ACCESS_KEY,
)


def cursor_execute(
    query,
    database=None,
    cursortype="tuple",
    buffersize=1000000,
    output_location=config.ATHENA_GARBAGE_PATH,
    region=config.REGION,
    workgroup=config.WORKGROUP,
    **kwargs
):

    kwargs["chunksize"] = buffersize
    df_cur = athena_to_panda(
        query,
        database=database,
        output_location=output_location,
        region=region,
        workgroup=workgroup,
        **kwargs
    )
    for df in df_cur:
        if cursortype == "dict":
            all_rows = df.where(pd.notnull(df), None).to_dict("records")
        if cursortype == "tuple":
            all_rows = df.where(pd.notnull(df), None).itertuples(index=False, name=None)
        for row in all_rows:
            yield row


def athena_to_panda(
    query,
    database=None,
    output_location=config.ATHENA_GARBAGE_PATH,
    region=config.REGION,
    workgroup=config.WORKGROUP,
    **kwargs
):
    query_execution_id = athena_start_query(
        query,
        database=database,
        output_location=output_location,
        region=region,
        workgroup=workgroup,
        wait_until_finished=True,
    )
    df = pandas_read_csv(
        os.path.join(output_location, query_execution_id + ".csv"), **kwargs
    )
    return df


def athena_start_query(
    query,
    database=None,
    output_location=config.ATHENA_GARBAGE_PATH,
    region=config.REGION,
    workgroup=config.WORKGROUP,
    wait_until_finished=True,
):
    query_execution_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
        ResultConfiguration={"OutputLocation": output_location},
    )["QueryExecutionId"]

    seconds_to_wait = 1

    if wait_until_finished:
        while True:
            time.sleep(seconds_to_wait)
            seconds_to_wait += 1
            #             seconds_to_wait *= 2

            execution = athena.get_query_execution(QueryExecutionId=query_execution_id)

            if execution["QueryExecution"]["Status"]["State"] not in [
                "QUEUED",
                "RUNNING",
            ]:
                break

        if execution["QueryExecution"]["Status"]["State"] != "SUCCEEDED":
            raise Exception(
                "Athena query failed: %s"
                % (execution["QueryExecution"]["Status"]["StateChangeReason"],),
                query_execution_id,
            )

    return query_execution_id


# Copied from
# https://github.com/pandas-dev/pandas/blob/master/pandas/io/common.py
# Import it instead, when it's updated.


def is_s3_url(url):
    """Check for an s3, s3n, or s3a url"""
    try:
        return urlparse(url).scheme in ["s3", "s3n", "s3a"]
    except Exception:
        return False


def seperate_bucket_key(url):
    m = re.match("s3://([^/]+)/(.*)", url)
    return m.group(1), m.group(2)


def list_all(path):
    if is_s3_url(path):
        bucket, key = seperate_bucket_key(path)
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        if "Contents" not in objects:
            return []
        return [key["Key"] for key in objects["Contents"]]
    if not os.path.exists(path):
        return []
    return listdir(path)


def del_all_files(path):
    filelist = list_all(path)
    if is_s3_url(path):
        bucket, key = seperate_bucket_key(path)
        for f in filelist:
            s3.delete_object(Bucket=bucket, Key=f)
        return
    filelist = [os.path.join(path, f) for f in filelist]
    for f in filelist:
        if os.path.isfile(f):
            os.remove(f)
        else:
            shutil.rmtree(f)


def drop_external_table(
    tablename,
    location,
    database=None,
    output_location=config.ATHENA_GARBAGE_PATH,
    region=config.REGION,
    workgroup=config.WORKGROUP,
):
    athena_start_query(
        "drop table if exists {}".format(tablename),
        database=database,
        output_location=output_location,
        region=region,
        workgroup=workgroup,
    )
    del_all_files(location)


def pandas_read_csv(filepath_or_buffer, **kwargs):
    bucket, key = seperate_bucket_key(filepath_or_buffer)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(SomethingIO(obj["Body"].read()), **kwargs)


def read(filename):
    if is_s3_url(filename):
        bucket, key = seperate_bucket_key(filename)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()
    with open(filename) as f:
        return f.read()


def write(body, filename):
    bucket, key = seperate_bucket_key(filename)
    s3.put_object(Bucket=bucket, Key=key, Body=body)
    return


def file_name_append(filename, append, ommitext):
    filename_base, ext = os.path.splitext(filename)
    if ommitext:
        return "%s%s" % (filename_base, append)
    return "%s%s%s" % (filename_base, append, ext)


def write_many(read_cursor, filename, buffersize=config.BUFFERSIZE):
    chunkcount = 0
    while True:
        buffer_df = pd.DataFrame.from_records(read_cursor, nrows=buffersize)
        if buffer_df.empty:
            break
        buffer = buffer_df.to_csv(index=False, header=False, sep="\t")
        chunk_fname = file_name_append(
            filename, "_{}".format(chunkcount), ommitext=False
        )
        write(buffer, chunk_fname)
        chunkcount += 1


def file_exists(filename):
    bucket, key = seperate_bucket_key(filename)
    try:
        s3.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            return False
        else:
            # Something else has gone wrong.
            raise
    else:
        return True

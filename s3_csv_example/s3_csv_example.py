#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe with a comma separated values
(CSV) file. All operations are performed in memory, so will run very
quickly on datasets up to ~10,000 rows.

We start with a CSV file containing our messy data. In this example,
it is listings of early childhood education centers in Chicago
compiled from several different sources.

The output will be a CSV with our clustered results.

For larger datasets, see our [mysql_example](mysql_example.html)
"""

import os
import csv
import re
import logging
import optparse
import sys

import dedupe
from unidecode import unidecode


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    column = unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    # If data is missing, indicate that by setting the value to `None`
    if not column:
        column = None
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records,
    where the key is a unique record ID and each value is dict
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for row in reader:
            clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
            row_id = int(row['Id'])
            data_d[row_id] = dict(clean_row)

    return data_d


if __name__ == '__main__':

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. This
    # code block lets you change the level of loggin on the command
    # line. You don't need it if you don't want that. To enable verbose
    # logging, run `python examples/csv_example/csv_example.py -v`
    optp = optparse.OptionParser()
    optp.add_option('-v', '--verbose', dest='verbose', action='count',
                    help='Increase verbosity (specify multiple times for more)'
                    )
    (opts, args) = optp.parse_args()
    log_level = logging.WARNING
    if opts.verbose:
        if opts.verbose == 1:
            log_level = logging.INFO
        elif opts.verbose >= 2:
            log_level = logging.DEBUG
    logging.getLogger().setLevel(log_level)

    # ## Setup

    input_file = 's3_csv_example_messy_input.csv'
    output_file = 's3_csv_example_output.csv'
    settings_file = 's3_csv_example_learned_settings'
    training_file = 's3_csv_example_training.json'

    scriptpath = os.path.dirname(__file__)
    input_file = os.path.join(scriptpath, input_file)
    #output_file = os.path.join(scriptpath, output_file)
    settings_file = os.path.join(scriptpath, settings_file)
    training_file = os.path.join(scriptpath, training_file)

    import sys
    if len(sys.argv) > 1:
        bucket = sys.argv[1]
    if len(sys.argv) > 1:
        output_bucket = sys.argv[2]
    
    s3files = []

    import boto3
    s3_client = boto3.client('s3')
    result = s3_client.list_objects(Bucket = bucket, Prefix='')
    for o in result.get('Contents'):
        filename = o.get('Key');
        print(filename)
        data = s3_client.get_object(Bucket=bucket, Key=filename)
        if filename[:2] == 's3':
            input_file = filename
            s3_client.download_file(bucket,filename,filename)
            s3files.append(filename)
            response = s3_client.delete_object(
                Bucket=bucket,
                Key=filename)

    print('importing data ...')
    data_d = {}
    for eachFile in s3files:
        data_d.update(readData(eachFile))

    if not data_d:
        print('no files found to process in s3 bucket')
        os._exit(1)

    # If a settings file already exists, we'll just load that and skip training
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        # ## Training

        # Define the fields dedupe will pay attention to
        fields = [
            {'field': 'Site name', 'type': 'String'},
            {'field': 'Address', 'type': 'String'},
            {'field': 'Zip', 'type': 'Exact', 'has missing': True},
            {'field': 'Phone', 'type': 'String', 'has missing': True},
            ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields)

        # If we have training data saved from a previous run of dedupe,
        # look for it and load it in.
        # __Note:__ if you want to train from scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file, 'rb') as f:
                deduper.prepare_training(data_d, f)
        else:
            deduper.prepare_training(data_d)

        # ## Active learning
        # Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.
        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        print('starting active labeling...')

        dedupe.console_label(deduper)

        # Using the examples we just labeled, train the deduper and learn
        # blocking predicates
        deduper.train()

        # When finished, save our training to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

    # ## Clustering

    # `partition` will return sets of records that dedupe
    # believes are all referring to the same entity.

    print('clustering...')
    clustered_dupes = deduper.partition(data_d, 0.5)

    print('# duplicate sets', len(clustered_dupes))

    # ## Writing Results

    # Write our original data back out to a CSV with a new column called
    # 'Cluster ID' which indicates which records refer to each other.

    cluster_membership = {}
    for cluster_id, (records, scores) in enumerate(clustered_dupes):
        for record_id, score in zip(records, scores):
            cluster_membership[record_id] = {
                "Cluster ID": cluster_id,
                "confidence_score": score
            }

    with open(output_file, 'w') as f_output, open(input_file) as f_input:

        reader = csv.DictReader(f_input)
        fieldnames = ['Cluster ID', 'confidence_score'] + reader.fieldnames

        writer = csv.DictWriter(f_output, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row_id = int(row['Id'])
            row.update(cluster_membership[row_id])
            writer.writerow(row)
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(output_file, output_bucket, output_file)
    os.remove(output_file)


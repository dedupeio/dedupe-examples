#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use some extended dedupe variables 
"""
from future.builtins import next

import os
import csv
import re
import logging
import optparse

import dedupe
from unidecode import unidecode

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 
if opts.verbose :
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)


# ## Setup

input_file = 'officers.csv'
output_file = 'officers_output.csv'
settings_file = 'officers_settings'
training_file = 'officers_training.json'


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    import unidecode
    #column = column.decode("utf8")
    column = unidecode.unidecode(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
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
            clean_row = {k: preProcess(v) for (k, v) in row.items()}
            clean_row['name'] = ' '.join([clean_row['FirstName'], 
                                          clean_row['LastName']])
            if not clean_row['name'] :
                clean_row['name'] = None
            clean_row['address'] = ' '.join([clean_row['Address1'], 
                                             clean_row['Address2']])
            if not clean_row['address'] :
                clean_row['address'] = None
            row_id = int(row['ID'])

            for k, v in clean_row.items():
                if not v:
                    clean_row[k] = None

            data_d[row_id] = clean_row

    return data_d


print('importing data ...')
data_d = readData(input_file)

# ## Training

if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field. 
    fields = [
        {'field' : 'name', 'type': 'Name', 
         'crf' : True},
        {'field' : 'address', 'type': 'Address',
         'crf' : True},
        {'field' : 'City', 'type' : 'ShortString'},
        {'field' : 'State', 'type' : 'ShortString'},
        {'field' : 'Zip', 'type' : 'ShortString'},
        {'field' : 'Phone', 'type' : 'ShortString'},
        {'field' : 'RedactionRequested', 'type' : 'Categorical',
         'categories' : ['true', 'false']}]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file, 'rb') as f:
            deduper.prepare_training(data_d, training_file=f)
    else:
        deduper.prepare_training(data_d)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('starting active labeling...')

    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'wb') as sf :
        deduper.writeSettings(sf)


# ## Blocking

print('blocking...')

# ## Clustering

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print('clustering...')
clustered_dupes = deduper.match(data_d, threshold=0.5)

print('# duplicate sets', len(clustered_dupes))

# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = 0
for (cluster_id, cluster) in enumerate(clustered_dupes):
    id_set, scores = cluster
    cluster_d = [data_d[c] for c in id_set]
    for record_id, score in zip(id_set, scores) :
        cluster_membership[record_id] = {
            "cluster id" : cluster_id,
            "confidence": score
        }

singleton_id = cluster_id + 1

with open(output_file, 'w') as f_output:
    writer = csv.writer(f_output)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = next(reader)
        heading_row.insert(0, 'confidence_score')
        heading_row.insert(0, 'Cluster ID')
        
        writer.writerow(heading_row)

        for row in reader:
            row_id = int(row[0])
            if row_id in cluster_membership :
                cluster_id = cluster_membership[row_id]["cluster id"]
                row.insert(0, cluster_membership[row_id]['confidence'])
                row.insert(0, cluster_id)
            else:
                row.insert(0, None)
                row.insert(0, singleton_id)
                singleton_id += 1
            writer.writerow(row)

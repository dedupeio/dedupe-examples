#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates the Gazetteer.

We will use one of the sample files from the RecordLink example as the
canonical set.

"""
from __future__ import print_function

import os
import csv
import re
import logging
import optparse
import random

import dedupe
from unidecode import unidecode

# ## Logging

# dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose logging, run `python
# examples/csv_example/csv_example.py -v`
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

output_file = 'gazetteer_output.csv'
settings_file = 'gazetteer_learned_settings'
training_file = 'gazetteer_training.json'


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """

    column = unidecode(column)
    column = re.sub('\n', ' ', column)
    column = re.sub('-', '', column)
    column = re.sub('/', ' ', column)
    column = re.sub("'", '', column)
    column = re.sub(",", '', column)
    column = re.sub(":", ' ', column)
    column = re.sub(' +', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    if not column:
        column = None
    return column


def readData(filename):
    """
    Read in our data from a CSV file and create a dictionary of records, 
    where the key is a unique record ID.
    """

    data_d = {}

    with open(filename) as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = dict([(k, preProcess(v)) for (k, v) in row.items()])
            if clean_row['price']:
                clean_row['price'] = float(clean_row['price'][1:])
            data_d[filename + str(i)] = dict(clean_row)

    return data_d


print('importing data ...')
messy = readData('AbtBuy_Abt.csv')
print('N data 1 records: {}'.format(len(messy)))

canonical = readData('AbtBuy_Buy.csv')
print('N data 2 records: {}'.format(len(canonical)))


def descriptions():
    for dataset in (messy, canonical):
        for record in dataset.values():
            yield record['description']


if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as sf:
        gazetteer = dedupe.StaticGazetteer(sf)

else:
    # Define the fields the gazetteer will pay attention to
    #
    # Notice how we are telling the gazetteer to use a custom field comparator
    # for the 'price' field.
    fields = [
        {'field': 'title', 'type': 'String'},
        {'field': 'title', 'type': 'Text', 'corpus': descriptions()},
        {'field': 'description', 'type': 'Text',
         'has missing': True, 'corpus': descriptions()},
        {'field': 'price', 'type': 'Price', 'has missing': True}]

    # Create a new gazetteer object and pass our data model to it.
    gazetteer = dedupe.Gazetteer(fields)
    # To train the gazetteer, we feed it a sample of records.
    # Gazetteer inherits sample from RecordLink
    gazetteer.sample(messy, canonical, 15000)

    # If we have training data saved from a previous run of gazetteer,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf:
            gazetteer.readTraining(tf)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as matches
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('starting active labeling...')

    dedupe.consoleLabel(gazetteer)

    gazetteer.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf:
        gazetteer.writeTraining(tf)

    # Make the canonical set
    gazetteer.index(canonical)
    
    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'wb') as sf:
        gazetteer.writeSettings(sf, index=True)

    gazetteer.cleanupTraining()

gazetteer.index(canonical)
# Calc threshold
print('Start calculating threshold')
threshold = gazetteer.threshold(messy, recall_weight=1.0)
print('Threshold: {}'.format(threshold))


results = gazetteer.match(messy, threshold=threshold, n_matches=1, generator=True)

cluster_membership = {}
cluster_id = None
for cluster_id, (row,) in enumerate(results):
    cluster, score = row
    for record_id in cluster:
        cluster_membership[record_id] = (cluster_id, score)

if cluster_id :
    unique_id = cluster_id + 1
else :
    unique_id =0
    

with open(output_file, 'w') as f:
    writer = csv.writer(f)
    
    header_unwritten = True

    for fileno, filename in enumerate(('AbtBuy_Abt.csv', 'AbtBuy_Buy.csv')) :
        with open(filename) as f_input :
            reader = csv.reader(f_input)

            if header_unwritten :
                heading_row = next(reader)
                heading_row.insert(0, 'source file')
                heading_row.insert(0, 'Link Score')
                heading_row.insert(0, 'Cluster ID')
                writer.writerow(heading_row)
                header_unwritten = False
            else :
                next(reader)

            for row_id, row in enumerate(reader):
                cluster_details = cluster_membership.get(filename + str(row_id))
                if cluster_details is None :
                    cluster_id = unique_id
                    unique_id += 1
                    score = None
                else :
                    cluster_id, score = cluster_details
                row.insert(0, filename)
                row.insert(0, score)
                row.insert(0, cluster_id)
                writer.writerow(row)

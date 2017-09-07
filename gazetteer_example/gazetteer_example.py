#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates the Gazetteer. It is derived from the RecordLink example.

We will use one of the sample files from the RecordLink example as the canonical set.
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

# dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`
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

output_file = 'data_matching_output.csv'
settings_file = 'data_matching_learned_settings'
training_file = 'data_matching_training.json'


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
data_1 = readData('AbtBuy_Abt.csv')
print('N data 1 records: {}'.format(len(data_1)))

data_2 = readData('AbtBuy_Buy.csv')
print('N data 2 records: {}'.format(len(data_2)))


def descriptions():
    for dataset in (data_1, data_2):
        for record in dataset.values():
            yield record['description']


# Training ---------------------------------------------------------------------------------------
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
    gazetteer.sample(data_1, data_2, 15000)

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

    gazetteer.train(index_predicates=False)

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf:
        gazetteer.writeTraining(tf)

    # Make the canonical set
    gazetteer.index(data_1)
    
    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'wb') as sf:
        gazetteer.writeSettings(sf, index=True)

    gazetteer.cleanupTraining()

# Calc threshold
print('Start calculating threshold')
threshold = gazetteer.threshold(data_1, recall_weight=2.0)
print('Threshold: {}'.format(threshold))

# Try some matches
matched = []
not_matched = []
for s_key in random.sample(data_2.keys(), 10):
    values = data_2[s_key]
    unique_id = values['unique_id']
    del values['unique_id']
    messy_data = {unique_id: values}
    print(values)

    try:
        results = gazetteer.match(messy_data, threshold=threshold)
    except ValueError:
        results = None
        not_matched.append(messy_data)

    if results:
        key = results[0][0][0][1]

        # ignore added records because in this example, the csv file can get out of sync with
        # the settings file.
        if not key.startswith('added'):
            print(data_1[key])
            print('score: {}'.format(results[0][0][1]))
            matched.append(messy_data)
    else:
        print('No match')

    print('---------------------')


# Add the not matches and try to match. This simulates something like a mailing list. If there is an
# address that is not a match, add it to the list. Next time it appears it will be a match.
new_data = {}
for nd in not_matched:
    for unique_id in nd:
        new_data['added_{}'.format(unique_id)] = nd[unique_id]

gazetteer.index(new_data)

# Confirm that the old matches still work
for a_match in matched:
    # This will crash if there is not a match
    results = gazetteer.match(a_match, threshold=threshold)

# Check that new data now matches
for a_match in not_matched:
    # This will crash if there is not a match
    results = gazetteer.match(a_match, threshold=threshold)


# Reload gazetteer and show that the not_matches do not match any more
with open(settings_file, 'rb') as sf:
    gazetteer = dedupe.StaticGazetteer(sf)


for a_match in not_matched:
    # This will crash if there is not a match
    try:
        results = gazetteer.match(a_match, threshold=threshold)
        print('***ERROR: should not match')
    except ValueError:
        pass


# Add the new data again and write results
gazetteer.index(new_data)
with open(settings_file, 'wb') as sf:
    gazetteer.writeSettings(sf, index=True)


# Reload gazetteer and show that the added matches are still matches
with open(settings_file, 'rb') as sf:
    gazetteer = dedupe.StaticGazetteer(sf)


for a_match in not_matched:
    # This will crash if there is not a match
    results = gazetteer.match(a_match, threshold=threshold)


print('Done: everything worked!')

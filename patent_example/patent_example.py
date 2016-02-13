#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe to disambiguate patent
authors and demonstrates the Set and LatLong data types.

"""
from __future__ import print_function
from future.builtins import next
from future.utils import viewvalues

import os
import csv
import re
import collections
import logging
import optparse
import math

import dedupe
from unidecode import unidecode

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added
# for convenience.  To enable verbose logging, run `python
# patent_example.py -v`

optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING 

if opts.verbose :
    if opts.verbose == 1 :
        log_level = logging.INFO
    elif opts.verbose > 1 :
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)


def readData(filename, set_delim='**'):
    """
    Remap columns for the following cases:
    - Lat and Long are mapped into a single LatLong tuple
    - Class and Coauthor are stored as delimited strings but mapped into 
      tuples
    """

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            row = dict((k, v.lower()) for k, v in row.items())
            if row['Lat'] == row['Lng'] == '0.0' :
                row['LatLong'] = None
            else :
                row['LatLong'] = (float(row['Lat']), float(row['Lng']))
            row['Class'] = tuple(sorted(row['Class'].split(set_delim))) if row['Class'] else None
            row['Coauthor'] = tuple(sorted([author for author
                                            in row['Coauthor'].split(set_delim)
                                            if author != 'none']))
            if row['Name'] == '' :
                row['Name'] = None
            
            data_d[idx] = row

    return data_d


def philips(field_1, field_2) :
    if 'philips' in field_1 and 'philips' in field_2 :
        return 1
    else :
        return 0


# These two generators will give us the corpora setting up the Set
# distance metrics
def classes(data) :
    for record in viewvalues(data) :
        yield record['Class']

def coauthors(data) :
    for record in viewvalues(data) :
        yield record['Coauthor']

def names(data) :
    for record in viewvalues(data) :
        yield record['Name']

input_file = 'patstat_input.csv'
output_file = 'patstat_output.csv'
settings_file = 'patstat_settings.json'
training_file = 'patstat_training.json'

print('importing data ...')
data_d = readData(input_file)

# ## Training

if os.path.exists(settings_file):
    print('reading from', settings_file)
    with open(settings_file, 'rb') as sf :
        deduper = dedupe.StaticDedupe(sf, num_cores=2)

else:
    # Define the fields dedupe will pay attention to
    fields = [
        {'field' : 'Name', 
         'variable name' : 'Name',
         'type': 'String', 
         'has missing' : True},
        {'field' : 'Name',
         'type' : 'Custom', 
         'comparator' : philips},
        {'field' : 'LatLong', 
         'type' : 'LatLong', 
         'has missing' : True},
        {'field' : 'Class', 
         'variable name' : 'Class',
         'type': 'Set', 
         'corpus' : classes(data_d),
         'has missing' : True},
        {'field' : 'Coauthor', 
         'variable name' : 'Coauthor',
         'type': 'Set', 
         'corpus' : coauthors(data_d),
         'has missing' : True},
        {'field' : 'Name',
         'variable name' : 'Name Text',
         'type' : 'Text',
         'corpus' : names(data_d),
         'has missing' : True},
        {'type' : 'Interaction',
         'interaction variables' : ['Name', 'Name Text']}
    ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=2)

    # To train dedupe, we feed it a sample of records.
    deduper.sample(data_d, 10000)
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    if os.path.exists(training_file):
        print('reading labeled examples from ', training_file)
        with open(training_file) as tf :
            deduper.readTraining(tf)

    # ## Active learning

    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('starting active labeling...')
    dedupe.consoleLabel(deduper)

    deduper.train(uncovered_dupes=5, ppc=0.01)

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'wb') as sf :
        deduper.writeSettings(sf)

clustered_dupes = deduper.match(data_d, 0.5)

print('# duplicate sets', len(clustered_dupes))

# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = 0
for cluster_id, (cluster, scores) in enumerate(clustered_dupes):
    for record_id, score in zip(cluster, scores):
        cluster_membership[record_id] = (cluster_id, score)

unique_id = cluster_id + 1

with open(output_file, 'w') as f_out, open(input_file) as f_in :
    writer = csv.writer(f_out)
    reader = csv.reader(f_in)

    heading_row = next(reader)
    heading_row.insert(0, 'Score')
    heading_row.insert(0, 'Cluster ID')
    writer.writerow(heading_row)

    for row_id, row in enumerate(reader):
        if row_id in cluster_membership:
            cluser_id, score = cluster_membership[row_id]
        else:
            cluster_id, score = unique_id, None
            unique_id += 1
        row.insert(0, score)
        row.insert(0, cluster_id)
        writer.writerow(row)


#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
This code demonstrates how to use dedupe to disambiguate patent
authors and demonstrates the Set and LatLong data types.

"""

import os
import csv
import re
import collections
import logging
import optparse
import math

import dedupe

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
if opts.verbose == 1:
    log_level = logging.INFO
elif opts.verbose >= 2:
    log_level = logging.DEBUG
logging.basicConfig(level=log_level)


# ## Setup

input_file = 'patstat_input.csv'
output_file = 'patstat_output.csv'
settings_file = 'patstat_settings.json'
training_file = 'patstat_training.json'


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of
    [AsciiDammit](https://github.com/tnajdek/ASCII--Dammit) and
    Regex. Things like casing, extra spaces, quotes and new lines can
    be ignored.
    """

    column = dedupe.asciiDammit(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column


def readData(filename, set_delim='**'):
    """
    Remap columns for the following cases:
    - Lat and Long are mapped into a single LatLong tuple
    - Class and Coauthor are stored as delimited strings but mapped into 
      tuples
    """

    word_count = collections.defaultdict(int)
    all_words = 0.0

    data_d = {}
    with open(filename) as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            for k in row:
                 row[k] = preProcess(row[k])
            row['LatLong'] = (float(row['Lat']), float(row['Lng']))
            del row['Lat']
            del row['Lng']
            row['Class'] = tuple(row['Class'].split(set_delim))
            row['Coauthor'] = tuple([author for author
                                     in row['Coauthor'].split(set_delim)
                                     if author != 'none'])
            
            if not row['Name'] :
                word_count[''] += 1
                all_words += 1

            for word in row['Name'].split() :
                word_count[word] += 1
                all_words += 1
            
            data_d[idx] = row

    for word in word_count :
        word_count[word] /= all_words

    for idx, record in data_d.items() :
        name_prob = 1
        if not record['Name'] :
            name_prob *= word_count['']

        for word in record['Name'].split() :
            name_prob *= word_count[word]
        record['Name Probability'] = name_prob
        
    return data_d

# With this feature we are trying to control for the fact that
# some inventors appear hundreds of times.
def name_probability_log_odds(field_1, field_2) :
    phi = field_1 * field_2
    return math.log(phi/(1-phi))

print 'importing data ...'
data_d = readData(input_file)

# These two generators will give us the corpora setting up the Set
# distance metrics
def classes() :
    for record in data_d.itervalues() :
        yield record['Class']

def coauthors() :
    for record in data_d.itervalues() :
        yield record['Coauthor']

# ## Training

if os.path.exists(settings_file):
    print 'reading from', settings_file
    with open(settings_file) as sf :
        deduper = dedupe.StaticDedupe(sf)

else:
    # Define the fields dedupe will pay attention to
    fields = [
        {'field' : 'Name', 
         'variable name' : 'Name',
         'type': 'String', 
         'Has Missing':True},
        {'field' : 'LatLong','type' : 'LatLong', 'Has Missing' : True},
        {'field' : 'Class', 
         'variable name' : 'Class',
         'type': 'Set', 
         'corpus' : classes()},
        {'field' : 'Coauthor', 
         'variable name' : 'Coauthor',
         'type': 'Set', 
         'corpus' : coauthors()},
        {'field' : 'Name Probability', 
         'variable name' : 'NameProb',
         'type' : 'Custom', 
         'comparator' : name_probability_log_odds},
        {'type' : 'Interaction', 
         'Interaction Fields' : ['NameProb', 
                                 'Class']},
        {'type' : 'Interaction', 
         'Interaction Fields' : ['NameProb', 
                                 'Name']},
        {'type' : 'Interaction', 
         'Interaction Fields' : ['NameProb', 
                                 'Coauthor']}
    ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # To train dedupe, we feed it a random sample of records.
    deduper.sample(data_d, 60000)
    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        with open(training_file) as tf :
            deduper.readTraining(tf)
    # ## Active learning

    # Starts the training loop. Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.

    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print 'starting active labeling...'
    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    with open(training_file, 'w') as tf :
        deduper.writeTraining(tf)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    with open(settings_file, 'w') as sf :
        deduper.writeSettings(sf)

threshold = deduper.threshold(data_d, recall_weight=4)

clustered_dupes = deduper.match(data_d, threshold)

print '# duplicate sets', len(clustered_dupes)

# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
cluster_id = None
for cluster_id, (cluster, score) in enumerate(clustered_dupes):
    for record_id in cluster:
        cluster_membership[record_id] = (cluster_id, score)

if cluster_id :
    unique_id = cluster_id + 1
else :
    unique_id = 0

with open(output_file, 'w') as f_out, open(input_file) as f_in :
    writer = csv.writer(f_out)

    reader = csv.reader(f_in)

    heading_row = reader.next()
    heading_row.insert(0, 'Score')
    heading_row.insert(0, 'Cluster ID')
    writer.writerow(heading_row)

    for idx, row in enumerate(reader):
        row_id = int(idx)
        if row_id in cluster_membership:
            cluser_id, score = cluster_membership[row_id]
        else:
            cluster_id, score = unique_id, None
            unique_id += 1
        row.insert(0, score)
        row.insert(0, cluster_id)
        writer.writerow(row)


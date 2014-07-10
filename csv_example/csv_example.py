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

For larger datasets, see our [mysql_example](http://open-city.github.com/dedupe/doc/mysql_example.html)
"""

import os
import csv
import re
import collections
import logging
import optparse
import numpy as np
from numpy import nan
from numpy import matlib

import dedupe

# ## Logging

# Dedupe uses Python logging to show or suppress verbose output. Added for convenience.
# To enable verbose logging, run `python examples/csv_example/csv_example.py -v`
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
logging.getLogger().setLevel(log_level)


# ## Setup

# Switch to our working directory and set up our input and out put paths,
# as well as our settings and training file locations
input_file = 'csv_example_messy_input.csv'
output_file = 'csv_example_output.csv'
settings_file = 'csv_example_learned_settings'
training_file = 'csv_example_training.json'


# Dedupe can take custom field comparison functions, here's one
# we'll use for zipcodes
def sameOrNotComparator(field_1, field_2) :
    if field_1 and field_2 :
        if field_1 == field_2 :
            return 1
        else:
            return 0
    else :
        return nan



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


print 'importing data ...'
data_d = readData(input_file)

# ## Training

if os.path.exists(settings_file):
    print 'reading from', settings_file
    deduper = dedupe.StaticDedupe(settings_file)

else:
    # Define the fields dedupe will pay attention to
    #
    # Notice how we are telling dedupe to use a custom field comparator
    # for the 'Zip' field. 
    fields = {
        'Site name': {'type': 'String'},
        'Address': {'type': 'String'},
        'Zip': {'type': 'Custom', 
                'comparator' : sameOrNotComparator, 
                'Has Missing' : True},
        'Phone': {'type': 'String', 'Has Missing' : True},
        }

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields)

    # To train dedupe, we feed it a random sample of records.
    deduper.sample(data_d, 150000)


    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    # __Note:__ if you want to train from scratch, delete the training_file
    if os.path.exists(training_file):
        print 'reading labeled examples from ', training_file
        deduper.readTraining(training_file)

    # ## Active learning
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print 'starting active labeling...'

    dedupe.consoleLabel(deduper)

    deduper.train()

    # When finished, save our training away to disk
    deduper.writeTraining(training_file)

    # Save our weights and predicates to disk.  If the settings file
    # exists, we will skip all the training and learning next time we run
    # this file.
    deduper.writeSettings(settings_file)


# ## Blocking

print 'blocking...'

# ## Clustering

# Find the threshold that will maximize a weighted average of our precision and recall. 
# When we set the recall weight to 2, we are saying we care twice as much
# about recall as we do precision.
#
# If we had more data, we would not pass in all the blocked data into
# this function but a representative sample.

threshold = deduper.threshold(data_d, recall_weight=2)

# `match` will return sets of record IDs that dedupe
# believes are all referring to the same entity.

print 'clustering...'
clustered_dupes = deduper.match(data_d, threshold)

print '# duplicate sets', len(clustered_dupes)



#Canonicalization###########################################################################

# takes in a list of attribute values for a field,
# evaluates the centroid using the comparator,
# & returns the centroid (i.e. the 'best' value for the field)
def getCentroid( attribute_variants, comparator ):
    n = len(attribute_variants)
    # if all values were empty & ignored, return ''
    if n == 0:
        return ''
    if n == 1:
        return attribute_variants[0]
    dist_matrix = np.matlib.zeros([n,n])
    avg_dist = np.zeros(n)
    # this is a matrix of distances between all strings
    # populate distance matrix by looping through elements of matrix triangle
    for i in range (1,n):
        for j in range (0, i):
            dist = comparator(attribute_variants[i], attribute_variants[j])
            dist_matrix[i,j] = dist
            dist_matrix[j,i] = dist
    # find avg distance per string
    for i in range (1,n):
        avg_dist = dist_matrix.mean(0)
    # find string with min avg distance
    avg_dist = np.array(avg_dist)
    min_dist_indices = np.where(avg_dist==avg_dist.min())
    # if there is only one value w/ min avg dist
    if len(min_dist_indices[1])==1:
        centroid_index = min_dist_indices[1][0]
        return attribute_variants[centroid_index]
    # if there are multiple values w/ min avg dist
    else:
        return breakCentroidTie( attribute_variants, min_dist_indices )

# find centroid when there are multiple values w/ min avg distance (e.g. any dupe cluster of 2)
# right now this just selects the first among a set of ties
# TO-DO? set this up so that for strings, it breaks ties by selecting the longest string (would need to take in comparator)
def breakCentroidTie( attribute_variants, min_dist_indices ):
    return attribute_variants[min_dist_indices[1][0]]

#this is the string distance calculation
from dedupe.distance.affinegap import normalizedAffineGapDistance as comparator

# takes in a cluster of duplicates & data, returns canonical representation of cluster
# TO-DO: get this to take in data model, so that it knows data types, comparators
def getCanonicalRep( dupe_cluster, data_d):
    keys = data_d[0].keys()
    canonical_rep = dict()
    ####### TO-DO ############
    # comparator = 

    #loop through keys & values in data, get centroid for each key
    for key in keys:
        key_values = []
        for record_id in dupe_cluster :
            #ignore empty values (assume non-empty values always better than empty value for canonical record)
            if data_d[record_id][key] != '':
                key_values.append(data_d[record_id][key])
        canonical_rep[key] = getCentroid(key_values, comparator)
    return canonical_rep


#############################################################################################




# ## Writing Results

# Write our original data back out to a CSV with a new column called 
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = collections.defaultdict(lambda : 'x')
for (cluster_id, cluster) in enumerate(clustered_dupes):
    for record_id in cluster:
        cluster_membership[record_id] = cluster_id


with open(output_file, 'w') as f:
    writer = csv.writer(f)

    with open(input_file) as f_input :
        reader = csv.reader(f_input)

        heading_row = reader.next()
        heading_row.insert(0, 'Cluster ID')
        writer.writerow(heading_row)

        for row in reader:
            row_id = int(row[0])
            cluster_id = cluster_membership[row_id]
            row.insert(0, cluster_id)
            writer.writerow(row)


"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the Athena database.

__Note:__ You will need to run `python mysql_init_db.py`
before running this script. See the annotates source for
[mysql_init_db.py](mysql_init_db.html)

For smaller datasets (<10,000), see our
[csv_example](csv_example.html)
"""

# There is a little bit difference between the result 
# of this module and the mysql one. The reason is due to
# Some special (and mostly erroneous) characters, such as \a .. 
# Which are dealt with differently by mysql and athena/panda

import sys
import os
import itertools
import time
import logging
import optparse
import locale
import json
from io import StringIO
import csv
import pandas as pd

import boto3
import dedupe
import dedupe.backport
sys.path.insert(0, '../athena_example/')
import config
sys.path.insert(0, '../athena_example/')
import athenautils

def cursor_execute(query, database):
    '''
    The MySQL compatible Cursor
    '''
    return athenautils.cursor_execute(query, database=database, 
                                      cursortype='tuple', buffersize=config.BUFFERSIZE,
                                      escapechar=None, keep_default_na=False, na_values=[''])

def dict_cursor_execute(query, database):
    '''
    The MySQL compatible DicCursor
    '''
    return athenautils.cursor_execute(query, database=database, 
                                      cursortype='dict', buffersize=config.BUFFERSIZE,
                                      escapechar=None, keep_default_na=False, na_values=[''])
def record_pairs(result_set):
    for i, row in enumerate(result_set):
        a_record_id, a_record, b_record_id, b_record = row
        record_a = (a_record_id, json.loads(a_record))
        record_b = (b_record_id, json.loads(b_record))

        yield record_a, record_b

        if i % 10000 == 0:
            print(i)


def cluster_ids(clustered_dupes):

    for cluster, scores in clustered_dupes:
        cluster_id = cluster[0]
        for donor_id, score in zip(cluster, scores):
            yield donor_id, cluster_id, score

if True:
# if __name__ == '__main__':

    ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. Added
    # for convenience.  To enable verbose output, run `python
    # examples/mysql_example/mysql_example.py -v`
    
#     optp = optparse.OptionParser()
#     optp.add_option('-v', '--verbose', dest='verbose', action='count',
#                     help='Increase verbosity (specify multiple times for more)'
#                     )
#     (opts, args) = optp.parse_args()
    log_level = logging.WARNING
#     if opts.verbose:
#         if opts.verbose == 1:
#             log_level = logging.INFO
#         elif opts.verbose >= 2:
#             log_level = logging.DEBUG


#     logging.getLogger().setLevel(log_level)

    


    settings_file = 'mysql_example_settings'
    training_file = 'mysql_example_training.json'

    start_time = time.time()

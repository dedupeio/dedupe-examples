
"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the Athena database.

__Note:__ You will need to run `python athena_init_db.py`
before running this script. See the annotates source for
[athena_init_db.py](athena_init_db.html)

For smaller datasets (<10,000), see our
[csv_example](csv_example.html)
"""

# There is a little bit difference between the result 
# of this module and the athena one. The reason is due to
# Some special (and mostly erroneous) characters, such as \a .. 
# Which are dealt with differently by athena and athena/panda

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


if __name__ == '__main__':

    ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. Added
    # for convenience.  To enable verbose output, run `python
    # examples/athena_example/athena_example.py -v`
    
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

    


    settings_file = 'athena_example_settings'
    training_file = 'athena_example_training.json'

    start_time = time.time()

    # We'll be using variations on this following select statement to pull
    # in campaign donor info.
    #
    # We did a fair amount of preprocessing of the fields in
    # `athena_init_db.py`    
    DONOR_SELECT = """SELECT donor_id, city, name, zip, state, address
                      from as_processed_donors"""

    # ## Training

    if os.path.exists(settings_file):
        print('reading from ', settings_file)
        with open(settings_file, 'rb') as sf:
            deduper = dedupe.StaticDedupe(sf, num_cores=4)
    else:
        # Define the fields dedupe will pay attention to
        #
        # The address, city, and zip fields are often missing, so we'll
        # tell dedupe that, and we'll learn a model that take that into
        # account
        fields = [{'field': 'name', 'type': 'String'},
                  {'field': 'address', 'type': 'String',
                   'has missing': True},
                  {'field': 'city', 'type': 'ShortString', 'has missing': True},
                  {'field': 'state', 'type': 'ShortString', 'has missing': True},
                  {'field': 'zip', 'type': 'ShortString', 'has missing': True},
                  ]

        # Create a new deduper object and pass our data model to it.
        deduper = dedupe.Dedupe(fields, num_cores=4)

        # We will sample pairs from the entire donor table for training
        cur = dict_cursor_execute(DONOR_SELECT, database=config.DATABASE)
        temp_d = {i: row for i, row in enumerate(cur)}
            

        # If we have training data saved from a previous run of dedupe,
        # look for it an load it in.
        #
        # __Note:__ if you want to train from
        # scratch, delete the training_file
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file) as tf:
                deduper.prepare_training(temp_d, training_file=tf)
        else:
            deduper.prepare_training(temp_d)

        del temp_d

        # ## Active learning

        print('starting active labeling...')
        # Starts the training loop. Dedupe will find the next pair of records
        # it is least certain about and ask you to label them as duplicates
        # or not.

        # use 'y', 'n' and 'u' keys to flag duplicates
        # press 'f' when you are finished
        dedupe.convenience.console_label(deduper)
        # When finished, save our labeled, training pairs to disk
        with open(training_file, 'w') as tf:
            deduper.write_training(tf)

        # Notice our the argument here
        #
        # `recall` is the proportion of true dupes pairs that the learned
        # rules must cover. You may want to reduce this if your are making
        # too many blocks and too many comparisons.
        deduper.train(recall=0.90)

        with open(settings_file, 'wb') as sf:
            deduper.write_settings(sf)

        # We can now remove some of the memory hobbing objects we used
        # for training
        deduper.cleanup_training()

    # ## Blocking

    print('blocking...')

    # To run blocking on such a large set of data, we create a separate table
    # that contains blocking keys and record ids
    print('creating as_blocking_map database')
    athenautils.drop_external_table("as_blocking_map", 
                                    location = 's3://{}/{}'.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_blocking_map'),
                                    database=config.DATABASE)

    q="""
    CREATE EXTERNAL TABLE as_blocking_map     
        (block_key VARCHAR(200), donor_id INTEGER)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'  
    LOCATION
        's3://{}/{}' 
    TBLPROPERTIES (
        'classification'='csv', 
        --'skip.header.line.count'='1',  
        'serialization.null.format'='')
    """.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_blocking_map') 
    athenautils.athena_start_query(q, database=config.DATABASE)

    # If dedupe learned a Index Predicate, we have to take a pass
    # through the data and create indices.
    print('creating inverted index')

    # Armin: 
    # This never runs, index_fields is empty, possible bug?
    for field in deduper.fingerprinter.index_fields:
        q = """
        SELECT DISTINCT {field} FROM as_processed_donors
        WHERE {field} IS NOT NULL
        """.format(field=field)
        cur = dict_cursor_execute(q, databse=config.DATABASE)
        field_data = (row[field] for row in cur)
        deduper.fingerprinter.index(field_data, field)
     

    # Now we are ready to write our blocking map table by creating a
    # generator that yields unique `(block_key, donor_id)` tuples.
    print('writing blocking map')
    
    read_cur  = dict_cursor_execute(DONOR_SELECT, database=config.DATABASE)
    full_data = ((row['donor_id'], row) for row in read_cur)

    b_data = deduper.fingerprinter(full_data)
    athenautils.write_many(b_data, 
                           filename='s3://{}/{}'.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_blocking_map/blocking.csv'))


    # select unique pairs to compare
    q="""
        SELECT a.donor_id,
            json_format(CAST (MAP(ARRAY['city', 'name', 'zip', 'state', 'address'],
                                  ARRAY[ a.city, a.name, a.zip, a.state, a.address])
                        AS JSON)),
            b.donor_id,
            json_format(CAST (MAP(ARRAY['city', 'name', 'zip', 'state', 'address'], 
                      ARRAY[ b.city, b.name, b.zip, b.state, b.address])
                  AS JSON))
        FROM (SELECT DISTINCT l.donor_id as east, r.donor_id as west
             from as_blocking_map as l
             INNER JOIN as_blocking_map as r
             using (block_key)
             where l.donor_id < r.donor_id) ids
        INNER JOIN as_processed_donors a on ids.east=a.donor_id
        INNER JOIN as_processed_donors b on ids.west=b.donor_id
       """
    read_cur = cursor_execute(q, database=config.DATABASE)


    # ## Clustering

    print('clustering...')
    clustered_dupes = deduper.cluster(deduper.score(record_pairs(read_cur)),
                                      threshold=0.5)

#     athenautils.athena_start_query("DROP TABLE IF EXISTS as_entity_map", database=config.DATABASE)
    athenautils.drop_external_table("as_entity_map", 
                                    location='s3://{}/{}'.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_entity_map/'), 
                                    database=config.DATABASE)
    
    print('creating as_entity_map database')
    q="""
    CREATE EXTERNAL TABLE as_entity_map     
        (donor_id INTEGER, canon_id INTEGER, 
         cluster_score FLOAT)
    ROW FORMAT DELIMITED
      FIELDS TERMINATED BY '\t'
      LINES TERMINATED BY '\n'  
    LOCATION
        's3://{}/{}' 
    TBLPROPERTIES (
        'classification'='csv', 
        --'skip.header.line.count'='1',  
        'serialization.null.format'='')
    """.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_entity_map') 
    athenautils.athena_start_query(q, database=config.DATABASE) 

    athenautils.write_many(cluster_ids(clustered_dupes),
                          filename='s3://{}/{}'.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_entity_map/entity_map.csv'))

    # Print out the number of duplicates found
    print('# duplicate sets')

    # ## Payoff

    # With all this done, we can now begin to ask interesting questions
    # of the data
    #
    # For example, let's see who the top 10 donors are.

    locale.setlocale(locale.LC_ALL, 'en_CA.UTF-8')  # for pretty printing numbers
    
    athenautils.athena_start_query("DROP TABLE IF EXISTS as_e_map", database=config.DATABASE)
    
    q = """
        CREATE TABLE as_e_map as 
        SELECT COALESCE(canon_id, as_entity_map.donor_id) AS canon_id, as_entity_map.donor_id 
        FROM as_entity_map 
        RIGHT JOIN as_donors USING(donor_id)        
        """    
    athenautils.athena_start_query(q, database=config.DATABASE)
    
    q = """
        SELECT array_join(filter(array[as_donors.first_name, as_donors.last_name], x-> x IS NOT NULL), ' ') AS name,   
            donation_totals.totals AS totals 
        FROM as_donors INNER JOIN 
            (SELECT canon_id, SUM(cast (amount as double)) AS totals 
            FROM as_contributions INNER JOIN as_e_map 
            USING (donor_id) 
            GROUP BY (canon_id) 
            ORDER BY totals 
            DESC LIMIT 10) 
            AS donation_totals 
        ON as_donors.donor_id = donation_totals.canon_id
        ORDER BY totals DESC
    """
    cur = dict_cursor_execute(q, database=config.DATABASE)

    print("Top Donors (deduped)")
    for row in cur:
        row['totals'] = locale.currency(row['totals'], grouping=True)
        print('%(totals)20s: %(name)s' % row)

    # Compare this to what we would have gotten if we hadn't done any
    # deduplication
    q = """
        with donorscontributions as(

            SELECT as_donors.donor_id, 
                array_join(filter(array[as_donors.first_name, as_donors.last_name], x-> x IS NOT NULL), ' ') AS name,
                cast(as_contributions.amount as double) as amount
            FROM as_donors INNER JOIN as_contributions 
                USING (donor_id) 
            )
        SELECT name, sum(amount) AS totals  
        FROM donorscontributions
        GROUP BY donor_id, name
        ORDER BY totals DESC 
        LIMIT 10
    """
    cur = dict_cursor_execute(q, database=config.DATABASE)

    print("Top Donors (raw)")
    for row in cur:
        row['totals'] = locale.currency(row['totals'], grouping=True)
        print('%(totals)20s: %(name)s' % row)

    print('ran in', time.time() - start_time, 'seconds')

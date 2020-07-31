#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# %load ../mysql_example/mysql_example.py
#!/usr/bin/python

"""
This is an example of working with very large data. There are about
700,000 unduplicated donors in this database of Illinois political
campaign contributions.

With such a large set of input data, we cannot store all the comparisons
we need to make in memory. Instead, we will read the pairs on demand
from the MySQL database.

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
import utils

def as_pandas(query, **kwrgs):
    df = utils.athena_to_panda(query, escapechar=None, keep_default_na=False, na_values=[''], **kwrgs)
    return df.where(pd.notnull(df), None)

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

    # ## Logging

    # Dedupe uses Python logging to show or suppress verbose output. Added
    # for convenience.  To enable verbose output, run `python
    # examples/mysql_example/mysql_example.py -v`
    
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

    


    settings_file = 'mysql_example_settings'
    training_file = 'mysql_example_training.json'

    start_time = time.time()


# In[ ]:


# We'll be using variations on this following select statement to pull
# in campaign donor info.
#
# We did a fair amount of preprocessing of the fields in
# `mysql_init_db.py`    
DONOR_SELECT = "SELECT donor_id, city, name, zip, state, address "                "from processed_donors"

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
#         with read_con.cursor() as cur:

    # Armin: The problem is the donor_id, it's numpy's int64, should be converted to int! 
    # But for that, astype doesn't work, and a loop on temp_d is slow, so for now let's just use str
#         with conn.cursor(PandasCursor, schema_name=schema_name) as cursor:
    temp_df = as_pandas(DONOR_SELECT)
    temp_d = temp_df.to_dict('index')
        

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


# In[ ]:


# ## Blocking

print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and record ids
print('creating blocking_map database')
utils.athena_start_query("DROP TABLE IF EXISTS blocking_map")

q='''
CREATE EXTERNAL TABLE blocking_map     
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
'''.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'blocking_map') 
utils.athena_start_query(q)


# In[ ]:


# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

# Armin: 
# This never runs, index_fields is empty, possible bug?
for field in deduper.fingerprinter.index_fields:
    q = '''
    SELECT DISTINCT {field} FROM processed_donors 
    WHERE {field} IS NOT NULL
    '''.format(field=field)
    cur_df = as_pandas(q)
    # Do I need to cast it as a list?
    field_data = cur_df[field]
    deduper.fingerprinter.index(field_data, field)
 


# In[ ]:


# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(block_key, donor_id)` tuples.
print('writing blocking map')


read_cur_dict = as_pandas(DONOR_SELECT).to_dict('records')
full_data = ((row['donor_id'], row) for row in read_cur_dict)


# In[ ]:


b_data = deduper.fingerprinter(full_data)
buffer = pd.DataFrame.from_records(b_data).to_csv(index=False, header=False, sep='\t')    utils.s3.put_object(Bucket=config.DATABASE_BUCKET, Key=config.DATABASE_ROOT_KEY+'blocking_map/blocking.csv', Body=buffer)    


# In[ ]:



    # select unique pairs to compare
    q='''
    SELECT a.donor_id,
        json_format(CAST (MAP(ARRAY['city', 'name', 'zip', 'state', 'address'],
                              ARRAY[ a.city, a.name, a.zip, a.state, a.address])
                    AS JSON)),
        b.donor_id,
        json_format(CAST (MAP(ARRAY['city', 'name', 'zip', 'state', 'address'], 
                  ARRAY[ b.city, b.name, b.zip, b.state, b.address])
              AS JSON))
    FROM (SELECT DISTINCT l.donor_id as east, r.donor_id as west
         from blocking_map as l
         INNER JOIN blocking_map as r
         using (block_key)
         where l.donor_id < r.donor_id) ids
    INNER JOIN processed_donors a on ids.east=a.donor_id
    INNER JOIN processed_donors b on ids.west=b.donor_id
    '''
    read_cur_dict=as_pandas(q).itertuples(index=False, name=None)


# In[ ]:


# ## Clustering

print('clustering...')
clustered_dupes = deduper.cluster(deduper.score(record_pairs(read_cur_dict)),
                                  threshold=0.5)


# In[ ]:


utils.athena_start_query("DROP TABLE IF EXISTS entity_map")

print('creating entity_map database')
q='''
CREATE EXTERNAL TABLE entity_map     
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
'''.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'entity_map') 
utils.athena_start_query(q) 

buffer = pd.DataFrame.from_records(cluster_ids(clustered_dupes)).to_csv(index=False, header=False, sep='\t')
utils.s3.put_object(Bucket=config.DATABASE_BUCKET, Key=config.DATABASE_ROOT_KEY+'entity_map/entity_map.csv', Body=buffer)    


# In[ ]:


# Print out the number of duplicates found
print('# duplicate sets')

# ## Payoff

# With all this done, we can now begin to ask interesting questions
# of the data
#
# For example, let's see who the top 10 donors are.

locale.setlocale(locale.LC_ALL, 'en_CA.UTF-8')  # for pretty printing numbers

utils.athena_start_query("DROP TABLE IF EXISTS e_map")
q = '''
CREATE TABLE e_map as 
    SELECT COALESCE(canon_id, entity_map.donor_id) AS canon_id, entity_map.donor_id 
    FROM entity_map 
        RIGHT JOIN donors USING(donor_id)
'''

utils.athena_start_query(q)
q ='''
SELECT array_join(filter(array[donors.first_name, donors.last_name], x-> x IS NOT NULL), ' ') AS name,   
    donation_totals.totals AS totals 
FROM donors INNER JOIN 
    (SELECT canon_id, SUM(cast (amount as double)) AS totals 
    FROM contributions INNER JOIN e_map 
    USING (donor_id) 
    GROUP BY (canon_id) 
    ORDER BY totals 
    DESC LIMIT 10) 
    AS donation_totals 
ON donors.donor_id = donation_totals.canon_id
ORDER BY totals DESC
'''
cur_dict = as_pandas(q).to_dict('records')

print("Top Donors (deduped)")
for row in cur_dict:
    row['totals'] = locale.currency(row['totals'], grouping=True)
    print('%(totals)20s: %(name)s' % row)

# Compare this to what we would have gotten if we hadn't done any
# deduplication

q = '''
with donorscontributions as(

    SELECT donors.donor_id, 
        array_join(filter(array[donors.first_name, donors.last_name], x-> x IS NOT NULL), ' ') AS name,
        cast(contributions.amount as double) as amount
    FROM donors INNER JOIN contributions 
        USING (donor_id) 
)
SELECT name, sum(amount) AS totals  
FROM donorscontributions
GROUP BY donor_id, name
ORDER BY totals DESC 
LIMIT 10
'''

cur_dict = as_pandas(q).to_dict('records')

print("Top Donors (raw)")
for row in cur_dict:
    row['totals'] = locale.currency(row['totals'], grouping=True)
    print('%(totals)20s: %(name)s' % row)

# Close our database connection
#     read_con.close()
#     write_con.close()

print('ran in', time.time() - start_time, 'seconds')


# In[9]:


get_ipython().system('jupyter nbconvert --to script athena_example.ipynb --output-dir=../athena_example/')


# In[ ]:





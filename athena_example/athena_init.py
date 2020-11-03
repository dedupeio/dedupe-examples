#!/usr/bin/python
"""
This is a setup script for athena_example.  It downloads a zip file of
Illinois campaign contributions and loads them into a Athena database
named 'contributions'.
 
__Note:__ You will need to run this script first before execuing
[athena_example.py](athena_example.py).
 
Tables created:
* as_raw_table - raw import of entire CSV file
* donors - all distinct donors based on name and address
* recipients - all distinct campaign contribution recipients
* contributions - contribution amounts tied to donor and recipients tables
"""

import os
import zipfile
import warnings
import pandas as pd
import numpy as np
from urllib.request import urlopen
import boto3
import config
import csv
import sys
sys.path.insert(0, '../athena_example/')
import athenautils


contributions_zip_file = 'Illinois-campaign-contributions.txt.zip'
contributions_txt_file = 'Illinois-campaign-contributions.txt'

if not os.path.exists(contributions_zip_file) :
    print('downloading', contributions_zip_file, '(~60mb) ...')
    u = urlopen('https://s3.amazonaws.com/dedupe-data/Illinois-campaign-contributions.txt.zip')
    localFile = open(contributions_zip_file, 'wb')
    localFile.write(u.read())
    localFile.close()

if not os.path.exists(contributions_txt_file) :
    zip_file = zipfile.ZipFile(contributions_zip_file, 'r')
    print('extracting %s' % contributions_zip_file)
    zip_file_contents = zip_file.namelist()
    for f in zip_file_contents:
        if ('.txt' in f):
            zip_file.extract(f)
    zip_file.close()




print('importing raw data from csv...')
athenautils.drop_external_table("as_raw_table", 
                                location = 's3://{}/{}'.format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_raw_table'),
                                database=config.DATABASE)    
athenautils.athena_start_query("DROP TABLE IF EXISTS as_donors", database=config.DATABASE)
athenautils.athena_start_query("DROP TABLE IF EXISTS as_recipients", database=config.DATABASE)
athenautils.athena_start_query("DROP TABLE IF EXISTS as_contributions", database=config.DATABASE)
athenautils.athena_start_query("DROP TABLE IF EXISTS as_processed_donors", database=config.DATABASE)


q=r"""
CREATE EXTERNAL TABLE as_raw_table 
    (reciept_id INT, last_name VARCHAR(70), first_name VARCHAR(35), 
    address_1 VARCHAR(35), address_2 VARCHAR(36), city VARCHAR(20), 
    state VARCHAR(15), zip VARCHAR(11), report_type VARCHAR(24), 
    date_recieved VARCHAR(10), loan_amount VARCHAR(12), 
    amount VARCHAR(23), receipt_type VARCHAR(23), 
    employer VARCHAR(70), occupation VARCHAR(40), 
    vendor_last_name VARCHAR(70), vendor_first_name VARCHAR(20), 
    vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31), 
    vendor_city VARCHAR(20), vendor_state VARCHAR(10), 
    vendor_zip VARCHAR(10), description VARCHAR(90), 
    election_type VARCHAR(10), election_year VARCHAR(10), 
    report_period_begin VARCHAR(10), report_period_end VARCHAR(33), 
    committee_name VARCHAR(70), committee_id VARCHAR(37)) 
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  ESCAPED BY '\\'
  LINES TERMINATED BY '\n'  
LOCATION
    's3://{}/{}' 
TBLPROPERTIES (
    'classification'='csv', 
    'skip.header.line.count'='1',  
    'serialization.null.format'='')
""".format(config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY+'as_raw_table') 
athenautils.athena_start_query(q, database=config.DATABASE)


df_cursor = pd.read_csv(contributions_txt_file, sep='\t', escapechar='\\', quoting=csv.QUOTE_NONE,  
                        error_bad_lines=False, warn_bad_lines=True, dtype=str, keep_default_na=False, na_values=[''],
                        chunksize=config.BUFFERSIZE)
chunkcount = 0
filename=os.path.join("s3://", config.DATABASE_BUCKET, config.DATABASE_ROOT_KEY,'as_raw_table', os.path.splitext(contributions_txt_file)[0]+'.csv')
for df in df_cursor: 
    # Remove the very few records that mess up the demo 
    # (demo purposes only! Don't do something like this in production)
    df = df[df['RcvDate'].str.len()>=10]

    # set empty, non-zero, strings in date columns to null
    df.loc[df['RptPdBegDate'].str.len()<10,'RptPdBegDate'] = np.nan

    df.loc[df['RptPdEndDate'].str.len()<10,'RptPdEndDate'] = np.nan

    #committee ID is requred. Remove the 2 rows that don't have it.
    df = df[df['ID']!='']

    # There's a record with a date stuck in the committee_id column, which causes
    # problems when inserting into the contributions table below. Get rid of it this 
    # way.
    df = df[df['ID'].str.len() <=9]

    # dropping the last columns
    df = df.drop(columns='Unnamed: 29')

    df_lower=df.apply(lambda x: x.str.lower().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8') if x.dtype=='object' else x, result_type='expand')
    
    buffer = df_lower.to_csv(quoting=csv.QUOTE_NONE, sep="\t", escapechar='\\', index=None)
    
    chunk_fname = athenautils.file_name_append(filename, '_{}'.format(chunkcount), ommitext=False)
    athenautils.write(body=buffer, filename=chunk_fname)
    chunkcount += 1    
    
print('creating donors table...')
q="""
CREATE TABLE as_donors as
    with tmp as
      (SELECT DISTINCT 
           NULLIF(TRIM(last_name), '') as last_name, 
           NULLIF(TRIM(first_name), '') as first_name, 
           NULLIF(TRIM(address_1), '') as address_1, 
           NULLIF(TRIM(address_2), '') as address_2, 
           NULLIF(TRIM(city), '') city, 
           NULLIF(TRIM(state), '') as state, 
           NULLIF(TRIM(zip), '') as zip, 
           NULLIF(TRIM(employer), '') as employer, 
           NULLIF(TRIM(occupation), '') as occupation
      FROM as_raw_table)
    SELECT row_number() over () as donor_id, * from tmp"""
athenautils.athena_start_query(q, database=config.DATABASE)


q="""
CREATE TABLE as_recipients as
    SELECT DISTINCT committee_id as recipient_id, committee_name as name FROM as_raw_table
"""
athenautils.athena_start_query(q, database=config.DATABASE)

print('creating contributions table')

q="""
CREATE TABLE as_contributions as
    SELECT reciept_id as contribution_id, 
        donors.donor_id as donor_id , 
        committee_id as recipient_id, 
        report_type, date_parse(date_recieved, '%m/%d/%Y') as date_recieved, 
        loan_amount, amount, 
        receipt_type, vendor_last_name , 
        vendor_first_name, vendor_address_1, vendor_address_2, 
        vendor_city, vendor_state, vendor_zip, description, 
        election_type, election_year, 
        date_parse(report_period_begin, '%m/%d/%Y') as report_period_begin, 
        date_parse(report_period_end, '%m/%d/%Y') as report_period_end 
    FROM as_raw_table JOIN as_donors donors ON 
        coalesce(donors.first_name, '') = coalesce(TRIM(as_raw_table.first_name), '') AND 
        coalesce(donors.last_name, '') = coalesce(TRIM(as_raw_table.last_name), '') AND 
        coalesce(donors.address_1, '') = coalesce(TRIM(as_raw_table.address_1), '') AND 
        coalesce(donors.address_2, '') = coalesce(TRIM(as_raw_table.address_2), '') AND 
        coalesce(donors.city, '') = coalesce(TRIM(as_raw_table.city), '') AND 
        coalesce(donors.state, '') = coalesce(TRIM(as_raw_table.state), '') AND 
        coalesce(donors.employer, '') = coalesce(TRIM(as_raw_table.employer), '') AND 
        coalesce(donors.occupation , '')= coalesce(TRIM(as_raw_table.occupation), '') AND 
        coalesce(donors.zip, '') = coalesce(TRIM(as_raw_table.zip), '')"""

athenautils.athena_start_query(q, database=config.DATABASE)

q = """
CREATE TABLE as_processed_donors AS  
    SELECT donor_id,  
     LOWER(city) AS city,  
     CASE WHEN (first_name IS NULL AND last_name IS NULL) 
          THEN NULL 
          ELSE LOWER(array_join(filter(array[first_name, last_name], x-> x IS NOT NULL), ' ')) 
     END AS name,  
     LOWER(zip) AS zip,  
     LOWER(state) AS state,  
     CASE WHEN (address_1 IS NULL AND address_2 IS NULL) 
          THEN NULL 
          ELSE LOWER(array_join(filter(array[address_1, address_2], x-> x IS NOT NULL), ' '))
     END AS address,  
     LOWER(occupation) AS occupation, 
     LOWER(employer) AS employer, 
     first_name is null AS person 
 FROM as_donors"""
athenautils.athena_start_query(q, database=config.DATABASE)




print('done')

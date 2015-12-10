#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This is a setup script for mysql_example.  It downloads a zip file of
Illinois campaign contributions and loads them in t aMySQL database
named 'contributions'.

__Note:__ You will need to run this script first before execuing
[mysql_example.py](http://datamade.github.com/dedupe-examples/docs/mysql_example.html).

Tables created:
* raw_table - raw import of entire CSV file
* donors - all distinct donors based on name and address
* recipients - all distinct campaign contribution recipients
* contributions - contribution amounts tied to donor and recipients tables
"""
import csv
import os
import urllib2
import zipfile

import dj_database_url
import psycopg2
import psycopg2.extras
import unidecode

_file = 'Illinois-campaign-contributions'
contributions_zip_file = _file + '.txt.zip'
contributions_txt_file = _file + '.txt'
contributions_csv_file = _file + '.csv'

if not os.path.exists(contributions_zip_file):
    print 'downloading', contributions_zip_file, '(~60mb) ...'
    u = urllib2.urlopen(
        'https://s3.amazonaws.com/dedupe-data/'
        'Illinois-campaign-contributions.txt.zip')
    localFile = open(contributions_zip_file, 'w')
    localFile.write(u.read())
    localFile.close()

if not os.path.exists(contributions_txt_file):
    zip_file = zipfile.ZipFile(contributions_zip_file, 'r')
    print 'extracting %s' % contributions_zip_file
    zip_file_contents = zip_file.namelist()
    for f in zip_file_contents:
        if ('.txt' in f):
            zip_file.extract(f)
    zip_file.close()

# Create a cleaned up CSV version of file with consistent row lengths.
# Postgres COPY doesn't handle "ragged" files very well
if not os.path.exists(contributions_csv_file):
    print 'converting tab-delimited raw file to csv...'
    with open(contributions_txt_file, 'rU') as txt_file, \
            open(contributions_csv_file, 'w') as csv_file:
        csv_writer = csv.writer(csv_file, quoting=csv.QUOTE_ALL)
        for line in txt_file:
            if not all(ord(c) < 128 for c in line):
                line = unidecode.unidecode(line)
            row = line.rstrip('\t\r\n').split('\t')
            if len(row) != 29:
                print 'skipping bad row (length %s, expected 29):' % len(row)
                print row
                continue
            csv_writer.writerow(row)

db_conf = dj_database_url.config()

if not db_conf:
    raise Exception(
        'set DATABASE_URL environment variable with your connection, e.g. '
        'export DATABASE_URL=postgres://user:password@host/mydatabase'
    )

conn = psycopg2.connect(database=db_conf['NAME'],
                        user=db_conf['USER'],
                        password=db_conf['PASSWORD'],
                        host=db_conf['HOST'],
                        port=db_conf['PORT'])

c = conn.cursor()

print 'importing raw data from csv...'
c.execute("DROP TABLE IF EXISTS raw_table")
c.execute("DROP TABLE IF EXISTS donors")
c.execute("DROP TABLE IF EXISTS recipients")
c.execute("DROP TABLE IF EXISTS contributions")
c.execute("DROP TABLE IF EXISTS processed_donors")

c.execute("CREATE TABLE raw_table "
          "(reciept_id INT, last_name VARCHAR(70), first_name VARCHAR(35), "
          " address_1 VARCHAR(35), address_2 VARCHAR(36), city VARCHAR(20), "
          " state VARCHAR(15), zip VARCHAR(11), report_type VARCHAR(24), "
          " date_recieved VARCHAR(10), loan_amount VARCHAR(12), "
          " amount VARCHAR(23), receipt_type VARCHAR(23), "
          " employer VARCHAR(70), occupation VARCHAR(40), "
          " vendor_last_name VARCHAR(70), vendor_first_name VARCHAR(20), "
          " vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31), "
          " vendor_city VARCHAR(20), vendor_state VARCHAR(10), "
          " vendor_zip VARCHAR(10), description VARCHAR(90), "
          " election_type VARCHAR(10), election_year VARCHAR(10), "
          " report_period_begin VARCHAR(10), report_period_end VARCHAR(33), "
          " committee_name VARCHAR(70), committee_id VARCHAR(37))")

conn.commit()

with open(contributions_csv_file, 'rU') as csv_file:
    c.copy_expert("COPY raw_table "
                  "(reciept_id, last_name, first_name, "
                  " address_1, address_2, city, state, "
                  " zip, report_type, date_recieved, "
                  " loan_amount, amount, receipt_type, "
                  " employer, occupation, vendor_last_name, "
                  " vendor_first_name, vendor_address_1, "
                  " vendor_address_2, vendor_city, vendor_state, "
                  " vendor_zip, description, election_type, "
                  " election_year, "
                  " report_period_begin, report_period_end, "
                  " committee_name, committee_id) "
                  "FROM STDIN CSV HEADER", csv_file)

conn.commit()

print 'creating donors table...'
c.execute("CREATE TABLE donors "
          "(donor_id SERIAL PRIMARY KEY, "
          " last_name VARCHAR(70), first_name VARCHAR(35), "
          " address_1 VARCHAR(35), address_2 VARCHAR(36), "
          " city VARCHAR(20), state VARCHAR(15), "
          " zip VARCHAR(11), employer VARCHAR(70), "
          " occupation VARCHAR(40))")

c.execute("INSERT INTO donors "
          "(first_name, last_name, address_1, "
          " address_2, city, state, zip, employer, occupation) "
          "SELECT DISTINCT "
          "LOWER(TRIM(first_name)), LOWER(TRIM(last_name)), "
          "LOWER(TRIM(address_1)), LOWER(TRIM(address_2)), "
          "LOWER(TRIM(city)), LOWER(TRIM(state)), LOWER(TRIM(zip)), "
          "LOWER(TRIM(employer)), LOWER(TRIM(occupation)) "
          "FROM raw_table")

conn.commit()

print 'creating indexes on donors table...'
c.execute("CREATE INDEX donors_donor_info ON donors "
          "(last_name, first_name, address_1, address_2, city, "
          " state, zip)")
conn.commit()

print 'creating recipients table...'
c.execute("CREATE TABLE recipients "
          "(recipient_id SERIAL PRIMARY KEY, name VARCHAR(70))")

c.execute("INSERT INTO recipients "
          "SELECT DISTINCT CAST(committee_id AS INTEGER), "
          "committee_name FROM raw_table")
conn.commit()

print 'creating contributions table...'
c.execute("CREATE TABLE contributions "
          "(contribution_id INT, donor_id INT, recipient_id INT, "
          " report_type VARCHAR(24), date_recieved DATE, "
          " loan_amount VARCHAR(12), amount VARCHAR(23), "
          " receipt_type VARCHAR(23), "
          " vendor_last_name VARCHAR(70), "
          " vendor_first_name VARCHAR(20), "
          " vendor_address_1 VARCHAR(35), vendor_address_2 VARCHAR(31), "
          " vendor_city VARCHAR(20), vendor_state VARCHAR(10), "
          " vendor_zip VARCHAR(10), description VARCHAR(90), "
          " election_type VARCHAR(10), election_year VARCHAR(10), "
          " report_period_begin DATE, report_period_end DATE)")


c.execute("INSERT INTO contributions "
          "SELECT reciept_id, donors.donor_id, CAST(committee_id AS INTEGER), "
          " report_type, TO_DATE(TRIM(date_recieved), 'MM/DD/YYYY'), "
          " loan_amount, amount, "
          " receipt_type, vendor_last_name , "
          " vendor_first_name, vendor_address_1,"
          " vendor_address_2, "
          " vendor_city, vendor_state, vendor_zip,"
          " description, "
          " election_type, election_year, "
          " TO_DATE(TRIM(report_period_begin), 'MM/DD/YYYY'), "
          " TO_DATE(TRIM(report_period_end), 'MM/DD/YYYY') "
          "FROM raw_table JOIN donors ON "
          "donors.first_name = LOWER(TRIM(raw_table.first_name)) AND "
          "donors.last_name = LOWER(TRIM(raw_table.last_name)) AND "
          "donors.address_1 = LOWER(TRIM(raw_table.address_1)) AND "
          "donors.address_2 = LOWER(TRIM(raw_table.address_2)) AND "
          "donors.city = LOWER(TRIM(raw_table.city)) AND "
          "donors.state = LOWER(TRIM(raw_table.state)) AND "
          "donors.employer = LOWER(TRIM(raw_table.employer)) AND "
          "donors.occupation = LOWER(TRIM(raw_table.occupation)) AND "
          "donors.zip = LOWER(TRIM(raw_table.zip))")
conn.commit()

print 'creating indexes on contributions...'
c.execute("ALTER TABLE contributions ADD PRIMARY KEY(contribution_id)")
c.execute("CREATE INDEX donor_idx ON contributions (donor_id)")
c.execute("CREATE INDEX recipient_idx ON contributions (recipient_id)")

conn.commit()

print 'nullifying empty strings in donors...'
c.execute(
    "UPDATE donors "
    "SET "
    "first_name = CASE first_name WHEN '' THEN NULL ELSE first_name END, "
    "last_name = CASE last_name WHEN '' THEN NULL ELSE last_name END, "
    "address_1 = CASE address_1 WHEN '' THEN NULL ELSE address_1 END, "
    "address_2 = CASE address_2 WHEN '' THEN NULL ELSE address_2 END, "
    "city = CASE city WHEN '' THEN NULL ELSE city END, "
    "state = CASE state WHEN '' THEN NULL ELSE state END, "
    "employer = CASE employer WHEN '' THEN NULL ELSE employer END, "
    "occupation = CASE occupation WHEN '' THEN NULL ELSE occupation END, "
    "zip = CASE zip WHEN '' THEN NULL ELSE zip END"
)


conn.commit()

print 'creating processed_donors...'
c.execute("CREATE TABLE processed_donors AS "
          "(SELECT donor_id, "
          " LOWER(city) AS city, "
          " CASE WHEN (first_name IS NULL AND last_name IS NULL) "
          "      THEN NULL "
          "      ELSE LOWER(CONCAT_WS(' ', first_name, last_name)) "
          " END AS name, " 
          " LOWER(zip) AS zip, "
          " LOWER(state) AS state, "
          " CASE WHEN (address_1 IS NULL AND address_2 IS NULL) "
          "      THEN NULL "
          "      ELSE LOWER(CONCAT_WS(' ', address_1, address_2)) "
          " END AS address, " 
          " LOWER(occupation) AS occupation, "
          " LOWER(employer) AS employer, "
          " CAST((first_name IS NULL) AS INTEGER) AS person "
          " FROM donors)")

c.execute("CREATE INDEX processed_donor_idx ON processed_donors (donor_id)")

conn.commit()

c.close()
conn.close()
print 'done'
